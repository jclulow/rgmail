/*
 * Copyright 2022 Joshua M. Clulow <josh@sysmgr.org>
 */

use anyhow::Result;
use futures_core::Stream;
use reqwest::header;
use serde::Deserialize;
use slog::debug;
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::Poll;

use super::gmail;
use super::types::*;
use super::util::*;

pub struct MessagesConfig {
    parent: Arc<gmail::GMailInner>,
    perpage: Option<u32>,
    q: Option<String>,
    spamtrash: bool,
    label_ids: Vec<String>,
    resume_from_token: Option<String>,
}

impl MessagesConfig {
    pub(crate) fn new(parent: &gmail::GMail) -> MessagesConfig {
        MessagesConfig {
            parent: Arc::clone(&parent.0),
            perpage: None,
            q: None,
            spamtrash: false,
            label_ids: Vec::new(),
            resume_from_token: None,
        }
    }

    pub fn query<S: AsRef<str>>(mut self, s: S) -> MessagesConfig {
        self.q = Some(s.as_ref().to_string());
        self
    }

    pub fn include_spam_trash(mut self, i: bool) -> MessagesConfig {
        self.spamtrash = i;
        self
    }

    pub fn resume_from_token(mut self, s: &str) -> MessagesConfig {
        self.resume_from_token = Some(s.to_string());
        self
    }

    pub fn batch_size(mut self, n: u32) -> MessagesConfig {
        self.perpage = Some(n);
        self
    }

    pub fn labels_clear(mut self) -> MessagesConfig {
        self.label_ids.clear();
        self
    }

    pub fn label_add(mut self, label_id: &str) -> MessagesConfig {
        let s = label_id.to_string();

        if !self.label_ids.contains(&s) {
            self.label_ids.push(s);
        }

        self
    }

    pub fn start(self) -> Messages {
        Messages {
            fin: false,
            previous_token: None,
            page_token: self.resume_from_token.clone(),
            c: Arc::new(self),
            infl: VecDeque::new(),
            fetch: None,
        }
    }
}

pub struct Messages {
    c: Arc<MessagesConfig>,
    fin: bool,
    previous_token: Option<String>,
    page_token: Option<String>,
    infl: VecDeque<RMessage>,
    fetch: Option<Pin<Box<(dyn Future<Output = Result<RMessages>>)>>>,
}

impl Messages {
    pub fn resume_token(&self) -> Option<String> {
        self.previous_token.clone()
    }
}

impl Stream for Messages {
    type Item = Result<RMessage>;

    fn poll_next(
        mut self: Pin<&mut Messages>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        loop {
            /*
             * If there is something in the queue already, we can just
             * return it.
             */
            if let Some(rm) = self.infl.pop_front() {
                return Poll::Ready(Some(Ok(rm)));
            }

            /*
             * If we have already read the last page, there is nothing left
             * to do.
             */
            if self.fin {
                debug!(self.c.parent.log, "finished completely!");
                return Poll::Ready(None);
            }

            /*
             * At this point, we either need to spawn a task to load the next
             * page from the server, or if we already have a running task, we
             * need to wait for it to be finished.
             */
            if let Some(fetch) = self.fetch.as_mut() {
                let pin = Pin::new(fetch);
                match pin.poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Ok(o)) => {
                        self.fetch = None;

                        debug!(
                            self.c.parent.log,
                            "result count estimate: {}", o.result_size_estimate
                        );
                        debug!(
                            self.c.parent.log,
                            "new next page token: {:?}", o.next_page_token
                        );

                        self.previous_token = self.page_token.clone();
                        self.page_token = o.next_page_token;
                        if self.page_token.is_none() {
                            /*
                             * If we do not have a next page token, the stream
                             * is finished.
                             */
                            self.fin = true;
                        }

                        debug!(
                            self.c.parent.log,
                            "got {} messages records",
                            o.messages.len()
                        );

                        for rm in o.messages {
                            self.infl.push_back(rm);
                        }
                    }
                    Poll::Ready(Err(e)) => {
                        self.fetch = None;
                        return Poll::Ready(Some(Err(e)));
                    }
                }
            } else {
                let pt = self.page_token.clone();

                /*
                 * No fetch was in progress.  Start one.
                 */
                debug!(
                    self.c.parent.log,
                    "requesting more messages (pt {:?})", pt,
                );
                self.fetch =
                    Some(Box::pin(fetch_page(Arc::clone(&self.c), pt)));
            }
        }
    }
}

async fn fetch_page(
    c: Arc<MessagesConfig>,
    page_token: Option<String>,
) -> Result<RMessages> {
    let log = &c.parent.log;

    debug!(log, "requesting more message IDs (pt {:?})", page_token);

    let url = bu("users/me/messages");

    c.parent.auth.check_refresh().await?;

    let mut req = c.parent.client.get(&url).header(
        header::AUTHORIZATION,
        format!("Bearer {}", c.parent.auth.access_token()),
    );

    if let Some(q) = &c.q {
        req = req.query(&[("q", q)]);
    }
    if c.spamtrash {
        req = req.query(&[("includeSpamTrash", "true")]);
    }
    for l in &c.label_ids {
        req = req.query(&[("labelIds", l)]);
    }
    if let Some(pt) = &page_token {
        req = req.query(&[("pageToken", pt)]);
    }
    if let Some(pp) = &c.perpage {
        req = req.query(&[("maxResults", pp.to_string())]);
    }

    let req = req.build()?;
    debug!(log, "request for page: {}", req.url());

    let res = c.parent.client.execute(req).await?.error_for_status()?;

    Ok(res.json().await?)
}
