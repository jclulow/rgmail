/*
 * Copyright 2022 Joshua M. Clulow <josh@sysmgr.org>
 */

use std::collections::{HashSet, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;

use anyhow::Result;
use futures_core::stream::Stream;
use reqwest::header;
use serde::Deserialize;
use serde_aux::prelude::*;
use slog::debug;

use super::gmail;
use super::messages;
use super::types::*;
use super::util::*;

pub struct HistoryConfig {
    parent: Arc<gmail::GMailInner>,
    perpage: Option<u32>,
    label_id: Option<String>,
    history_types: Vec<String>,
    start_at: u64,
}

impl HistoryConfig {
    pub(crate) fn new(parent: &gmail::GMail, start_at: u64) -> HistoryConfig {
        HistoryConfig {
            parent: Arc::clone(&parent.0),
            perpage: None,
            label_id: None,
            history_types: Vec::new(),
            start_at,
        }
    }

    pub fn batch_size(mut self, n: u32) -> HistoryConfig {
        self.perpage = Some(n);
        self
    }

    pub fn history_types_clear(mut self) -> HistoryConfig {
        self.history_types.clear();
        self
    }

    pub fn history_type_add(mut self, history_type: &str) -> HistoryConfig {
        let s = history_type.to_string();

        if !self.history_types.contains(&s) {
            self.history_types.push(s);
        }

        self
    }

    pub fn label(mut self, label_id: &str) -> HistoryConfig {
        self.label_id = Some(label_id.to_string());
        self
    }

    pub fn start(self) -> History {
        History {
            c: Arc::new(self),
            fin: false,
            page_token: None,
            infl: VecDeque::new(),
            final_id: None,
            fetch: None,
        }
    }
}

pub struct History {
    c: Arc<HistoryConfig>,
    fin: bool,
    page_token: Option<String>,
    infl: VecDeque<RHistoryRecord>,
    final_id: Option<u64>,
    fetch: Option<Pin<Box<(dyn Future<Output = Result<RHistory>>)>>>,
}

impl Stream for History {
    type Item = Result<RHistoryRecord>;

    fn poll_next(
        mut self: Pin<&mut History>,
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
                            "new next page token: {:?}", o.next_page_token
                        );

                        self.page_token = o.next_page_token;
                        if self.page_token.is_none() {
                            /*
                             * If we do not have a next page token, the stream
                             * is finished.
                             */
                            self.fin = true;
                            self.final_id = Some(o.history_id);
                        }

                        debug!(
                            self.c.parent.log,
                            "got {} history records",
                            o.history.len()
                        );

                        for hr in o.history {
                            self.infl.push_back(hr);
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
                    "requesting more histories (pt {:?})", pt,
                );
                self.fetch =
                    Some(Box::pin(fetch_page(Arc::clone(&self.c), pt)));
            }
        }
    }
}

impl History {
    pub fn final_id(&self) -> u64 {
        self.final_id.unwrap()
    }
}

async fn fetch_page(
    c: Arc<HistoryConfig>,
    page_token: Option<String>,
) -> Result<RHistory> {
    let log = &c.parent.log;

    let url = bu("users/me/history");

    c.parent.auth.check_refresh().await?;

    let mut req = c.parent.client.get(&url).header(
        header::AUTHORIZATION,
        format!("Bearer {}", c.parent.auth.access_token()),
    );

    req = req.query(&[("startHistoryId", c.start_at.to_string())]);
    if let Some(label_id) = &c.label_id {
        req = req.query(&[("labelId", label_id)]);
    }
    for t in &c.history_types {
        req = req.query(&[("historyTypes", t)]);
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
