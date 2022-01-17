/* vim: set tw=80: */

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use serde::{Deserialize, Serialize};

use reqwest::header;
use reqwest::redirect;
use reqwest::StatusCode;
use reqwest::{Client, ClientBuilder};
use serde_aux::prelude::*;

use slog::{debug, trace, Logger};

use anyhow::{bail, Result};

use super::gauth::GAuth;
use super::multipart::multipart_parse;
use super::types::*;
use super::util::*;
use super::{history, messages};

#[derive(Clone)]
pub struct GMailInner {
    pub(crate) log: Logger,
    pub(crate) auth: GAuth,
    pub(crate) client: Client,
}

#[derive(Clone)]
pub struct GMail(pub(crate) Arc<GMailInner>);

impl std::ops::Deref for GMail {
    type Target = GMailInner;

    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Profile {
    pub email_address: String,
    pub messages_total: u64,
    pub threads_total: u64,
    #[serde(deserialize_with = "deserialize_number_from_string")]
    pub history_id: u64,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Label {
    id: String,
    name: String,
    #[serde(rename = "type")]
    typ: String,
}

impl Label {
    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn type_(&self) -> &str {
        &self.typ
    }
}

pub trait LabelsHelper {
    fn names(&self) -> Vec<&str>;
    fn id_of(&self, n: &str) -> Option<&str>;
}

impl LabelsHelper for Vec<Label> {
    fn names(&self) -> Vec<&str> {
        let mut names: Vec<&str> =
            self.iter().map(|l| l.name.as_str()).collect();
        names.sort_unstable();
        names
    }

    fn id_of(&self, n: &str) -> Option<&str> {
        for l in self.iter() {
            if l.name == n {
                return Some(l.id.as_str());
            }
        }

        None
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MessageHeader {
    pub name: String,
    pub value: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MessagePayload {
    pub headers: Vec<MessageHeader>,
    pub mime_type: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MessageRaw {
    pub id: String,
    pub thread_id: String,
    #[serde(default)]
    pub label_ids: HashSet<String>,
    #[serde(deserialize_with = "deserialize_number_from_string")]
    pub history_id: u64,
    #[serde(deserialize_with = "deserialize_number_from_string")]
    pub internal_date: u64,
    raw: String,
}

impl MessageRaw {
    pub fn raw(&self) -> Result<Vec<u8>> {
        Ok(base64::decode_config(
            self.raw.as_bytes(),
            base64::URL_SAFE,
        )?)
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MessageMinimal {
    pub id: String,
    pub thread_id: String,
    #[serde(default)]
    pub label_ids: HashSet<String>,
    pub snippet: String,
    pub size_estimate: u64,
    #[serde(deserialize_with = "deserialize_number_from_string")]
    pub history_id: u64,
    #[serde(deserialize_with = "deserialize_number_from_string")]
    pub internal_date: u64,
}

pub trait MessageId {
    fn id(&self) -> &str;
}

#[derive(Debug)]
pub enum MultiResult<T> {
    Present(T),
    Missing(String),
    RateLimit(String),
}

impl MessageMinimal {
    pub fn date(&self) -> SystemTime {
        SystemTime::UNIX_EPOCH
            .checked_add(Duration::from_millis(self.internal_date))
            .expect("system time add")
    }

    pub fn age_days(&self) -> f64 {
        SystemTime::now()
            .duration_since(self.date())
            .expect("since")
            .as_secs_f64()
            / 86_400.
    }
}

impl MessageId for MessageMinimal {
    fn id(&self) -> &str {
        self.id.as_str()
    }
}

impl MessageId for MessageRaw {
    fn id(&self) -> &str {
        self.id.as_str()
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Message {
    pub id: String,
    pub thread_id: String,
    pub history_id: String,
    pub internal_date: String,
    #[serde(default)]
    pub label_ids: HashSet<String>,
    pub payload: MessagePayload,
    pub size_estimate: u64,
    pub snippet: String,
}

impl Message {
    pub fn headers(&self, n: &str) -> Vec<&str> {
        let mut out = Vec::new();

        for mh in &self.payload.headers {
            if mh.name.eq_ignore_ascii_case(n) {
                out.push(mh.value.as_str());
            }
        }

        out
    }

    pub fn header_or_blank(&self, n: &str) -> &str {
        let h = self.headers(n);

        if let Some(s) = h.get(0) {
            s
        } else {
            ""
        }
    }

    pub fn subject(&self) -> &str {
        self.header_or_blank("subject")
    }

    pub fn mailer(&self) -> &str {
        self.header_or_blank("x-mailer")
    }

    pub fn date(&self) -> SystemTime {
        let ems: u64 = self.internal_date.parse().expect("system time");
        SystemTime::UNIX_EPOCH
            .checked_add(Duration::from_millis(ems))
            .expect("system time add")
    }

    pub fn age_days(&self) -> f64 {
        let ems: u64 = self.internal_date.parse().expect("system time");
        let then = SystemTime::UNIX_EPOCH
            .checked_add(Duration::from_millis(ems))
            .expect("system time add");
        SystemTime::now()
            .duration_since(then)
            .expect("since")
            .as_secs_f64()
            / 86_400.
    }
}

impl GMail {
    pub fn new(log: Logger, auth: GAuth) -> GMail {
        let cb = ClientBuilder::new().redirect(redirect::Policy::none());

        GMail(Arc::new(GMailInner {
            log,
            client: cb.build().expect("build client"),
            auth,
        }))
    }

    pub fn history_list(&self, start_at: u64) -> history::HistoryConfig {
        history::HistoryConfig::new(self, start_at)
    }

    pub fn messages_list(&self) -> messages::MessagesConfig {
        messages::MessagesConfig::new(self)
    }

    pub async fn profile(&self) -> Result<Profile> {
        let url = bu("users/me/profile");

        self.auth.check_refresh().await?;

        let res = self
            .client
            .get(&url)
            .header(
                header::AUTHORIZATION,
                format!("Bearer {}", self.auth.access_token()),
            )
            .send()
            .await?
            .error_for_status()?;

        Ok(res.json().await?)
    }

    pub async fn message_get_min(&self, id: &str) -> Result<MessageMinimal> {
        let url = bu(&format!("users/me/messages/{}", id));

        self.auth.check_refresh().await?;

        let res = self
            .client
            .get(&url)
            .header(
                header::AUTHORIZATION,
                format!("Bearer {}", self.auth.access_token()),
            )
            .query(&[("format", "minimal")])
            .send()
            .await?
            .error_for_status()?;

        let t = res.text().await?;

        match serde_json::from_str(&t) {
            Ok(r) => Ok(r),
            Err(e) => bail!("parsing response: {}: {}", e, t),
        }
    }

    pub async fn messages_get<S: AsRef<str>>(
        &self,
        ids: &[S],
    ) -> Result<Vec<MultiResult<MessageMinimal>>> {
        self.messages_get_common("minimal", ids).await
    }

    pub async fn messages_get_raw<S: AsRef<str>>(
        &self,
        ids: &[S],
    ) -> Result<Vec<MultiResult<MessageRaw>>> {
        self.messages_get_common("raw", ids).await
    }

    async fn messages_get_common<T, S: AsRef<str>>(
        &self,
        fmt: &str,
        ids: &[S],
    ) -> Result<Vec<MultiResult<T>>>
    where
        for<'de> T: Deserialize<'de> + MessageId,
    {
        let url = bbu();

        self.auth.check_refresh().await?;

        let mut body = String::new();
        let bound = "23121338-972e-11ea-a0c6-c3892af82e36";

        for (n, id) in ids.iter().enumerate() {
            body.push_str("--");
            body.push_str(bound);
            body.push_str("\r\n");

            body.push_str("Content-Type: application/http\r\n");
            body.push_str(&format!("Content-ID: req-{}\r\n", n));
            body.push_str("\r\n");

            body.push_str(&format!(
                "GET /gmail/v1/users/me/messages/{}?\
                    format={}\r\n",
                id.as_ref(),
                fmt
            ));
            body.push_str("\r\n");

            body.push_str("\r\n");
        }

        body.push_str("--");
        body.push_str(bound);
        body.push_str("--\r\n");

        trace!(self.log, "batch request: {:#?}", body);

        let buf = body.as_bytes().to_vec();

        let res = self
            .client
            .post(&url)
            .header(
                header::AUTHORIZATION,
                format!("Bearer {}", self.auth.access_token()),
            )
            .header(
                header::CONTENT_TYPE,
                format!(
                    "multipart/mixed; \
                boundary={}",
                    bound
                ),
            )
            .body(buf)
            .send()
            .await?
            .error_for_status()?;

        let rbnd = if let Some(ct) = res.headers().get(header::CONTENT_TYPE) {
            let ct: mime::Mime = ct.to_str()?.parse()?;
            if let Some(b) = ct.get_param("boundary") {
                b.to_string()
            } else {
                bail!("content type missing boundary");
            }
        } else {
            bail!("content type missing from response");
        };
        trace!(self.log, "boundary: {:#?}", rbnd);

        let x = res.bytes().await?;

        let mp = match multipart_parse(&x, rbnd.as_bytes()) {
            Ok(mp) => mp,
            Err(e) => {
                let report = if x.len() < 200 { &x } else { &x[..200] };
                debug!(
                    self.log,
                    "response: {:#?}",
                    String::from_utf8_lossy(report)
                );
                bail!("response multipart error: (boundary {:?}) {}", rbnd, e);
            }
        };

        let mut out: Vec<MultiResult<T>> = Vec::new();

        for p in &mp.parts {
            let report = if p.body.len() < 200 {
                &p.body
            } else {
                &p.body[..200]
            };
            trace!(
                self.log,
                "process part: {:#?} {:#?}",
                p.headers,
                String::from_utf8_lossy(report)
            );

            if let Some(ct) = p.headers.get("content-type") {
                let ct: mime::Mime = ct.parse()?;
                match (ct.type_(), ct.subtype().as_str()) {
                    (mime::APPLICATION, "http") => (),
                    ct => bail!("response part had wrong type: {:?}", ct),
                };
            } else {
                bail!("content type missing from response part");
            }

            let id: &str = if let Some(cid) = p.headers.get("content-id") {
                if let Some(n) = cid.strip_prefix("response-req-") {
                    let n: usize = n.parse()?;
                    if n < ids.len() {
                        ids[n].as_ref()
                    } else {
                        bail!("content id invalid in response part");
                    }
                } else {
                    bail!("content id invalid in response part");
                }
            } else {
                bail!("content type missing from response part");
            };

            let mut headers = [httparse::EMPTY_HEADER; 32];
            let mut parser = httparse::Response::new(&mut headers);
            let res = parser.parse(&p.body)?;

            if res.is_complete() {
                let c = res.unwrap();
                let status = parser.code.unwrap();

                let mut ct: Option<String> = None;
                let mut cl: Option<usize> = None;
                for h in &headers {
                    trace!(self.log, "part header: {:?}", h);
                    if h.name.to_ascii_lowercase() == "content-type" {
                        ct = Some(String::from_utf8(h.value.to_vec())?);
                    }
                    if h.name.to_ascii_lowercase() == "content-length" {
                        cl =
                            Some(String::from_utf8(h.value.to_vec())?.parse()?);
                    }
                }

                if ct.is_none() {
                    debug!(
                        self.log,
                        "response: {:#?}",
                        String::from_utf8_lossy(report)
                    );
                    bail!("headers missing from response part response");
                }

                #[allow(dead_code)]
                #[derive(Deserialize, Debug)]
                struct Eee {
                    domain: String,
                    reason: String,
                    message: String,
                }

                #[allow(dead_code)]
                #[derive(Deserialize, Debug)]
                struct Ee {
                    errors: Vec<Eee>,
                    code: u32,
                    message: String,
                }

                #[derive(Deserialize)]
                struct E {
                    error: Ee,
                }

                if status == 404 {
                    /*
                     * Report that this message was not found.
                     */
                    out.push(MultiResult::Missing(id.to_string()));
                    continue;
                } else if status == 429 {
                    out.push(MultiResult::RateLimit(id.to_string()));
                    continue;
                } else if status != 200 {
                    if status == 403 {
                        match serde_json::from_slice::<E>(&p.body[c..]) {
                            Ok(e) => {
                                debug!(
                                    self.log,
                                    "403 error: {}", e.error.message
                                );

                                let mut ok = false;
                                for ee in &e.error.errors {
                                    if ee.domain == "usageLimits"
                                        && (ee.reason
                                            == "userRateLimitExceeded"
                                            || ee.reason == "rateLimitExceeded")
                                    {
                                        out.push(MultiResult::RateLimit(
                                            id.to_string(),
                                        ));
                                        ok = true;
                                        break;
                                    }
                                }

                                if ok {
                                    continue;
                                }

                                bail!(
                                    "{} error for {}: {:?}",
                                    status,
                                    id,
                                    e.error
                                );
                            }
                            Err(e) => {
                                let b = String::from_utf8_lossy(&p.body[c..]);
                                debug!(self.log, "response: {}", b);
                                bail!("could not parse 403: {}", e);
                            }
                        }
                    }

                    let b = String::from_utf8_lossy(&p.body[c..]);
                    debug!(self.log, "response: {}", b);
                    bail!(
                        "inner response part had wrong status: {} for {}",
                        status,
                        id
                    );
                }

                let ct: mime::Mime = ct.unwrap().parse()?;
                match (ct.type_(), ct.subtype()) {
                    (mime::APPLICATION, mime::JSON) => (),
                    ct => {
                        bail!("response part response had wrong type: {:?}", ct)
                    }
                };

                if let Some(cl) = cl {
                    if cl != p.body.len() - c {
                        bail!(
                            "response part body len {} not what we \
                            expected (i.e., {})",
                            p.body.len() - c,
                            cl
                        );
                    }
                }

                out.push(MultiResult::Present(serde_json::from_slice(
                    &p.body[c..],
                )?));
            } else {
                bail!("response part response incomplete");
            }
        }

        if out.len() != ids.len() {
            bail!("did not get enough messages");
        }

        /*
         * Final (and obviously not optimal) checks for completeness:
         */
        for o in &out {
            let mut found = false;
            for i in ids {
                match o {
                    MultiResult::Present(msg) => {
                        if msg.id() == i.as_ref() {
                            found = true;
                            break;
                        }
                    }
                    MultiResult::Missing(id) | MultiResult::RateLimit(id) => {
                        if id.as_str() == i.as_ref() {
                            found = true;
                            break;
                        }
                    }
                }
            }
            if !found {
                bail!("message missing from response");
            }
        }

        Ok(out)
    }

    pub async fn message_get(&self, id: &str) -> Result<Message> {
        let url = bu(&format!("users/me/messages/{}", id));

        self.auth.check_refresh().await?;

        let res = self
            .client
            .get(&url)
            .header(
                header::AUTHORIZATION,
                format!("Bearer {}", self.auth.access_token()),
            )
            .query(&[("format", "metadata")])
            .send()
            .await?
            .error_for_status()?;

        Ok(res.json().await?)
    }

    pub async fn message_get_raw(&self, id: &str) -> Result<Vec<u8>> {
        let url = bu(&format!("users/me/messages/{}", id));

        self.auth.check_refresh().await?;

        let res = self
            .client
            .get(&url)
            .header(
                header::AUTHORIZATION,
                format!("Bearer {}", self.auth.access_token()),
            )
            .query(&[("format", "raw")])
            .send()
            .await?
            .error_for_status()?;

        let mr: MessageRaw = res.json().await?;

        Ok(base64::decode_config(mr.raw.as_bytes(), base64::URL_SAFE)?)
    }

    pub async fn thread_remove_label(
        &self,
        thread_id: &str,
        label: &str,
    ) -> Result<()> {
        let url = bu(&format!("users/me/threads/{}/modify", thread_id));

        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct RB<'a> {
            remove_label_ids: Vec<&'a str>,
        }

        self.auth.check_refresh().await?;

        self.client
            .post(&url)
            .header(
                header::AUTHORIZATION,
                format!("Bearer {}", self.auth.access_token()),
            )
            .json(&RB {
                remove_label_ids: vec![label],
            })
            .send()
            .await?
            .error_for_status()?;

        Ok(())
    }

    pub async fn labels_list(&self) -> Result<Vec<Label>> {
        let url = bu("users/me/labels");

        self.auth.check_refresh().await?;

        let res = self
            .client
            .get(&url)
            .header(
                header::AUTHORIZATION,
                format!("Bearer {}", self.auth.access_token()),
            )
            .send()
            .await?;

        if res.status() != StatusCode::OK {
            bail!("oddball response: {:#?}", res);
        }

        let o: serde_json::Value = res.json().await?;

        match o.get("labels") {
            None => bail!("missing \"labels\" in response"),
            Some(l) => Ok(serde_json::from_value(l.to_owned())?),
        }
    }
}
