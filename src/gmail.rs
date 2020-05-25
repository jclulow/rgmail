/* vim: set tw=80: */

use std::collections::{VecDeque, HashSet};
use std::time::{Duration, SystemTime};

use serde::{Serialize, Deserialize};

use reqwest::blocking::{Client, ClientBuilder};
use reqwest::redirect;
use reqwest::StatusCode;
use reqwest::header;
use serde_aux::prelude::*;

use slog::{debug, Logger};

use super::Result;
use super::gauth::GAuth;
use super::multipart::multipart_parse;

pub struct GMail<'a> {
    log: Logger,
    auth: &'a GAuth,
    client: Client,
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

pub trait LabelsHelper {
    fn names(&self) -> Vec<&str>;
    fn id_of(&self, n: &str) -> Option<&str>;
}

impl LabelsHelper for Vec<Label> {
    fn names(&self) -> Vec<&str> {
        let mut names: Vec<&str> = self.iter().map(|l| {
            l.name.as_str()
        }).collect();
        names.sort();
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
        Ok(base64::decode_config(self.raw.as_bytes(), base64::URL_SAFE)?)
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
        SystemTime::from(SystemTime::UNIX_EPOCH)
            .checked_add(Duration::from_millis(self.internal_date))
            .expect("system time add")
    }

    pub fn age_days(&self) -> f64 {
        SystemTime::now()
            .duration_since(self.date()).expect("since")
            .as_secs_f64() / 86_400.
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
        SystemTime::from(SystemTime::UNIX_EPOCH)
            .checked_add(Duration::from_millis(ems))
            .expect("system time add")
    }

    pub fn age_days(&self) -> f64 {
        let ems: u64 = self.internal_date.parse().expect("system time");
        let then = SystemTime::from(SystemTime::UNIX_EPOCH)
            .checked_add(Duration::from_millis(ems))
            .expect("system time add");
        SystemTime::now()
            .duration_since(then).expect("since")
            .as_secs_f64() / 86_400.
    }
}

fn bu(s: &str) -> String {
    format!("https://www.googleapis.com/gmail/v1/{}", s)
}

fn bbu() -> String {
    format!("https://www.googleapis.com/batch/gmail/v1")
}

impl<'a> GMail<'a> {
    pub fn new(log: Logger, auth: &GAuth) -> GMail {
        let cb = ClientBuilder::new()
            .redirect(redirect::Policy::none());

        GMail {
            log,
            client: cb.build().expect("build client"),
            auth: auth,
        }
    }

    pub fn history_list(&self, start_at: u64) -> HistoryConfig{
        HistoryConfig {
            parent: self,
            label_id: None,
            history_types: Vec::new(),
            perpage: None,
            start_at,
        }
    }

    pub fn messages_list(&self) -> MessagesConfig {
        MessagesConfig {
            parent: self,
            q: None,
            spamtrash: false,
            label_ids: Vec::new(),
            perpage: None,
            resume_from_token: None,
        }
    }

    pub fn profile(&self) -> Result<Profile> {
        let url = bu("users/me/profile");

        self.auth.check_refresh()?;

        let res = self.client.get(&url)
            .header(header::AUTHORIZATION, format!("Bearer {}",
                self.auth.access_token()))
            .send()?
            .error_for_status()?;

        Ok(res.json()?)
    }

    pub fn message_get_min(&self, id: &str) -> Result<MessageMinimal> {
        let url = bu(&format!("users/me/messages/{}", id));

        self.auth.check_refresh()?;

        let res = self.client.get(&url)
            .header(header::AUTHORIZATION, format!("Bearer {}",
                self.auth.access_token()))
            .query(&[("format", "minimal")])
            .send()?
            .error_for_status()?;

        let t = res.text()?;

        match serde_json::from_str(&t) {
            Ok(r) => Ok(r),
            Err(e) => Err(format!("parsing response: {}: {}",
                e, t).into()),
        }
    }

    pub fn messages_get<S: AsRef<str>>(&self, ids: &[S])
        -> Result<Vec<MultiResult<MessageMinimal>>>
    {
        self.messages_get_common("minimal", ids)
    }

    pub fn messages_get_raw<S: AsRef<str>>(&self, ids: &[S])
        -> Result<Vec<MultiResult<MessageRaw>>>
    {
        self.messages_get_common("raw", ids)
    }

    fn messages_get_common<T, S: AsRef<str>>(&self,
        fmt: &str, ids: &[S])
        -> Result<Vec<MultiResult<T>>>
        where for<'de> T: Deserialize<'de> + MessageId
    {
        let url = bbu();

        self.auth.check_refresh()?;

        let mut body = String::new();
        let bound = "23121338-972e-11ea-a0c6-c3892af82e36";

        for (n, id) in ids.iter().enumerate() {
            body.push_str("--");
            body.push_str(bound);
            body.push_str("\r\n");

            body.push_str("Content-Type: application/http\r\n");
            body.push_str(&format!("Content-ID: req-{}\r\n", n));
            body.push_str("\r\n");

            body.push_str(&format!("GET /gmail/v1/users/me/messages/{}?\
                    format={}\r\n",
                id.as_ref(), fmt));
            body.push_str("\r\n");

            body.push_str("\r\n");
        }

        body.push_str("--");
        body.push_str(bound);
        body.push_str("--\r\n");

        let buf = body.as_bytes().to_vec();

        let res = self.client.post(&url)
            .header(header::AUTHORIZATION, format!("Bearer {}",
                self.auth.access_token()))
            .header(header::CONTENT_TYPE, format!("multipart/mixed; \
                boundary={}", bound))
            .body(buf)
            .send()?
            .error_for_status()?;

        let rbnd = if let Some(ct) = res.headers().get(header::CONTENT_TYPE) {
            let ct: mime::Mime = ct.to_str()?.parse()?;
            if let Some(b) = ct.get_param("boundary") {
                b.to_string()
            } else {
                return Err("content type missing boundary".into());
            }
        } else {
            return Err("content type missing from response".into());
        };

        let x = res.bytes()?;

        let mp = multipart_parse(&x, rbnd.as_bytes())?;

        let mut out: Vec<MultiResult<T>> = Vec::new();

        for p in &mp.parts {
            if let Some(ct) = p.headers.get("content-type") {
                let ct: mime::Mime = ct.parse()?;
                match (ct.type_(), ct.subtype().as_str()) {
                    (mime::APPLICATION, "http") => (),
                    ct => {
                        return Err(format!("response part had wrong \
                            type: {:?}", ct).into());
                    }
                };
            } else {
                return Err("content type missing from response part".into());
            }

            let id: &str = if let Some(cid) = p.headers.get("content-id") {
                if cid.starts_with("response-req-") {
                    let n: usize = cid[13..].parse()?;
                    if n < ids.len() {
                        ids[n].as_ref()
                    } else {
                        return Err("content id invalid in response \
                            part".into());
                    }
                } else {
                    return Err("content id invalid in response part".into());
                }
            } else {
                return Err("content type missing from response part".into());
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
                    if h.name.to_ascii_lowercase() == "content-type" {
                        ct = Some(String::from_utf8(h.value.to_vec())?);
                    }
                    if h.name.to_ascii_lowercase() == "content-length" {
                        cl = Some(String::from_utf8(h.value.to_vec())?
                            .parse()?);
                    }
                }

                if ct.is_none() || cl.is_none() {
                    return Err("headers missing from response part response"
                        .into());
                }

                #[allow(dead_code)]
                #[derive(Deserialize)]
                struct EEE {
                    domain: String,
                    reason: String,
                    message: String,
                }

                #[allow(dead_code)]
                #[derive(Deserialize)]
                struct EE {
                    errors: Vec<EEE>,
                    code: u32,
                    message: String,
                }

                #[derive(Deserialize)]
                struct E {
                    error: EE,
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
                } if status != 200 {
                    if status == 403 {
                        match serde_json::from_slice::<E>(&p.body[c..]) {
                            Ok(e) => {
                                debug!(self.log, "403 error: {}",
                                    e.error.message);

                                let mut ok = false;
                                for ee in &e.error.errors {
                                    if ee.domain == "usageLimits" &&
                                        ee.reason == "userRateLimitExceeded"
                                    {
                                        out.push(MultiResult::RateLimit(
                                            id.to_string()));
                                        ok = true;
                                        break;
                                    }
                                }

                                if ok {
                                    continue;
                                }
                            }
                            Err(e) => {
                                let b = String::from_utf8_lossy(&p.body[c..]);
                                debug!(self.log, "response: {}", b);
                                return Err(format!("could not parse 403:
                                    {}", e).into());
                            }
                        }
                    }

                    let b = String::from_utf8_lossy(&p.body[c..]);
                    debug!(self.log, "response: {}", b);
                    return Err(format!("inner response part had wrong \
                        status: {} for {}", status, id).into());
                }

                let ct: mime::Mime = ct.unwrap().parse()?;
                match (ct.type_(), ct.subtype()) {
                    (mime::APPLICATION, mime::JSON) => (),
                    ct => {
                        return Err(format!("response part response had wrong \
                            type: {:?}", ct).into());
                    }
                };

                let cl = cl.unwrap();
                if cl != p.body.len() - c {
                    return Err(format!("response part body len {} not what \
                        we expected (i.e., {})", p.body.len() - c, cl)
                        .into());
                }

                out.push(MultiResult::Present(
                    serde_json::from_slice(&p.body[c..])?));

            } else {
                return Err("response part response incomplete".into());
            }
        }

        if out.len() != ids.len() {
            return Err("did not get enough messages".into());
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
                return Err("message missing from response".into());
            }
        }

        Ok(out)
    }

    pub fn message_get(&self, id: &str) -> Result<Message> {
        let url = bu(&format!("users/me/messages/{}", id));

        self.auth.check_refresh()?;

        let res = self.client.get(&url)
            .header(header::AUTHORIZATION, format!("Bearer {}",
                self.auth.access_token()))
            .query(&[("format", "metadata")])
            .send()?
            .error_for_status()?;

        Ok(res.json()?)
    }

    pub fn message_get_raw(&self, id: &str) -> Result<Vec<u8>> {
        let url = bu(&format!("users/me/messages/{}", id));

        self.auth.check_refresh()?;

        let res = self.client.get(&url)
            .header(header::AUTHORIZATION, format!("Bearer {}",
                self.auth.access_token()))
            .query(&[("format", "raw")])
            .send()?
            .error_for_status()?;

        let mr: MessageRaw = res.json()?;

        Ok(base64::decode_config(mr.raw.as_bytes(), base64::URL_SAFE)?)
    }

    pub fn thread_remove_label(&self, thread_id: &str, label: &str)
        -> Result<()>
    {
        let url = bu(&format!("users/me/threads/{}/modify", thread_id));

        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct RB<'a> {
            remove_label_ids: Vec<&'a str>,
        }

        self.auth.check_refresh()?;

        self.client.post(&url)
            .header(header::AUTHORIZATION, format!("Bearer {}",
                self.auth.access_token()))
            .json(&RB { remove_label_ids: vec!(label) })
            .send()?
            .error_for_status()?;

        Ok(())
    }

    pub fn labels_list(&self) -> Result<Vec<Label>> {
        let url = bu("users/me/labels");

        self.auth.check_refresh()?;

        let res = self.client.get(&url)
            .header(header::AUTHORIZATION, format!("Bearer {}",
                self.auth.access_token()))
            .send()?;

        if res.status() != StatusCode::OK {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidData,
                format!("oddball response: {:#?}", res)).into());
        }

        let o: serde_json::Value = res.json()?;

        match o.get("labels") {
            None => {
                return Err(std::io::Error::new(std::io::ErrorKind::InvalidData,
                    format!("missing \"labels\" in response")).into());
            }
            Some(l) => {
                Ok(serde_json::from_value(l.to_owned())?)
            }
        }
    }
}

pub struct MessagesConfig<'a> {
    parent: &'a GMail<'a>,
    perpage: Option<u32>,
    q: Option<String>,
    spamtrash: bool,
    label_ids: Vec<String>,
    resume_from_token: Option<String>,
}

impl<'a> MessagesConfig<'a> {
    pub fn query<S: AsRef<str>>(mut self, s: S) -> MessagesConfig<'a> {
        self.q = Some(s.as_ref().to_string());
        self
    }

    pub fn include_spam_trash(mut self, i: bool) -> MessagesConfig<'a> {
        self.spamtrash = i;
        self
    }

    pub fn resume_from_token(mut self, s: &str) -> MessagesConfig<'a> {
        self.resume_from_token = Some(s.to_string());
        self
    }

    pub fn batch_size(mut self, n: u32) -> MessagesConfig<'a> {
        self.perpage = Some(n);
        self
    }

    pub fn labels_clear(mut self) -> MessagesConfig<'a> {
        self.label_ids.clear();
        self
    }

    pub fn label_add(mut self, label_id: &str) -> MessagesConfig<'a> {
        let s = label_id.to_string();

        if !self.label_ids.contains(&s) {
            self.label_ids.push(s);
        }

        self
    }

    pub fn start(self) -> Messages<'a> {
        Messages {
            fin: false,
            previous_token: None,
            page_token: self.resume_from_token.clone(),
            c: self,
            infl: VecDeque::new(),
        }
    }
}

pub struct Messages<'a> {
    fin: bool,
    previous_token: Option<String>,
    page_token: Option<String>,
    c: MessagesConfig<'a>,
    infl: VecDeque<RMessage>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RMessage {
    id: String,
    thread_id: String,
}

impl RMessage {
    pub fn id(&self) -> &str {
        self.id.as_str()
    }

    pub fn thread_id(&self) -> &str {
        self.thread_id.as_str()
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RMessages {
    messages: Vec<RMessage>,
    next_page_token: Option<String>,
    result_size_estimate: u64,
}

impl<'a> Messages<'a> {
    pub fn resume_token(&self) -> Option<String> {
        self.previous_token.clone()
    }

    fn fetch_page(&mut self) -> Result<RMessages> {
        let log = &self.c.parent.log;

        debug!(log, "requesting more message IDs (pt {:?})", self.page_token);

        let url = bu("users/me/messages");

        self.c.parent.auth.check_refresh()?;

        let mut req = self.c.parent.client.get(&url)
            .header(header::AUTHORIZATION, format!("Bearer {}",
                self.c.parent.auth.access_token()));

        if let Some(q) = &self.c.q {
            req = req.query(&[("q", q)]);
        }
        if self.c.spamtrash {
            req = req.query(&[("includeSpamTrash", "true")]);
        }
        for l in &self.c.label_ids {
            req = req.query(&[("labelIds", l)]);
        }
        if let Some(pt) = &self.page_token {
            req = req.query(&[("pageToken", pt)]);
        }
        if let Some(pp) = &self.c.perpage {
            req = req.query(&[("maxResults", pp.to_string())]);
        }

        let req = req.build()?;
        debug!(log, "request for page: {}", req.url());

        let res = self.c.parent.client.execute(req)?.error_for_status()?;

        Ok(res.json()?)
    }

    fn next_(&mut self) -> Option<Result<RMessage>> {
        let log = &self.c.parent.log;

        loop {
            if let Some(rm) = self.infl.pop_front() {
                return Some(Ok(rm));
            }

            if self.fin {
                debug!(log, "finished completely!");
                return None;
            }

            let o = match self.fetch_page() {
                Ok(o) => o,
                Err(e) => return Some(Err(e)),
            };

            debug!(log, "result count estimate: {}", o.result_size_estimate);

            debug!(log, "new next page token: {:?}", o.next_page_token);
            self.previous_token = self.page_token.clone();
            self.page_token = o.next_page_token;
            if self.page_token.is_none() {
                /*
                 * If we do not have a next page token, the stream is finished
                 * once we dispatch all messages.
                 */
                self.fin = true;
            }

            debug!(log, "got {} messages", o.messages.len());
            for rm in o.messages {
                self.infl.push_back(rm);
            }
        }
    }
}

impl<'a> Iterator for Messages<'a> {
    type Item = Result<RMessage>;

    fn next(&mut self) -> Option<Result<RMessage>> {
        self.next_()
    }
}

pub struct HistoryConfig<'a> {
    parent: &'a GMail<'a>,
    perpage: Option<u32>,
    label_id: Option<String>,
    history_types: Vec<String>,
    start_at: u64,
}

impl<'a> HistoryConfig<'a> {
    pub fn batch_size(mut self, n: u32) -> HistoryConfig<'a> {
        self.perpage = Some(n);
        self
    }

    pub fn history_types_clear(mut self) -> HistoryConfig<'a> {
        self.history_types.clear();
        self
    }

    pub fn history_type_add(mut self, history_type: &str)
        -> HistoryConfig<'a>
    {
        let s = history_type.to_string();

        if !self.history_types.contains(&s) {
            self.history_types.push(s);
        }

        self
    }

    pub fn label(mut self, label_id: &str) -> HistoryConfig<'a> {
        self.label_id = Some(label_id.to_string());
        self
    }

    pub fn start(self) -> History<'a> {
        History {
            fin: false,
            page_token: None,
            c: self,
            infl: VecDeque::new(),
            final_id: None,
        }
    }
}

pub struct History<'a> {
    fin: bool,
    page_token: Option<String>,
    c: HistoryConfig<'a>,
    infl: VecDeque<RHistoryRecord>,
    final_id: Option<u64>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RHistoryMessage {
    pub id: String,
    pub thread_id: String,
    #[serde(default)]
    pub label_ids: HashSet<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RHistoryMessageWrap {
    pub message: RHistoryMessage,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RHistoryLabels {
    pub label_ids: Vec<String>,
    pub message: RHistoryMessage,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RHistoryRecord {
    #[serde(deserialize_with = "deserialize_number_from_string")]
    pub id: u64,
    pub messages: Vec<RMessage>,
    pub messages_added: Option<Vec<RHistoryMessageWrap>>,
    pub messages_deleted: Option<Vec<RHistoryMessageWrap>>,
    pub labels_removed: Option<Vec<RHistoryLabels>>,
    pub labels_added: Option<Vec<RHistoryLabels>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RHistory {
    #[serde(default)]
    pub history: Vec<RHistoryRecord>,
    pub next_page_token: Option<String>,
    #[serde(deserialize_with = "deserialize_number_from_string")]
    pub history_id: u64,
}

impl<'a> History<'a> {
    pub fn final_id(&self) -> u64 {
        self.final_id.unwrap()
    }

    fn fetch_page(&mut self) -> Result<RHistory> {
        let log = &self.c.parent.log;

        debug!(log, "requesting more histories (pt {:?})", self.page_token);

        let url = bu("users/me/history");

        self.c.parent.auth.check_refresh()?;

        let mut req = self.c.parent.client.get(&url)
            .header(header::AUTHORIZATION, format!("Bearer {}",
                self.c.parent.auth.access_token()));

        req = req.query(&[("startHistoryId",
            self.c.start_at.to_string())]);
        if let Some(label_id) = &self.c.label_id {
            req = req.query(&[("labelId", label_id)]);
        }
        for t in &self.c.history_types {
            req = req.query(&[("historyTypes", t)]);
        }
        if let Some(pt) = &self.page_token {
            req = req.query(&[("pageToken", pt)]);
        }
        if let Some(pp) = &self.c.perpage {
            req = req.query(&[("maxResults", pp.to_string())]);
        }

        let req = req.build()?;
        debug!(log, "request for page: {}", req.url());

        let res = self.c.parent.client.execute(req)?.error_for_status()?;

        Ok(res.json()?)
    }
}

impl<'a> Iterator for &mut History<'a> {
    type Item = Result<RHistoryRecord>;

    fn next(&mut self) -> Option<Result<RHistoryRecord>> {
        let log = &self.c.parent.log;

        loop {
            if let Some(rm) = self.infl.pop_front() {
                return Some(Ok(rm));
            }

            if self.fin {
                debug!(log, "finished completely!");
                return None;
            }

            debug!(log, "requesting more histories (pt {:?})", self.page_token);
            let o = match self.fetch_page() {
                Ok(o) => o,
                Err(e) => return Some(Err(e)),
            };

            debug!(log, "new next page token: {:?}", o.next_page_token);
            self.page_token = o.next_page_token;
            if self.page_token.is_none() {
                /*
                 * If we do not have a next page token, the stream is finished
                 * once we dispatch all messages.
                 */
                self.fin = true;
                self.final_id = Some(o.history_id);
            }

            debug!(log, "got {} history records", o.history.len());
            for hr in o.history {
                self.infl.push_back(hr);
            }
        }
    }
}
