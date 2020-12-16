/* vim: set tw=80: */

use std::collections::HashMap;
use std::time::{Duration, SystemTime};
use std::cell::RefCell;

use reqwest::blocking::{Client, ClientBuilder};
use reqwest::StatusCode;
use reqwest::header;
use reqwest::redirect;

use serde::Deserialize;

use slog::{debug, Logger};

use anyhow::{Result, bail, anyhow};

#[allow(dead_code)]
#[derive(Deserialize)]
struct RExchange {
    access_token: String,
    refresh_token: String,
    scope: String,
    token_type: String,
    expiry_date: u64,
}

#[allow(dead_code)]
#[derive(Deserialize)]
struct RRefresh {
    access_token: String,
    scope: String,
    token_type: String,
    expires_in: u64,
}

struct GAuthInner {
    refresh_token: String,
    access_token: String,
    expiry: Option<SystemTime>,
}

#[derive(Debug,Deserialize)]
pub struct ConfigInstalled {
    client_id: String,
    client_secret: String,
    auth_uri: String,
    token_uri: String,
}

#[derive(Debug,Deserialize)]
pub struct Config {
    installed: ConfigInstalled,
}

pub struct GAuth {
    ga_log: Logger,

    ga_client_id: String,
    ga_client_secret: String,
    ga_auth_uri: reqwest::Url,
    ga_token_uri: reqwest::Url,

    ga_client: Client,
    ga_inner: RefCell<GAuthInner>,
}

impl GAuth {
    pub fn new(log: Logger, config: Config) -> Result<GAuth> {
        let cb = ClientBuilder::new()
            .redirect(redirect::Policy::none());

        Ok(GAuth {
            ga_log: log,
            ga_client: cb.build().expect("build client"),

            ga_client_id: config.installed.client_id,
            ga_client_secret: config.installed.client_secret,
            ga_auth_uri: reqvalurl(&config.installed.auth_uri, "auth_uri")?,
            ga_token_uri: reqvalurl(&config.installed.token_uri, "token_uri")?,

            ga_inner: RefCell::new(GAuthInner {
                refresh_token: String::from(""),
                access_token: String::from(""),
                expiry: None,
            }),
        })
    }

    pub fn access_token(&self) -> String {
        self.ga_inner.borrow().access_token.to_string()
    }

    pub fn refresh_token(&self) -> String {
        self.ga_inner.borrow().refresh_token.to_string()
    }

    pub fn set_refresh_token(&self, rt: &str) {
        self.ga_inner.borrow_mut().refresh_token = String::from(rt);
    }

    pub fn auth_token(&self, readonly: bool) -> Result<String> {
        let mut params: HashMap<&str, &str> = HashMap::new();
        params.insert("client_id", &self.ga_client_id);
        params.insert("redirect_uri", "urn:ietf:wg:oauth:2.0:oob");
        params.insert("response_type", "code");

        let mut scope = String::from("profile");
        if readonly {
            scope.push_str(" https://www.googleapis.com/auth/gmail.readonly");
        } else {
            scope.push_str(" https://www.googleapis.com/auth/gmail.modify");
        }
        params.insert("scope", &scope);

        let res = self.ga_client.get(self.ga_auth_uri.as_ref())
            .query(&params)
            .send()?;

        if res.status() != StatusCode::FOUND {
            bail!("oddball response: {:#?}", res);
        }

        if let Some(l) = res.headers().get(header::LOCATION) {
            Ok(String::from(l.to_str().unwrap()))
        } else {
            bail!("oddball response (no location): {:#?}", res);
        }
    }

    pub fn exchange(&self, code: &str) -> Result<()> {
        let mut params: HashMap<&str, &str> = HashMap::new();
        params.insert("code", code);
        params.insert("client_id", &self.ga_client_id);
        params.insert("client_secret", &self.ga_client_secret);
        params.insert("redirect_uri", "urn:ietf:wg:oauth:2.0:oob");
        params.insert("grant_type", "authorization_code");

        let res = self.ga_client.post(self.ga_token_uri.as_ref())
            .form(&params)
            .send()?;

        if res.status() != StatusCode::OK {
            bail!("oddball response: {:#?}", res);
        }
        debug!(self.ga_log, "exchange response: {:#?}", &res);

        let o: RExchange = res.json()?;

        let et = SystemTime::UNIX_EPOCH
            .checked_add(Duration::from_millis(o.expiry_date - 600_000))
            .ok_or_else(|| anyhow!("invalid expiry time"))?;

        let mut i = self.ga_inner.borrow_mut();

        i.refresh_token = o.refresh_token;
        i.access_token = o.access_token;
        i.expiry = Some(et);

        Ok(())
    }

    pub fn refresh(&self) -> Result<()> {
        let mut i = self.ga_inner.borrow_mut();

        let mut params: HashMap<&str, &str> = HashMap::new();
        params.insert("client_id", &self.ga_client_id);
        params.insert("client_secret", &self.ga_client_secret);
        params.insert("refresh_token", &i.refresh_token);
        params.insert("grant_type", "refresh_token");

        let res = self.ga_client.post(self.ga_token_uri.as_ref())
            .form(&params)
            .send()?;

        if res.status() != reqwest::StatusCode::OK {
            bail!("oddball response: {:#?}", res);
        }

        let o: RRefresh = res.json()?;

        let et = SystemTime::now()
            .checked_add(Duration::from_secs(o.expires_in * 2 / 3))
            .ok_or_else(|| anyhow!("invalid expiry time"))?;

        i.access_token = o.access_token;
        i.expiry = Some(et);

        Ok(())
    }

    pub fn check_refresh(&self) -> Result<()> {
        let et = self.ga_inner.borrow().expiry;

        if let Some(et) = et {
            if SystemTime::now() > et {
                debug!(self.ga_log, "auth token expiry pending, refreshing");
                self.refresh()?;
            }
        } else {
            debug!(self.ga_log, "check_refresh: no expiry time?");
        }

        Ok(())
    }
}

fn reqvalurl<S: AsRef<str>>(val: S, n: &str) -> Result<reqwest::Url> {
    reqwest::Url::parse(val.as_ref())
        .map_err(|e| {
            anyhow!("client_id.json URL \"{}\" invalid: {}", n, e)
        })
}
