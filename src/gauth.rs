/*
 * Copyright 2022 Joshua M. Clulow <josh@sysmgr.org>
 * Copyright 2022 Oxide Computer Company
 */

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use reqwest::redirect;
use reqwest::StatusCode;
use reqwest::{Client, ClientBuilder};

use serde::Deserialize;

use slog::{debug, Logger};

use anyhow::{anyhow, bail, Result};

#[allow(dead_code)]
#[derive(Deserialize)]
struct RExchange {
    access_token: String,
    expires_in: u64,
    refresh_token: String,
    scope: String,
    token_type: String,
}

#[allow(dead_code)]
#[derive(Deserialize)]
struct RRefresh {
    access_token: String,
    scope: String,
    token_type: String,
    expires_in: u64,
}

#[derive(Clone)]
struct GAuthInner {
    refresh_token: String,
    access_token: String,
    expiry: Option<SystemTime>,
}

#[derive(Debug, Deserialize)]
pub struct ConfigInstalled {
    client_id: String,
    client_secret: String,
    auth_uri: String,
    token_uri: String,
}

#[derive(Debug, Deserialize)]
pub struct Config {
    installed: ConfigInstalled,
}

#[derive(Clone)]
pub struct GAuth {
    log: Logger,

    client_id: String,
    client_secret: String,
    auth_uri: reqwest::Url,
    token_uri: reqwest::Url,
    redirect_uri: String,

    client: Client,
    inner: Arc<Mutex<GAuthInner>>,
}

impl GAuth {
    pub fn new(log: Logger, config: Config) -> Result<GAuth> {
        /*
         * Even though, in its continuing war on users, Google have recklessly
         * deprecated the OOB redirect URI, we should not change the default
         * behaviour without a major roll -- especially when it is not clear
         * what the replacement would even be.
         */
        Self::new_with_redirect_uri(log, config, "urn:ietf:wg:oauth:2.0:oob")
    }

    pub fn new_with_redirect_uri(
        log: Logger,
        config: Config,
        redirect_uri: &str,
    ) -> Result<GAuth> {
        let cb = ClientBuilder::new()
            .tcp_keepalive(Duration::from_secs(30))
            .connect_timeout(Duration::from_secs(30))
            .redirect(redirect::Policy::none());

        Ok(GAuth {
            log,
            client: cb.build().expect("build client"),

            client_id: config.installed.client_id,
            client_secret: config.installed.client_secret,
            auth_uri: reqvalurl(&config.installed.auth_uri, "auth_uri")?,
            token_uri: reqvalurl(&config.installed.token_uri, "token_uri")?,
            redirect_uri: redirect_uri.to_string(),

            inner: Arc::new(Mutex::new(GAuthInner {
                refresh_token: String::from(""),
                access_token: String::from(""),
                expiry: None,
            })),
        })
    }

    pub fn access_token(&self) -> String {
        self.inner.lock().unwrap().access_token.to_string()
    }

    pub fn refresh_token(&self) -> String {
        self.inner.lock().unwrap().refresh_token.to_string()
    }

    pub fn set_refresh_token(&self, rt: &str) {
        self.inner.lock().unwrap().refresh_token = String::from(rt);
    }

    /**
     * Build a URL to give to the user, so that they can open it in their
     * browser and get an authentication code.  That code should then be passed
     * to exchange().
     */
    pub fn auth_token(&self, readonly: bool) -> Result<String> {
        let mut params: HashMap<&str, &str> = HashMap::new();
        params.insert("client_id", &self.client_id);
        params.insert("redirect_uri", &self.redirect_uri);
        params.insert("response_type", "code");

        let mut scope = String::from("profile");
        if readonly {
            scope.push_str(" https://www.googleapis.com/auth/gmail.readonly");
        } else {
            scope.push_str(" https://www.googleapis.com/auth/gmail.modify");
        }
        params.insert("scope", &scope);

        /*
         * We are only building a request here, not sending it to the server.
         * The URL we construct will be given to the user, and they will make a
         * request with their browser to authorise us.
         */
        let req = self
            .client
            .get(self.auth_uri.as_ref())
            .query(&params)
            .build()?;
        Ok(req.url().to_string())
    }

    /**
     * Exchange an authentication code from the user's browser to get a
     * permanent refresh token we can store.
     */
    pub async fn exchange(&self, code: &str) -> Result<()> {
        let mut params: HashMap<&str, &str> = HashMap::new();
        params.insert("code", code);
        params.insert("client_id", &self.client_id);
        params.insert("client_secret", &self.client_secret);
        params.insert("redirect_uri", &self.redirect_uri);
        params.insert("grant_type", "authorization_code");

        let res = self
            .client
            .post(self.token_uri.as_ref())
            .form(&params)
            .send()
            .await?;

        if res.status() != StatusCode::OK {
            bail!("oddball response: {:#?}", res);
        }
        debug!(self.log, "exchange response: {:#?}", &res);

        let oj: serde_json::Value = res.json().await?;
        debug!(self.log, "exchange body: {:#?}", &oj);

        let o: RExchange = serde_json::from_value(oj)?;

        let et = SystemTime::now()
            .checked_add(Duration::from_secs(o.expires_in * 2 / 3))
            .ok_or_else(|| anyhow!("invalid expiry time"))?;

        let mut i = self.inner.lock().unwrap();

        i.refresh_token = o.refresh_token;
        i.access_token = o.access_token;
        i.expiry = Some(et);

        Ok(())
    }

    pub async fn refresh(&self) -> Result<()> {
        let refresh_token = self.inner.lock().unwrap().refresh_token.clone();

        let mut params: HashMap<&str, &str> = HashMap::new();
        params.insert("client_id", &self.client_id);
        params.insert("client_secret", &self.client_secret);
        params.insert("refresh_token", &refresh_token);
        params.insert("grant_type", "refresh_token");

        let res = self
            .client
            .post(self.token_uri.as_ref())
            .form(&params)
            .send()
            .await?;

        if res.status() != reqwest::StatusCode::OK {
            bail!("oddball response: {:#?}", res);
        }

        let o: RRefresh = res.json().await?;

        let et = SystemTime::now()
            .checked_add(Duration::from_secs(o.expires_in * 2 / 3))
            .ok_or_else(|| anyhow!("invalid expiry time"))?;

        let mut i = self.inner.lock().unwrap();

        i.access_token = o.access_token;
        i.expiry = Some(et);

        Ok(())
    }

    pub async fn check_refresh(&self) -> Result<()> {
        let et = self.inner.lock().unwrap().expiry;

        let refresh = if self.inner.lock().unwrap().access_token == "" {
            debug!(self.log, "no auth token yet, refreshing");
            true
        } else if let Some(et) = et {
            if SystemTime::now() > et {
                debug!(self.log, "auth token expiry pending, refreshing");
                true
            } else {
                false
            }
        } else {
            debug!(self.log, "check_refresh: no expiry time?");
            false
        };

        if refresh {
            self.refresh().await?;
        }

        Ok(())
    }
}

fn reqvalurl<S: AsRef<str>>(val: S, n: &str) -> Result<reqwest::Url> {
    reqwest::Url::parse(val.as_ref())
        .map_err(|e| anyhow!("client_id.json URL \"{}\" invalid: {}", n, e))
}
