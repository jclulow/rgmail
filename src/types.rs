/*
 * Copyright 2022 Joshua M. Clulow <josh@sysmgr.org>
 */

use std::collections::HashSet;

use serde::Deserialize;
use serde_aux::prelude::*;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct RHistoryMessage {
    pub id: String,
    pub thread_id: String,
    #[serde(default)]
    pub label_ids: HashSet<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct RHistoryMessageWrap {
    pub message: RHistoryMessage,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct RHistoryLabels {
    pub label_ids: Vec<String>,
    pub message: RHistoryMessage,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
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
#[allow(dead_code)]
pub struct RHistory {
    #[serde(default)]
    pub history: Vec<RHistoryRecord>,
    pub next_page_token: Option<String>,
    #[serde(deserialize_with = "deserialize_number_from_string")]
    pub history_id: u64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct RMessage {
    id: String,
    thread_id: String,
}

#[allow(dead_code)]
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
#[allow(dead_code)]
pub struct RMessages {
    #[serde(default)]
    pub messages: Vec<RMessage>,
    pub next_page_token: Option<String>,
    pub result_size_estimate: u64,
}
