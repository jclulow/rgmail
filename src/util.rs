/*
 * Copyright 2022 Joshua M. Clulow <josh@sysmgr.org>
 */

pub fn bu(s: &str) -> String {
    format!("https://www.googleapis.com/gmail/v1/{}", s)
}

pub fn bbu() -> String {
    "https://www.googleapis.com/batch/gmail/v1".to_string()
}
