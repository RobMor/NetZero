use std::ffi::OsStr;

use tokio::prelude::*;
use tokio::stream::{Stream, StreamExt};
use tokio::io::BufReader;

use serde::Deserialize;

pub enum Purpose {
    Collect,
    Export,
}

impl AsRef<OsStr> for Purpose {
    fn as_ref(&self) -> &OsStr {
        match self {
            Purpose::Collect => "collect".as_ref(),
            Purpose::Export => "export".as_ref(),
        }
    }
}

#[derive(Deserialize, Debug)]
#[serde(tag = "type")]
pub enum ProgressMessage {
    SetMax {
        max: usize,
        status: Option<String>,
    },
    SetProgress {
        progress: usize,
        status: Option<String>,
    },
    SetStatus {
        status: String,
    },
    Reset,
    Done,
}

pub fn wrap(stream: impl AsyncRead) -> impl Stream<Item = Result<ProgressMessage, String>> {
    BufReader::new(stream).lines().map(|l| {
        l.map_err(|e| e.to_string())
        .and_then(|v| serde_json::from_str::<ProgressMessage>(&v).map_err(|e| e.to_string()))
    })
}