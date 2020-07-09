use std::ffi::OsStr;
use std::io::{BufRead, BufReader, Lines};
use std::process::ChildStdout;

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

pub struct Messages {
    inner: Lines<BufReader<ChildStdout>>,
}

impl Messages {
    pub fn new(incoming: ChildStdout) -> Messages {
        Messages {
            inner: BufReader::new(incoming).lines(),
        }
    }
}

impl Iterator for Messages {
    type Item = Result<ProgressMessage, String>;

    fn next(&mut self) -> Option<Self::Item> {
        let message = self.inner.next()?;
        let message = match message {
            Ok(m) => m,
            Err(e) => return Some(Err(format!("Failed to read message: {}", e))),
        };
        Some(serde_json::from_str(&message).map_err(|e| format!("Failed to parse message: {}", e)))
    }
}
