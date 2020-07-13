use std::ffi::OsStr;
use std::fmt;

use tokio::io::BufReader;
use tokio::prelude::*;
use tokio::stream::{Stream, StreamExt};

use serde::Deserialize;

pub enum Error {
    IoError(std::io::Error),
    DeserializationError(String, serde_json::error::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match self {
            Self::IoError(e) => write!(fmt, "Error reading from protocol stream: {}", e),
            Self::DeserializationError(m, e) => {
                write!(fmt, "Error deserializing message `{}`: {}", m, e)
            }
        }
    }
}

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
pub enum Message {
    Progress { message: ProgressMessage },
    Done,
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
}

pub fn wrap(stream: impl AsyncRead) -> impl Stream<Item = Result<Message, Error>> {
    BufReader::new(stream).lines().map(|l| {
        l.map_err(|e| Error::IoError(e)).and_then(|v| {
            serde_json::from_str::<Message>(&v).map_err(|e| Error::DeserializationError(v, e))
        })
    })
}
