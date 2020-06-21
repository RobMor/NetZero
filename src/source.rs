use std::ffi::OsStr;
use std::io::{BufRead, BufReader, BufWriter, Lines, Read, Write};
use std::process::{ChildStdin, ChildStdout, Command, Stdio};

use serde::{Deserialize, Serialize};
use time::Date;

use crate::config::Manifest;

enum Purpose {
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

pub struct OutgoingPipe {
    inner: BufWriter<ChildStdin>,
}

#[derive(Serialize, Debug)]
#[serde(tag = "type")]
pub enum OutgoingMessage {
    Start,
    Stop,
}

impl OutgoingPipe {
    pub fn new(outgoing: ChildStdin) -> OutgoingPipe {
        OutgoingPipe {
            inner: BufWriter::new(outgoing),
        }
    }

    pub fn send(&mut self, message: OutgoingMessage) -> Result<(), String> {
        let message = serde_json::to_string(&message)
            .map_err(|e| format!("Failed to serialize message: {}", e))?;

        writeln!(self.inner, "{}", message)
            .map_err(|e| format!("Failed to write message: {}", e))?;

        Ok(())
    }

    pub fn flush(&mut self) {
        self.inner.flush();
    }
}

pub struct IncomingPipe {
    inner: Lines<BufReader<ChildStdout>>,
}

#[derive(Deserialize, Debug)]
#[serde(tag = "type")]
pub enum IncomingMessage {
    Starting,
    SetMax { value: usize, status: Option<String> },
    SetProgress { progress: usize, status: Option<String> },
    SetStatus { status: String },
    Reset,
    Done,
}

impl IncomingPipe {
    pub fn new(incoming: ChildStdout) -> IncomingPipe {
        IncomingPipe {
            inner: BufReader::new(incoming).lines(),
        }
    }
}

impl Iterator for IncomingPipe {
    type Item = Result<IncomingMessage, String>;

    fn next(&mut self) -> Option<Self::Item> {
        let message = self.inner.next()?;
        let message = match message {
            Ok(m) => m,
            Err(e) => return Some(Err(format!("Failed to read message: {}", e))),
        };
        Some(serde_json::from_str(&message).map_err(|e| format!("Failed to parse message: {}", e)))
    }
}

#[derive(Debug)]
pub struct Source {
    name: String,
    command: String,
    args: Vec<String>,
}

impl Source {
    pub fn from_manifest(manifest: Manifest) -> Source {
        Source {
            name: manifest.name,
            command: manifest.command,
            args: manifest.args,
        }
    }

    fn start<I, K, V>(
        &self,
        purpose: Purpose,
        envs: I,
    ) -> Result<(IncomingPipe, OutgoingPipe), String>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<OsStr>,
        V: AsRef<OsStr>,
    {
        let mut command = Command::new(&self.command);

        command.args(&self.args);
        command.env("PURPOSE", purpose);
        command.envs(envs);

        command.stdin(Stdio::piped());
        command.stdout(Stdio::piped());

        println!("Command: {:?}", command);

        // TODO Is there some better error handling we can do here?
        let child = command
            .spawn()
            .map_err(|e| format!("Failed to start process for {}: {}", self.name, e))?;

        // TODO can we safely unwrap here?
        let outgoing = child
            .stdin
            .ok_or_else(|| format!("Failed to create outgoing pipe to {}", self.name))?;
        let incoming = child
            .stdout
            .ok_or_else(|| format!("Failed to create incoming pipe from {}", self.name))?;

        Ok((IncomingPipe::new(incoming), OutgoingPipe::new(outgoing)))
    }

    pub fn collect(&self, start_date: Option<Date>, end_date: Option<Date>) -> Result<(), String> {
        // TODO might be a cleaner way!
        let envs: Vec<(&str, String)> = start_date
            .iter()
            .map(|d| ("START_DATE", d.format("%Y-%m-%d")))
            .chain(end_date.map(|d| ("END_DATE", d.format("%Y-%m-%d"))))
            .collect();

        println!("Environment Variables {:?}", envs);

        let (mut incoming, mut outgoing) = self.start(Purpose::Collect, envs)?;

        outgoing.send(OutgoingMessage::Start)?;

        for message in incoming {
            let message = message?;

            match message {
                IncomingMessage::Starting => {
                    todo!()
                },
                IncomingMessage::SetMax { value, status } => {
                    todo!()
                },
                IncomingMessage::SetProgress { progress, status } => {
                    todo!()
                },
                IncomingMessage::SetStatus { status } => {
                    todo!()
                },
                IncomingMessage::Reset => {
                    todo!()
                },
                IncomingMessage::Done => {
                    todo!()
                },
            }
        }

        println!("Done");

        Ok(())
    }
}
