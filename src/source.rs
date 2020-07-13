use std::ffi::OsStr;
use std::process::Stdio;
use std::sync::{Arc, Mutex};

use time::Date;
use tokio::process::{Child, Command};
use tokio::stream::StreamExt;

use crate::config;
use crate::progress::ProgressBar;
use crate::protocol;

pub struct Source {
    name: String,
    command: String,
    args: Vec<String>,
    progress: Option<Arc<Mutex<dyn ProgressBar>>>,
}

impl Source {
    pub fn from_config(config: &config::Source) -> Source {
        Source {
            name: config.name.clone(),
            command: config.command.clone(),
            args: config.args.clone(),
            progress: None,
        }
    }

    pub fn use_progress(&mut self, progress: Arc<Mutex<dyn ProgressBar>>) {
        self.progress = Some(progress);
    }

    fn start<I, K, V>(&self, purpose: protocol::Purpose, envs: I) -> Result<Child, String>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<OsStr>,
        V: AsRef<OsStr>,
    {
        let mut command = Command::new(&self.command);

        // TODO kill on drop?
        command.args(&self.args);
        command.env("PURPOSE", purpose);
        command.envs(envs);

        command.stdout(Stdio::piped());

        // TODO Is there some better error handling we can do here?
        let child = command
            .spawn()
            .map_err(|e| format!("Failed to start process for {}: {}", self.name, e))?;

        Ok(child)
    }

    pub async fn collect(
        &mut self,
        start_date: Option<Date>,
        end_date: Option<Date>,
    ) -> Result<(), String> {
        let start_date = start_date.map(|d| ("START_DATE", d.format("%Y-%m-%d")));
        let end_date = end_date.map(|d| ("END_DATE", d.format("%Y-%m-%d")));
        let envs = start_date.into_iter().chain(end_date.into_iter());

        let child = self.start(protocol::Purpose::Collect, envs)?;
        self.run(child).await
    }

    async fn run(&mut self, child: Child) -> Result<(), String> {
        let mut messages = protocol::wrap(child.stdout.unwrap());

        while let Some(message) = messages.next().await {
            let message = message.map_err(|e| e.to_string())?;

            match message {
                protocol::Message::Progress { message } => {
                    if let Some(progress) = &mut self.progress {
                        let mut progress = progress.lock().unwrap(); // TODO Should we unwrap here?
                        progress
                            .handle_message(message)
                            .map_err(|e| format!("Failed to update progress bar: {}", e))?;
                    }
                }
                protocol::Message::Done => {
                    break;
                }
            }
        }

        Ok(())
    }

    pub fn get_name(&self) -> String {
        self.name.clone()
    }
}
