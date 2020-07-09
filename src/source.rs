use std::ffi::OsStr;
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};

use time::Date;

use crate::config;
use crate::progress::ProgressBar;
use crate::protocol::{Messages, Purpose};

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

    fn start<I, K, V>(&self, purpose: Purpose, envs: I) -> Result<Messages, String>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<OsStr>,
        V: AsRef<OsStr>,
    {
        let mut command = Command::new(&self.command);

        command.args(&self.args);
        command.env("PURPOSE", purpose);
        command.envs(envs);

        command.stdout(Stdio::piped());

        // TODO Is there some better error handling we can do here?
        let child = command
            .spawn()
            .map_err(|e| format!("Failed to start process for {}: {}", self.name, e))?;

        // TODO can we safely unwrap here?
        let incoming = child
            .stdout
            .ok_or_else(|| format!("Failed to create incoming pipe from {}", self.name))?;

        Ok(Messages::new(incoming))
    }

    pub fn collect(
        &mut self,
        start_date: Option<Date>,
        end_date: Option<Date>,
    ) -> Result<(), String> {
        // TODO might be a cleaner way!
        let envs: Vec<(&str, String)> = start_date
            .iter()
            .map(|d| ("START_DATE", d.format("%Y-%m-%d")))
            .chain(end_date.map(|d| ("END_DATE", d.format("%Y-%m-%d"))))
            .collect();

        let messages = self.start(Purpose::Collect, envs)?;

        for message in messages {
            let message = message?;

            if let Some(progress) = &mut self.progress {
                let mut progress = progress.lock().unwrap(); // TODO Can we unwrap here?
                progress.handle_message(message);
            }
        }

        Ok(())
    }

    pub fn get_name(&self) -> String {
        self.name.clone()
    }
}
