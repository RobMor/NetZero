use std::io::{stdout, Write};
use std::sync::{Arc, Mutex};
use std::fmt;

use tokio::stream::StreamExt;
use tokio::sync::mpsc;

use crossterm::{QueueableCommand, tty::IsTty, cursor, terminal, event};

use crate::protocol::ProgressMessage;

pub enum ProgressError {
    CommunicationError,
}

pub trait ProgressBar: Send {
    fn set_status(&mut self, status: String) -> Result<(), ProgressError>;
    fn set_max(&mut self, max: usize) -> Result<(), ProgressError>;
    fn set_progress(&mut self, progress: usize) -> Result<(), ProgressError>;
    fn reset(&mut self) -> Result<(), ProgressError>;
    fn done(&mut self) -> Result<(), ProgressError>;

    fn handle_message(&mut self, message: ProgressMessage) -> Result<(), ProgressError> {
        match message {
            ProgressMessage::SetMax { max, status } => {
                self.set_max(max)?;
                if let Some(status) = status {
                    self.set_status(status)?;
                }
            }
            ProgressMessage::SetProgress { progress, status } => {
                self.set_progress(progress)?;
                if let Some(status) = status {
                    self.set_status(status)?;
                }
            }
            ProgressMessage::SetStatus { status } => {
                self.set_status(status)?;
            }
            ProgressMessage::Reset => {
                self.reset()?;
            }
            ProgressMessage::Done => {
                self.done()?;
            }
        }

        Ok(())
    }
}

pub struct TextBar {
    channel: mpsc::UnboundedSender<TextBarMessage>,

    name: String,
    progress: usize,
    max: usize,
    status: String,
}

enum TextBarMessage {
    Update,
    Done,
}

impl TextBar {
    fn new(name: String, channel: mpsc::UnboundedSender<TextBarMessage>) -> TextBar {
        TextBar {
            channel: channel,
            name: name,
            progress: 0,
            max: 0,
            status: "".to_string(),
        }
    }
}

impl fmt::Display for TextBar {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        // TODO
        let name_width = 15;
        let bar_width = 50;
        let frac_width = 11;
        let status_width = 20;

        let bar = if self.progress >= self.max {
            format!("[{}]", "=".repeat(bar_width - 2))
        } else {
            let bar_width = bar_width - 3; // Subtract 3 for the decorations
            let bar_chars = (bar_width as f64 * ((self.progress as f64) / (self.max as f64))) as usize;
            format!("[{}>{}]", "=".repeat(bar_chars), " ".repeat(bar_width - bar_chars))
        };

        let frac = format!("{:>5}/{:<5}", self.progress, self.max);

        write!(
            fmt,
            "{:>name_width$} {:^bar_width$} {:^frac_width$} {:<status_width$}",
            self.name,
            bar,
            frac,
            self.status,
            name_width = name_width,
            bar_width = bar_width,
            frac_width = frac_width,
            status_width = status_width,
        )
    }
}

impl ProgressBar for TextBar {
    fn set_status(&mut self, status: String) -> Result<(), ProgressError> {
        self.status = status;
        self.channel.send(TextBarMessage::Update).map_err(|_| ProgressError::CommunicationError)
    }

    fn set_max(&mut self, value: usize) -> Result<(), ProgressError> {
        self.max = value;
        self.channel.send(TextBarMessage::Update).map_err(|_| ProgressError::CommunicationError)
    }

    fn set_progress(&mut self, value: usize) -> Result<(), ProgressError> {
        self.progress = value;
        self.channel.send(TextBarMessage::Update).map_err(|_| ProgressError::CommunicationError)
    }

    fn done(&mut self) -> Result<(), ProgressError> {
        self.channel.send(TextBarMessage::Done).map_err(|_| ProgressError::CommunicationError)
    }

    fn reset(&mut self) -> Result<(), ProgressError> {
        self.progress = 0;
        self.max = 0;
        self.status = "".to_string();
        self.channel.send(TextBarMessage::Update).map_err(|_| ProgressError::CommunicationError)
    }
}

pub struct TerminalBars {
    receiver: mpsc::UnboundedReceiver<TextBarMessage>,
    sender: mpsc::UnboundedSender<TextBarMessage>,

    bars: Vec<Arc<Mutex<TextBar>>>,
}

impl TerminalBars {
    pub fn new() -> Self {
        let (sender, receiver) = mpsc::unbounded_channel();

        TerminalBars {
            receiver,
            sender,
            bars: Vec::new(),
        }
    }

    pub fn new_bar(&mut self, name: String) -> Arc<Mutex<TextBar>> {
        let bar = Arc::new(Mutex::new(TextBar::new(name, self.sender.clone())));
        self.bars.push(bar.clone());
        bar
    }

    fn print(&self) {
        let mut stdout = stdout();

        if stdout.is_tty() {
            stdout.queue(cursor::MoveUp(self.bars.len() as u16)).expect("Failed to move cursor up");

            for handle in &self.bars {
                // TODO do we care about this unwrap?
                let bar = handle.lock().unwrap();

                stdout.queue(terminal::Clear(terminal::ClearType::CurrentLine)).expect("Failed to clear the current line");
                writeln!(stdout, "{}", bar).expect("Failed to draw progress bar");
            }

            stdout.flush().expect("Failed to flush progress bars to stdout");
        }
    }

    pub async fn print_until_complete(mut self) {
        let mut num_done = 0;

        while let Some(message) = self.receiver.next().await {
            match message {
                TextBarMessage::Update => self.print(),
                TextBarMessage::Done => {
                    num_done += 1;
                    if num_done >= self.bars.len() {
                        break;
                    }
                },
            }
        }
    }
}

