use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use console::Term;
use crossbeam::{channel, select};

use crate::protocol::ProgressMessage;

pub trait ProgressBar: Send {
    fn set_status(&mut self, status: String);
    fn set_max(&mut self, max: usize);
    fn set_progress(&mut self, progress: usize);
    fn reset(&mut self);
    fn done(&mut self);

    fn handle_message(&mut self, message: ProgressMessage) {
        match message {
            ProgressMessage::SetMax { max, status } => {
                self.set_max(max);
                if let Some(status) = status {
                    self.set_status(status);
                }
            },
            ProgressMessage::SetProgress { progress, status } => {
                self.set_progress(progress);
                if let Some(status) = status {
                    self.set_status(status);
                }
            },
            ProgressMessage::SetStatus { status } => {
                self.set_status(status);
            },
            ProgressMessage::Reset => {
                self.reset();
            },
            ProgressMessage::Done => {
                self.done();
            },
        }
    }
}

enum TextBarMessage {
    Update,
    Done,
}

pub struct TextBar {
    channel: channel::Sender<TextBarMessage>,

    name: String,
    progress: usize,
    max: usize,
    status: String,
}

impl TextBar {
    fn new(name: String, channel: channel::Sender<TextBarMessage>) -> TextBar {
        TextBar {
            channel: channel,
            name: name,
            progress: 0,
            max: 0,
            status: "".to_string(),
        }
    }

    pub fn to_string(&self, name_width: usize, bar_width: usize, status_width: usize) -> String {
        // TODO
        let frac = format!("{}/{}", self.progress, self.max);

        let bar_width = bar_width - frac.len() - 3;
        let bar_size = (bar_width as f64 * (self.progress as f64 / self.max as f64)) as usize;
        let bar = format!("[{}>{}]", "=".repeat(bar_size), " ".repeat(bar_width - bar_size));

        format!(
            "{:<name_width$} {} {} {:>status_width$}",
            self.name,
            bar,
            frac,
            self.status,
            name_width = name_width,
            status_width = status_width
        )
    }
}

impl ProgressBar for TextBar {
    fn set_status(&mut self, status: String) {
        self.status = status;
        self.channel.send(TextBarMessage::Update).unwrap();
    }

    fn set_max(&mut self, value: usize) {
        self.max = value;
        self.channel.send(TextBarMessage::Update).unwrap();
    }

    fn set_progress(&mut self, value: usize) {
        self.progress = value;
        self.channel.send(TextBarMessage::Update).unwrap();
    }

    fn done(&mut self) {
        self.channel.send(TextBarMessage::Done).unwrap();
    }

    fn reset(&mut self) {
        self.progress = 0;
        self.max = 0;
        self.status = "".to_string();
        self.channel.send(TextBarMessage::Update).unwrap();
    }
}

pub struct TerminalBars {
    receiver: channel::Receiver<TextBarMessage>,
    sender: channel::Sender<TextBarMessage>,
    term: Term,

    bars: Vec<Arc<Mutex<TextBar>>>,
}

impl TerminalBars {
    pub fn new() -> Self {
        let (sender, receiver) = channel::unbounded();

        TerminalBars {
            receiver,
            sender,
            term: Term::buffered_stdout(),
            bars: Vec::new(),
        }
    }

    pub fn new_bar(&mut self, name: String) -> Arc<Mutex<TextBar>> {
        let bar = Arc::new(Mutex::new(TextBar::new(name, self.sender.clone())));
        self.bars.push(bar.clone());
        bar
    }

    fn print(&self) {
        if self.term.is_term() {
            let (_, width) = self.term.size();

            let name_width = ((width as f32) * 0.1) as usize;
            let status_width = ((width as f32) * 0.25) as usize;
            let bar_width = width as usize - name_width - status_width - 3;
            
            // TODO is this unwrap safe
            self.term.move_cursor_up(self.bars.len()).unwrap();

            for handle in &self.bars {
                let bar = handle.lock().unwrap();
                self.term.clear_line().unwrap();
                self.term
                    .write_line(&bar.to_string(name_width, bar_width, status_width))
                    .unwrap();
            }

            // TODO is this unwrap safe
            self.term.flush().unwrap();
        }
    }

    pub fn print_until_complete(&self) {
        let mut num_done = 0;

        while num_done < self.bars.len() {
            select! {
                recv(self.receiver) -> message => {
                    match message.unwrap() {
                        TextBarMessage::Update => self.print(),
                        TextBarMessage::Done => num_done += 1,
                    }
                },
                default(Duration::from_secs(1)) => self.print(),
            }
        }
    }
}
