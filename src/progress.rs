use std::io::{stdout, Write};
use std::sync::{Arc, Mutex};

use tokio::stream::StreamExt;
use tokio::sync::mpsc;

use crossterm::{QueueableCommand, tty::IsTty, cursor, terminal, event};

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
            }
            ProgressMessage::SetProgress { progress, status } => {
                self.set_progress(progress);
                if let Some(status) = status {
                    self.set_status(status);
                }
            }
            ProgressMessage::SetStatus { status } => {
                self.set_status(status);
            }
            ProgressMessage::Reset => {
                self.reset();
            }
            ProgressMessage::Done => {
                self.done();
            }
        }
    }
}

enum TextBarMessage {
    Update,
    Done,
}

pub struct TextBar {
    channel: mpsc::UnboundedSender<TextBarMessage>,

    name: String,
    progress: usize,
    max: usize,
    status: String,
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

    pub fn to_string(&self, name_width: usize, bar_width: usize, status_width: usize) -> String {
        // TODO
        let frac = format!("{}/{}", self.progress, self.max);

        let bar_width = bar_width - frac.len() - 3;
        let bar_size = (bar_width as f64 * (self.progress as f64 / self.max as f64)) as usize;
        let bar = format!(
            "[{}>{}]",
            "=".repeat(bar_size),
            " ".repeat(bar_width - bar_size)
        );

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
        self.channel.send(TextBarMessage::Update).map_err(|e| e.to_string()).unwrap();
    }

    fn set_max(&mut self, value: usize) {
        self.max = value;
        self.channel.send(TextBarMessage::Update).map_err(|e| e.to_string()).unwrap();
    }

    fn set_progress(&mut self, value: usize) {
        self.progress = value;
        self.channel.send(TextBarMessage::Update).map_err(|e| e.to_string()).unwrap();
    }

    fn done(&mut self) {
        self.channel.send(TextBarMessage::Done).map_err(|e| e.to_string()).unwrap();
    }

    fn reset(&mut self) {
        self.progress = 0;
        self.max = 0;
        self.status = "".to_string();
        self.channel.send(TextBarMessage::Update).map_err(|e| e.to_string()).unwrap();
    }
}

enum TerminalBarMessage {
    TerminalMessage(crossterm::Result<event::Event>),
    BarMessage(TextBarMessage),
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

    fn print(bars: &Vec<Arc<Mutex<TextBar>>>) {
        let mut stdout = stdout();

        if stdout.is_tty() {
            let (cols, _) = terminal::size().unwrap(); // TODO unwrapping here?

            let name_width = ((cols as f32) * 0.1) as usize;
            let status_width = ((cols as f32) * 0.25) as usize;
            let bar_width = cols as usize - name_width - status_width - 3;

            // TODO is this unwrap safe
            stdout.queue(cursor::MoveUp(bars.len() as u16)).unwrap();

            for handle in bars {
                let bar = handle.lock().unwrap();
                let bar = bar.to_string(name_width, bar_width, status_width);

                stdout.queue(terminal::Clear(terminal::ClearType::CurrentLine)).unwrap();
                writeln!(stdout, "{}", bar).unwrap();
            }

            // TODO is this unwrap safe
            stdout.flush().unwrap();
        }
    }

    pub async fn print_until_complete(self) {
        let term_events = event::EventStream::new().map(|e| TerminalBarMessage::TerminalMessage(e));
        let bar_events = self.receiver.map(|e| TerminalBarMessage::BarMessage(e));

        let mut merged = bar_events.merge(term_events);
        
        let mut num_done = 0;

        while let Some(message) = merged.next().await {
            match message {
                TerminalBarMessage::BarMessage(m) => {
                    match m {
                        TextBarMessage::Update => TerminalBars::print(&self.bars),
                        TextBarMessage::Done => {
                            num_done += 1;
                            if num_done >= self.bars.len() {
                                break;
                            }
                        },
                    }
                },
                TerminalBarMessage::TerminalMessage(m) => {
                    match m {
                        Ok(event::Event::Resize(_, _)) => TerminalBars::print(&self.bars),
                        Ok(_) => (),
                        Err(e) => panic!("Somehow the terminal sent us an error: {}", e), // TODO how common is this??
                    }
                }
            }
        }
    }
}

