use std::fmt;

pub trait Progress: Send {
    fn set_status(&mut self, status: String);
    fn set_max(&mut self, value: usize);
    fn set_progress(&mut self, value: usize);
    fn reset(&mut self);
}

pub struct TerminalProgress {
    name: String,
    progress: usize,
    max: usize,
    status: String,
}

impl TerminalProgress {
    pub fn new(name: String) -> Self {
        TerminalProgress {
            name: name,
            progress: 0,
            max: 0,
            status: "".to_string(),
        }
    }
}

impl fmt::Display for TerminalProgress {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        if let Some(width) = formatter.width() {
            let bar_width = width - self.name.len() - self.status.len() - 3;

            write!(
                formatter,
                "{} {:width$} {}",
                self.name,
                TerminalBar(self.progress, self.max),
                self.status,
                width = bar_width
            )
        } else {
            todo!()
        }
    }
}

impl Progress for TerminalProgress {
    fn set_status(&mut self, status: String) {
        self.status = status;
    }

    fn set_max(&mut self, value: usize) {
        self.max = value;
    }

    fn set_progress(&mut self, value: usize) {
        self.progress = value;
    }

    fn reset(&mut self) {
        self.progress = 0;
        self.max = 0;
        self.status = "".to_string();
    }
}

struct TerminalBar(usize, usize);

impl fmt::Display for TerminalBar {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        let percentage = if self.1 > 0 {
            self.0 as f64 / self.1 as f64
        } else {
            0.0
        };

        if let Some(width) = formatter.width() {
            let size: usize = ((width - 2) as f64 * percentage).round() as usize;
            let bar = "=".repeat(size);
            let rest = " ".repeat(width - 2 - size);
            
            write!(formatter, "[{}>{}]", bar, rest)
        } else {
            todo!()
        }
    }
}
