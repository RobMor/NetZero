use time::Date;

use crate::config::Config;
use crate::mode::Mode;


pub struct CollectionTask {
    start_date: Date,
    end_date: Date,
    mode: Mode,
}

impl CollectionTask {

    pub fn new(start_date: Date, end_date: Date, config: Config, mode: Mode) -> CollectionTask {
        CollectionTask {
            start_date,
            end_date,
            mode,
        }
    }

    pub fn start(self) {
        ()
    }
}
