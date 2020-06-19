use std::path::PathBuf;
use std::collections::HashMap;

use dirs::data_dir;
use serde::{Serialize, Deserialize};


#[derive(Serialize, Deserialize, Debug)]
pub struct Config {
    general: General,
    plugins: Option<HashMap<String, Plugin>>,
}

#[derive(Serialize, Deserialize, Debug)]
struct General {
    database: PathBuf,
}

#[derive(Serialize, Deserialize, Debug)]
struct Plugin {
    executable: PathBuf,
    protocol_version: u8,
}

impl Default for Config {
    fn default() -> Config {
        return Config {
            general: General {
                // TODO unwrapping here also shouldn't cause any problems but replace it anyways...
                database: dirs::data_dir().unwrap().join(PathBuf::from("netzero/data.sqlite3")),
            },
            plugins: None,
        }
    }
}
