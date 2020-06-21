use std::collections::HashMap;
use std::fs::{create_dir_all, read_to_string, write};
use std::path::{Path, PathBuf};

extern crate dirs;
use serde::{Deserialize, Serialize};

use crate::source;

#[derive(Serialize, Deserialize, Debug)]
pub struct Config {
    general: General,
    #[serde(flatten)] // TODO
    sources: HashMap<String, Source>,
}

#[derive(Serialize, Deserialize, Debug)]
struct General {
    database: PathBuf,
}

#[derive(Serialize, Deserialize, Debug)]
struct Source {
    manifest: PathBuf,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Manifest {
    pub name: String,
    pub command: String,
    pub args: Vec<String>,
}

impl Default for Config {
    fn default() -> Config {
        // let mut test_map = HashMap::new();

        // test_map.insert("testing".to_string(), Source {
        //     manifest: PathBuf::from("whatever")
        // });
        // test_map.insert("testing2".to_string(), Source {
        //     manifest: PathBuf::from("whatever2")
        // });

        return Config {
            general: General {
                // TODO unwrapping here also shouldn't cause any problems but replace it anyways...
                database: dirs::data_dir()
                    .unwrap()
                    .join(PathBuf::from("netzero/data.sqlite3")),
            },
            sources: HashMap::new(),
        };
    }
}

impl Config {
    pub fn setup() -> Result<Config, String> {
        let config_dir = dirs::config_dir()
            .unwrap() // TODO unwrapping here shouldn't cause issues but replace it anyways...
            .join(Path::new("netzero"));

        if !config_dir.exists() {
            create_dir_all(&config_dir)
                .map_err(|e| format!("Could not create config directory: {}", e))?;
        }

        let config_path = config_dir.join(Path::new("config.toml"));

        let config = if !config_path.exists() {
            let config = Config::default();
            write(config_path, toml::to_string(&config).unwrap())
                .map_err(|e| format!("Could not write default config: {}", e))?;
            config
        } else {
            let contents = read_to_string(config_path)
                .map_err(|e| format!("Could not read configuration file: {}", e))?;
            toml::from_str(&contents)
                .map_err(|e| format!("Could not parse configuration file: {}", e))?
        };

        println!("{:?}", config);

        Ok(config)
    }

    pub fn all_sources(&self) -> Result<Vec<source::Source>, String> {
        // This is cool!
        self.sources
            .iter()
            .map(|(k, v)| load_source(k, v))
            .collect()
    }

    pub fn get_source(&self, source: &str) -> Result<source::Source, String> {
        let source_config = self
            .sources
            .get(source)
            .ok_or_else(|| format!("Unrecognized source name {}", source))?;

        load_source(source, source_config)
    }
}

fn load_source(name: &str, source_config: &Source) -> Result<source::Source, String> {
    let manifest = read_to_string(&source_config.manifest)
        .map_err(|e| format!("Could not read manifest for {}: {}", name, e))?;
    let manifest: Manifest = toml::from_str(&manifest)
        .map_err(|e| format!("Could not parse manifest for {}: {}", name, e))?;

    Ok(source::Source::from_manifest(manifest))
}
