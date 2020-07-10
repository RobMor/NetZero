use std::collections::HashMap;
use std::fs::{create_dir_all, read_to_string, write};
use std::path::{Path, PathBuf};

extern crate dirs;
use serde::{Deserialize, Serialize};

use crate::source;

#[derive(Serialize, Deserialize, Debug)]
pub struct Config {
    general: General,

    #[serde(flatten, with = "sources")]
    sources: Vec<Source>,
}

#[derive(Serialize, Deserialize, Debug)]
struct General {
    database: PathBuf,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Source {
    #[serde(skip)]
    pub name: String,
    pub description: String,

    pub short: String,
    pub long: String,

    pub command: String,
    pub args: Vec<String>,

    /// Everything else in the source data
    #[serde(flatten)]
    pub rest: HashMap<String, String>,
}

impl Default for Config {
    fn default() -> Config {
        return Config {
            general: General {
                // TODO unwrapping here also shouldn't cause any problems but replace it anyways...
                database: dirs::data_dir()
                    .unwrap()
                    .join(PathBuf::from("netzero/data.sqlite3")),
            },
            sources: Vec::new(),
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

        Ok(config)
    }

    pub fn raw_sources(&self) -> &Vec<Source> {
        &self.sources
    }

    pub fn all_sources(&self) -> Vec<source::Source> {
        self.sources
            .iter()
            .map(source::Source::from_config)
            .collect()
    }

    pub fn get_source(&self, name: &str) -> Result<source::Source, String> {
        for source in &self.sources {
            if name == source.short || name == source.long {
                return Ok(source::Source::from_config(source));
            }
        }

        Err(format!("Source {} not found", name))
    }
}

// Config source ser/de to store the "name" in the actual source struct.
mod sources {
    use std::fmt;

    use serde::de::{Deserializer, MapAccess, Visitor};
    use serde::ser::{SerializeMap, Serializer};

    use crate::config::Source;

    pub fn serialize<S>(sources: &Vec<Source>, ser: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = ser.serialize_map(Some(sources.len()))?;

        for source in sources {
            map.serialize_entry(&source.name, &source)?;
        }

        map.end()
    }

    struct SourceVisitor;

    impl<'de> Visitor<'de> for SourceVisitor {
        type Value = Vec<Source>;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("a map from strings to sources")
        }

        fn visit_map<M>(self, mut access: M) -> Result<Self::Value, M::Error>
        where
            M: MapAccess<'de>,
        {
            let mut sources: Vec<Source> = Vec::new();

            while let Some((key, mut value)) = access.next_entry::<String, Source>()? {
                value.name = key;
                sources.push(value);
            }

            Ok(sources)
        }
    }

    pub fn deserialize<'de, D>(de: D) -> Result<Vec<Source>, D::Error>
    where
        D: Deserializer<'de>,
    {
        de.deserialize_map(SourceVisitor)
    }
}
