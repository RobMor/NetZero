use std::fs::{create_dir_all, read_to_string, write, OpenOptions};
use std::io::{ErrorKind, Read};
use std::path::{Path, PathBuf};
use std::process::exit;

use clap::{crate_authors, crate_version, App, AppSettings, Arg, ArgGroup, ArgMatches, SubCommand};
use time::Date;

mod config;
mod collect;
mod web;

use crate::web::Session;
use crate::collect::CollectionTask;
use crate::config::Config;

fn main() {
    // TODO discover sources

    let matches = App::new("NetZero")
        .version(crate_version!())
        .author(crate_authors!())
        .about("Collects and analyzes energy data")
        .setting(AppSettings::DeriveDisplayOrder)
        .setting(AppSettings::VersionlessSubcommands)
        .subcommand(
            SubCommand::with_name("collect")
                .about("Collect data")
                .arg(
                    Arg::with_name("start_date")
                        .short("s")
                        .long("start")
                        .required(false)
                        .takes_value(true)
                        .value_name("YYYY-MM-DD")
                        .validator(|d| match Date::parse(d, "%Y-%m-%d") {
                            Ok(_) => Ok(()),
                            Err(e) => Err(format!("{}", e)),
                        }),
                )
                .arg(
                    Arg::with_name("end_date")
                        .short("e")
                        .long("end")
                        .required(false)
                        .takes_value(true)
                        .value_name("YYYY-MM-DD")
                        .validator(|d| match Date::parse(d, "%Y-%m-%d") {
                            Ok(_) => Ok(()),
                            Err(e) => Err(format!("{}", e)),
                        }),
                ),
        )
        .subcommand(
            SubCommand::with_name("export").about("Export data").arg(
                Arg::with_name("output_file")
                    .short("o")
                    .long("output")
                    .help("The file to store data in")
                    .required(true)
                    .takes_value(true)
                    .value_name("FILE"),
            ),
        )
        .subcommand(SubCommand::with_name("list").about("List available data sources"))
        .subcommand(SubCommand::with_name("install").about("Install data source plugins"))
        .subcommand(SubCommand::with_name("uninstall").about("Remove data source plugins"))
        .get_matches();

    let config_dir = dirs::config_dir()
        .unwrap() // TODO unwrapping here shouldn't cause issues but replace it anyways...
        .join(Path::new("netzero"));

    if !config_dir.exists() {
        create_dir_all(&config_dir).unwrap_or_else(|e| match e.kind() {
            _ => {
                println!("Could not create config directory: {}", e);
                exit(1);
            }
        });
    }

    let config_path = config_dir.join(Path::new("config.toml"));

    let config = if !config_path.exists() {
        let config = Config::default();
        write(config_path, toml::to_string(&config).unwrap()).unwrap_or_else(|e| {
            println!("Could not write default config: {}", e);
            exit(1);
        });
        config
    } else {
        let contents = read_to_string(config_path).unwrap_or_else(|e| {
            println!("Could not read configuration file: {}", e);
            exit(1);
        });
        toml::from_str(&contents).unwrap_or_else(|e| {
            println!("Could not parse configuration file: {}", e);
            exit(1);
        })
    };

    println!("{:?}", config);

    match matches.subcommand() {
        ("", None) => todo!(),
        ("collect", Some(sub_matches)) => collect(config, sub_matches),
        ("export", Some(sub_matches)) => todo!(),
        ("list", Some(sub_matches)) => todo!(),
        _ => todo!(),
    }
}

fn launch(config: Config) {
    Session::new(config).start();
}

fn collect(config: Config, matches: &ArgMatches) {
    // The dates were validated during command parsing so we can unwrap here.
    let start_date = matches
        .value_of("start_date")
        .map(|d| Date::parse(d, "%Y-%m-%d").unwrap());
    let end_date = matches
        .value_of("end_date")
        .map(|d| Date::parse(d, "%Y-%m-%d").unwrap());

    CollectionTask::new(start_date, end_date, config, mode::Mode::Terminal).start();
}
