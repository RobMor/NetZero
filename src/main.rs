use std::process::exit;
use std::sync::{Arc, Mutex};
use std::collections::BTreeMap;

use clap::{crate_authors, crate_version, App, AppSettings, Arg, ArgMatches, SubCommand};
use crossbeam::{thread, channel};
use time::Date;
use console::Term;

mod config;
mod server;
mod progress;
mod source;

use crate::config::Config;
use crate::server::Server;
use crate::source::{Source, IncomingMessage};
use crate::progress::{Progress, TerminalProgress};

fn main() {
    let config = Config::setup().unwrap_or_else(|e| {
        eprintln!("{}", e);
        exit(1);
    });

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
                )
                .arg(
                    Arg::with_name("sources")
                        .long("sources")
                        .required(false)
                        .takes_value(true)
                        .value_name("SOURCES"),
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

    match matches.subcommand() {
        ("", None) => launch_server(config, &matches),
        ("collect", Some(sub_matches)) => collect(config, sub_matches),
        ("list", Some(sub_matches)) => list(config, sub_matches),
        _ => todo!(),
    }
}

fn launch_server(config: Config, matches: &ArgMatches) {
    Server::new(config).start();
}

fn collect(config: Config, matches: &ArgMatches) {
    // The dates were validated during command parsing so we can unwrap here.
    let start_date = matches
        .value_of("start_date")
        .map(|d| Date::parse(d, "%Y-%m-%d").unwrap());
    let end_date = matches
        .value_of("end_date")
        .map(|d| Date::parse(d, "%Y-%m-%d").unwrap());

    let mut sources: Vec<Source> = matches
        .value_of("sources")
        .map_or_else(
            || config.all_sources(),
            |s| s.split(",").map(|s| config.get_source(s)).collect(),
        )
        .unwrap_or_else(|e| {
            eprintln!("Failed to load sources: {}", e);
            exit(1);
        });

    let term = Term::buffered_stdout();
    let (send, receive) = channel::unbounded();
    // Use a BTreeMap so we can iterate through them in a guaranteed consistent order
    let mut bars = BTreeMap::new();

    thread::scope(|s| {

        for source in &mut sources {
            bars.insert(source.get_name(), TerminalProgress::new(source.get_name()));

            source.use_channel(send.clone());

            s.spawn(move |_| {
                // TODO
                source.collect(start_date, end_date).unwrap();
            });
        }

        for msg in receive {
            if term.is_term() {
                let name = msg.name;
                let mut bar = bars.get_mut(&name).unwrap();

                match msg.message {
                    IncomingMessage::Starting => {
                        bar.set_status("Starting".to_string());
                    }
                    IncomingMessage::SetMax { value, status } => {
                        bar.set_max(value);
                        if let Some(s) = status { bar.set_status(s) };
                    },
                    IncomingMessage::SetProgress { progress, status } => {
                        bar.set_progress(progress);
                        if let Some(s) = status { bar.set_status(s) };
                    },
                    IncomingMessage::SetStatus { status } => {
                        bar.set_status(status);
                    },
                    IncomingMessage::Reset => {
                        bar.reset();
                    },
                    IncomingMessage::Done => {
                        bar.set_status("Done".to_string());
                    },
                }

                let (height, width) = term.size();

                term.move_cursor_up(bars.len());

                for (_, bar) in &bars {
                    term.clear_line().unwrap();
                    term.write_line(&format!("{:width$}", bar, width = width as usize)).unwrap();
                }

                term.flush().unwrap();
            }
        }
        
        if term.is_term() {
            term.write_line("Done");
        }
    })
    .unwrap();
}

fn list(config: Config, _matches: &ArgMatches) {
    for source in config.all_sources().unwrap() {
        println!("{}", source.get_name());
    }
}
