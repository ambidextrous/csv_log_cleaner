use clap::Parser;
use csv::{Reader, Writer};
use csv_log_cleaner::{clean_csv, get_schema_from_json_str, Column};
use fs::File;
use std::collections::HashMap;
use std::error::Error;
use std::fs;
use std::io;
use std::io::Read;
use std::io::Write;
use std::process;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    // Set up CLI
    #[clap(short = 'i', long, value_parser)]
    input: Option<String>,
    #[clap(short = 'o', long, value_parser)]
    output: Option<String>,
    #[clap(short = 'j', long, value_parser)]
    schema: String,
    #[clap(short = 'l', long, value_parser)]
    log: String,
    #[clap(short = 's', long, value_parser, default_value_t = ',')]
    sep: char,
    #[clap(short = 'b', long, value_parser, default_value_t = 1_000)]
    buffer_size: usize,
}

fn run() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();
    let log_path = args.log;
    let byte_sep = args.sep as u8;
    let schema_string = args.schema.as_str();
    let schema_map: HashMap<String, Column> = if std::path::Path::new(schema_string)
        .extension()
        .map_or(false, |ext| ext.eq_ignore_ascii_case("json"))
    {
        let schema_file_contents = fs::read_to_string(schema_string)?;
        get_schema_from_json_str(&schema_file_contents)?
    } else {
        get_schema_from_json_str(schema_string)?
    };
    let rdr: Box<dyn Read> = match args.input {
        Some(ref path) => {
            let file: File = File::open(path)?;
            Box::new(file)
        }
        None => Box::new(io::stdin()),
    };
    let mut rdr: Reader<Box<dyn Read>> = csv::ReaderBuilder::new()
        .delimiter(byte_sep)
        .from_reader(rdr);
    let wtr: Box<dyn Write + Send + Sync + 'static> = match args.output {
        Some(ref path) => {
            let file: File = fs::File::create(path)?;
            Box::new(file)
        }
        None => Box::new(io::stdout()),
    };
    let wtr: Writer<Box<dyn Write + Send + Sync>> = csv::WriterBuilder::new()
        .delimiter(byte_sep)
        .from_writer(wtr);
    let log_result = clean_csv(&mut rdr, wtr, schema_map, args.buffer_size)?;
    fs::write(log_path, log_result.json())?;

    Ok(())
}

fn main() {
    if let Err(err) = run() {
        eprintln!("Fatal {}, exiting processes with code 1", err);
        process::exit(1);
    }
}
