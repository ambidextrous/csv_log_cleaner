use clap::Parser;
use csv_log_cleaner::{get_schema_from_json_str, process_rows, JsonSchema};
use std::error::Error;
use std::fs;
use std::io;
use std::process;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    // Set up CLI
    #[clap(short = 'j', long, value_parser)]
    schema: String,
    #[clap(short = 'l', long, value_parser)]
    log: String,
    #[clap(short = 's', long, value_parser, default_value_t = ',')]
    sep: char,
    #[clap(short = 'b', long, value_parser, default_value_t = 1000)]
    buffer_size: usize,
}

fn run() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();
    let log_path = args.log;
    let byte_sep = args.sep as u8;
    let schema_string = fs::read_to_string(args.schema)?;
    let schema_map = get_schema_from_json_str(&schema_string)?;
    let mut rdr = csv::ReaderBuilder::new()
        .delimiter(byte_sep)
        .from_reader(io::stdin());
    let wtr = csv::WriterBuilder::new()
        .delimiter(byte_sep)
        .from_writer(io::stdout());
    let log_result = process_rows(&mut rdr, wtr, schema_map, args.buffer_size)?;
    fs::write(log_path, log_result.json())?;

    Ok(())
}

fn main() {
    if let Err(err) = run() {
        eprintln!("Fatal {}, exiting processes with code 1", err);
        process::exit(1);
    }
}
