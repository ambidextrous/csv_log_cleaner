use clap::Parser;
use csv_cleaner::generate_validated_schema;
use csv_cleaner::process_rows;
use csv_cleaner::JsonSchema;
use serde_json;
use std::error::Error;
use std::fs;
use std::process;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    // Set-up CLI
    #[clap(short = 'i', long, value_parser)]
    input: String,
    #[clap(short = 'o', long, value_parser)]
    output: String,
    #[clap(short = 'j', long, value_parser)]
    schema: String,
    #[clap(short = 'l', long, value_parser)]
    log: String,
    #[clap(short = 's', long, value_parser, default_value_t = ',')]
    sep: char,
}

fn run() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();
    // Parse JSON schema to Schema to
    let schema_string = fs::read_to_string(args.schema)?;
    let json_schema: JsonSchema = serde_json::from_str(&schema_string)?;
    let input_csv_path = args.input;
    let output_csv_path = args.output;
    let log_path = args.log;
    let schema_map = generate_validated_schema(json_schema)?;
    let byte_sep = args.sep as u8;
    let result = process_rows(
        &input_csv_path,
        &output_csv_path,
        &log_path,
        schema_map,
        byte_sep,
    );

    result
}

fn main() {
    if let Err(err) = run() {
        println!("{}", err);
        process::exit(1);
    }
}
