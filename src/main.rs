use clap::Parser;
use csv_cleaner::process_rows;
use std::error::Error;
use std::process;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    // Set up CLI
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
    let input_csv_path = args.input;
    let output_csv_path = args.output;
    let log_path = args.log;
    let schema_path = args.schema;
    let byte_sep = args.sep as u8;
    let result = process_rows(
        &input_csv_path,
        &output_csv_path,
        &log_path,
        &schema_path,
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
