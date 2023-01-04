use clap::Parser;
use csv_cleaner::process_rows;
use std::error::Error;
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
    let schema_path = args.schema;
    let byte_sep = args.sep as u8;
    let mut rdr = csv::Reader::from_reader(io::stdin());
    let wtr = csv::WriterBuilder::new()
        .delimiter(byte_sep)
        .from_writer(io::stdout());
    process_rows(&mut rdr, wtr, &log_path, &schema_path, args.buffer_size)
}

fn main() {
    if let Err(err) = run() {
        println!("{}", err);
        process::exit(1);
    }
}
