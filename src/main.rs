use clap::Parser;
use env_logger;
use log::{error,info};
use std::process;
use cboe_market_state::{Config, run, SerializationFormat};
use chrono::Utc;

/// A simple program to process files with options
#[derive(Parser, Debug)]
#[command(name = "cboe-market-state", version = "1.0", author = "Lyndon Drake <lyndon@arotau.com>", about = "Processes CBOE message files and keyframes market state")]
struct Args {
    /// Enables verbose mode
    #[arg(short = 'v', long = "verbose")]
    verbose: bool,

    /// Enables output of top-of-book for each symbol
    #[arg(short = 'b', long = "top-of-book", value_name = "TOB_FILE")]
    top_of_book: Option<String>,

    /// Set a dump file for serialising symbols and statistics at the end of processing
    #[arg(short = 'd', long = "dump-file", value_name = "DUMP_FILE")]
    dump_file: Option<String>,

    /// Sets an output file for serialising market state
    #[arg(short = 'o', long = "output-file", value_name = "OUTPUT_FILE")]
    output: Option<String>,

    /// Sets an input file for deserialising market state
    #[arg(short = 'i', long = "input-file", value_name = "INPUT_FILE")]
    input: Option<String>,

    /// Serialization format (json or msgpack)
    #[arg(short = 't', long = "format", value_name = "FORMAT", default_value = "json")]
    format: String,

    /// Skip building market state and only collect statistics
    #[arg(short = 'q', long = "quick-summary-only")]
    quick_summary_only: bool,
    
    /// Path to instruments CSV file
    #[arg(short = 's', long = "symbols", value_name = "SYMBOLS_FILE")]
    symbols_file: Option<String>,

    /// Name of underlying
    #[arg(short = 'r', long = "osi-root", value_name = "OSI_ROOT")]
    osi_root: Option<String>,

    /// Input file
    file: String,
}
fn main() {
    env_logger::init();

    let startup_time = Utc::now();
    info!("Startup time: {}", startup_time.to_rfc3339());

    let args = Args::parse();

    // Check for underlying without instruments_file
    if args.osi_root.is_some() && args.symbols_file.is_none() {
        error!("Error: --osi-root (-r) requires --symbols (-s) to also be specified.");
        process::exit(1);
    }

    let format = match args.format.as_str() {
        "msgpack" => SerializationFormat::MsgPack,
        _ => SerializationFormat::Json,
    };

    let output = args.output.as_deref().unwrap_or("");
    let config = Config::new(&args.file, format, output, args.quick_summary_only, args.symbols_file.is_some(), args.symbols_file.as_deref().unwrap_or("").to_string(), args.osi_root.is_some(), args.osi_root.as_deref().unwrap_or("").to_string(), args.dump_file.is_some(), args.dump_file.as_deref().unwrap_or("").to_string(), args.top_of_book.is_some(), args.top_of_book.as_deref().unwrap_or("").to_string(), args.input.is_some(), args.input.as_deref().unwrap_or("").to_string(), args.verbose);
    let result = run(&config);

    let shutdown_time = Utc::now();
    info!("Shutdown time: {}", shutdown_time.to_rfc3339());
    let elapsed = shutdown_time.signed_duration_since(startup_time);
    info!("Elapsed time: {} seconds", elapsed.num_seconds());

    if let Err(e) = result {
        error!("Application error: {}", e);
        process::exit(1);
    }
}
