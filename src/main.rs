use clap::Parser;
use env_logger;
use log::{error,info};
use std::process;
use cboe_market_state::{run, Config, OptionType, SerializationFormat};
use chrono::Utc;
use std::str::FromStr;
use std::vec::Vec;

/// A simple program to process files with options
#[derive(Parser, Debug)]
#[command(name = "cboe-market-state", version = "1.0", author = "Lyndon Drake <lyndon@arotau.com>", about = "Processes CBOE PITCH message files and keyframes market state")]
struct Args {
    /// Enables verbose mode
    #[arg(short = 'v', long = "verbose")]
    verbose: bool,

    /// Enables output of top-of-book for each symbol
    #[arg(short = 'b', long = "top-of-book-file", value_name = "TOB_FILE")]
    top_of_book_file: Option<String>,

    /// Set a dump file for serialising symbols and statistics at the end of processing
    #[arg(short = 'd', long = "dump-file", value_name = "DUMP_FILE")]
    dump_file: Option<String>,

    /// Sets an output file for serialising market state
    #[arg(short = 'o', long = "output-file", value_name = "OUTPUT_FILE")]
    output_file: Option<String>,

    /// Sets an input file for de-serialising market state
    #[arg(short = 'i', long = "input-file", value_name = "INPUT_FILE")]
    input_file: Option<String>,

    /// Serialization format (json or msgpack)
    #[arg(short = 'f', long = "format", value_name = "FORMAT", default_value = "json")]
    format: String,

    /// Limit processing to first N add messages (accounting for filtered symbols)
    #[arg(short = 'l', long = "limit", value_name = "LIMIT", default_value = "0")]
    limit: usize,

    /// Skip building market state and only collect statistics
    #[arg(short = 'q', long = "quick-summary-only")]
    quick_summary_only: bool,
    
    /// Path to instruments CSV file
    #[arg(short = 'm', long = "symbol-map-file", value_name = "SYMBOL_MAP_FILE")]
    symbol_map_file: Option<String>,

    /// Name of underlying
    #[arg(short = 'r', long = "osi-root", value_name = "OSI_ROOT")]
    osi_root: Option<String>,

    /// Type of option
    #[arg(short = 't', long = "option-type", value_name = "OPTION_TYPE" )]
    option_type: Option<String>,

    /// Strike price of option
    #[arg(short = 's', long = "strike-price", value_name = "OPTION_STRIKE_PRICE")]
    option_strike_price: Option<String>,

    /// Expiry date of option (YYYY-MM-DD)
    #[arg(short = 'e', long = "expiry-date", value_name = "OPTION_EXPIRY_DATE")]
    option_expiry_date: Option<String>,

    /// Input file
    file: String,
}
fn main() {
    env_logger::init();

    let startup_time = Utc::now();
    info!("Startup time: {}", startup_time.to_rfc3339());

    let args = Args::parse();

    // Check for underlying without instruments_file
    if args.osi_root.is_some() && args.symbol_map_file.is_none() {
        error!("Error: --osi-root (-r) requires --symbol-map-file (-m) to also be specified.");
        process::exit(1);
    }

    let format = match args.format.as_str() {
        "msgpack" => SerializationFormat::MsgPack,
        _ => SerializationFormat::Json,
    };

    let output = args.output_file.as_deref().unwrap_or("");
    let option_type_str = args.option_type.as_deref().unwrap_or("both").to_lowercase();
    let option_type = OptionType::from_str(&option_type_str).unwrap_or(OptionType::Both);
    let strike_prices: Vec<f64> = args
    .option_strike_price
    .as_deref()
    .unwrap_or("")
    .split(',')
    .filter_map(|s| s.trim().parse::<f64>().ok())
    .collect();

    let config = Config::new(
        &args.file,
        format,
        output,
        args.quick_summary_only,
        args.symbol_map_file.is_some(),
        args.symbol_map_file.as_deref().unwrap_or("").to_string(),
        args.osi_root.is_some(),
        args.osi_root.as_deref().unwrap_or("").to_string(),
        args.dump_file.is_some(),
        args.dump_file.as_deref().unwrap_or("").to_string(),
        args.top_of_book_file.is_some(),
        args.top_of_book_file.as_deref().unwrap_or("").to_string(),
        args.input_file.is_some(),
        args.input_file.as_deref().unwrap_or("").to_string(),
        args.verbose,
        option_type,
        strike_prices,
        args.option_expiry_date.as_deref().unwrap_or("").to_string(),
        args.limit,
    );
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
