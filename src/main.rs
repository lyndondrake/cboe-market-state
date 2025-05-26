use clap::Parser;
use env_logger;
use std::process;
use cboe_market_state::{Config, run, SerializationFormat};
use chrono::Utc;

/// A simple program to process files with options
#[derive(Parser, Debug)]
#[command(name = "cboe-market-state", version = "1.0", author = "Lyndon Drake <lyndon@arotau.com>", about = "Processes CBOE message files and keyframes market state")]
struct Args {
    /// Enables verbose mode
    #[arg(short, long)]
    verbose: bool,

    /// Sets an output file
    #[arg(short, long, value_name = "FILE")]
    output: Option<String>,

    /// Serialization format (json or msgpack)
    #[arg(short, long, value_name = "FORMAT", default_value = "json")]
    format: String,

    /// Skip building market state and only collect statistics
    #[arg(short = 's', long = "summary-only")]
    summary_only: bool,
    
    /// Input file
    file: String,
}
fn main() {
    env_logger::init();

    let startup_time = Utc::now();
    eprintln!("Startup time: {}", startup_time.to_rfc3339());

    let args = Args::parse();

    let format = match args.format.as_str() {
        "msgpack" => SerializationFormat::MsgPack,
        _ => SerializationFormat::Json,
    };

    let output = args.output.as_deref().unwrap_or("");
    let config = Config::new(&args.file, format, output, args.summary_only);
    let result = run(&config);

    let shutdown_time = Utc::now();
    eprintln!("Shutdown time: {}", shutdown_time.to_rfc3339());
    let elapsed = shutdown_time.signed_duration_since(startup_time);
    eprintln!("Elapsed time: {} seconds", elapsed.num_seconds());

    if let Err(e) = result {
        eprintln!("Application error: {}", e);
        process::exit(1);
    }
}
