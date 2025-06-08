use libc::{madvise, MADV_SEQUENTIAL};
use log::{error,warn,info,debug,trace};
use memmap2::Mmap;
use hashbrown::HashMap;
use lazy_static::lazy_static;
use strum::IntoEnumIterator; // Import the trait for iterating over enums
use strum_macros::{EnumIter, EnumString, IntoStaticStr}; // Derive macro for generating iterator
use std::collections::{BTreeMap,HashSet};
use std::error::Error;
use std::fs::File;
use std::{io};
use std::io::Write;
use csv::Writer;
use serde::{Serialize, Deserialize, Serializer, Deserializer};
use serde::ser::SerializeMap;
use serde::de::{self, MapAccess, Visitor};
use std::fmt;
use rmp_serde::{encode::write_named, decode::from_read};
use serde_json::{to_writer, json, Map, Value};

#[derive(Clone)]
pub enum SerializationFormat {
    Json,
    MsgPack,
}

pub struct Config {
    pub file_path: String,
    pub format: SerializationFormat,
    pub output_file_path: String,
    pub summary_only: bool,
    pub instruments_file_provided: bool,
    pub instruments_file_path: String,
    pub osi_root_provided: bool,
    pub osi_root: String,
    pub dump_file_provided: bool,
    pub dump_file: String,
    pub top_of_book_provided: bool,
    pub top_of_book_file: String,
    pub input_file_provided: bool,
    pub input_file_path: String,
    pub verbose: bool,
}

impl Config {
    // Constructor to create a new Config instance
    pub fn new(file_path: &str, format: SerializationFormat, output_file_path: &str,  summary_only: bool, instruments_file_provided: bool, instruments_file_path: String, osi_root_provided: bool, osi_root: String, dump_file_provided: bool, dump_file: String, top_of_book_provided: bool, top_of_book_file: String, input_file_provided: bool, input_file_path: String, verbose: bool) -> Self { 
        Self {
            file_path: file_path.to_string(),
            format,
            output_file_path: output_file_path.to_string(),
            summary_only,
            instruments_file_provided,
            instruments_file_path,
            osi_root_provided,
            osi_root,
            dump_file_provided,
            dump_file,
            top_of_book_provided,
            top_of_book_file,
            input_file_provided,
            input_file_path,
            verbose,
        }
    }
}

fn str_to_u8_6<'de, D>(deserializer: D) -> Result<[u8; 6], D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s: &str = serde::Deserialize::deserialize(deserializer)?;
    let bytes = s.as_bytes();
    if bytes.len() != 6 {
        return Err(serde::de::Error::custom("Expected 6 bytes for BATS Symbol"));
    }
    let mut arr = [0u8; 6];
    arr.copy_from_slice(bytes);
    Ok(arr)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SymbolMapping {
    #[serde(rename = "BATS Symbol", deserialize_with = "str_to_u8_6")]
    bats_symbol: [u8; 6],
    #[serde(rename = "OSI Root")]
    osi_root: String,
    #[serde(rename = "Expire Date")]
    expiry: String,
    #[serde(rename = "Call/Put Flag", default)]
    call_put: Option<char>,
    #[serde(rename = "Strike Price")]
    strike: f64,
    #[serde(rename = "Underlying")]
    _underlying: String,
    #[serde(rename = "OSI Symbol")]
    _osi_symbol: String,
}

fn read_symbol_mapping_from_csv(path: &str) -> Result<HashMap<[u8; 6], SymbolMapping>, Box<dyn std::error::Error>> {
    let mut rdr = csv::Reader::from_path(path)?;
    let mut map = HashMap::new();
    for result in rdr.deserialize() {
        let row: SymbolMapping = result?;
        map.insert(row.bats_symbol.clone(), row);
    }
    Ok(map)
}

#[derive(Clone, Copy, EnumIter, EnumString, IntoStaticStr)] // Add EnumIter derive macro
enum MessageType {
    Login = 0x01,
    LoginResponse = 0x02,
    Time = 0x20,
    AddOrderLong = 0x21,
    AddOrderShort = 0x22,
    OrderExecuted = 0x23,
    OrderExecutedAtPriceSize = 0x24,
    ReduceSizeLong = 0x25,
    ReduceSizeShort = 0x26,
    ModifyOrderLong = 0x27,
    ModifyOrderShort = 0x28,
    DeleteOrder = 0x29,
    TradeLong = 0x2A,
    TradeShort = 0x2B,
    EndOfSession = 0x2D,
    SymbolMapping = 0x2E,
    AddOrderExpanded = 0x2F,
    TradingStatus = 0x31,
    AuctionSummary = 0x96,
    AuctionNotification = 0xAD,
    AuctionCancel = 0xAE,
    AuctionTrade = 0xAF,
    TimeReference = 0xB1,
    TransactionBegin = 0xBC,
    TransactionEnd = 0xBD,
    OptionsAuctionUpdate = 0xD1,
    Unknown,
}


impl MessageType {
    fn from_byte(byte: u8) -> Self {
        MESSAGE_TYPE_MAP.get(&byte).cloned().unwrap_or(MessageType::Unknown)
    }

    fn name(&self) -> &'static str {
        self.into()
    }
}

lazy_static! {
    static ref MESSAGE_TYPE_MAP: HashMap<u8, MessageType> = {
        let mut m = HashMap::new();
        for message_type in MessageType::iter() {
            m.insert(message_type as u8, message_type);
        }
        m
    };
}




pub struct MessageStats {
    pub message_count: usize,
    pub first_offset: usize,
    pub total_length: usize,
}

#[derive(Serialize, Deserialize)]
struct Order {
    symbol: [u8; 6],
    side: char,
    quantity: u32,
    price: u64,
    // TODO: add timestamp for last modified
}

impl Order {
    fn new(symbol: [u8; 6], side: char, quantity: u32, price: u64) -> Self {
        Order {
            symbol,
            side,
            quantity,
            price,
        }
    }
}

#[derive(Serialize, Deserialize)]
struct OrderMap {
    orders: HashMap<u64, Order>,
}

impl OrderMap {
    fn new() -> Self {
        OrderMap {
            orders: HashMap::new(),
        }
    }

    fn add_order(&mut self, order_id: u64, order: Order) {
        self.orders.insert(order_id, order);
    }

    fn _modify_order(&mut self, order_id: u64, new_order: Order) {
        if let Some(order) = self.orders.get_mut(&order_id) {
            *order = new_order;
        }
    }

    fn remove_order(&mut self, order_id: u64) -> Option<Order> {
        self.orders.remove(&order_id)
    }
}

#[derive(Serialize, Deserialize)]
struct InstrumentMarket {
    // for a given instrument, the bids are a mapping from price u64 to a list of quantities at that price in the order of insertion
    // TODO: are there other aspects of the order book that must be preserved: (a) how is top-of-book determined e.g. does modifying an order push that order to the bottom of the priority queue? (b) can priority be modified in other ways which means that this should store Order objects rather than just quantities which would use more space but be more flexible?
    bids: BTreeMap<u64, Vec<u32>>,
    offers: BTreeMap<u64, Vec<u32>>,
}

impl InstrumentMarket {
    fn new() -> Self {
        InstrumentMarket {
            bids: BTreeMap::new(),
            offers: BTreeMap::new(),
        }
    }

    fn add_bid(&mut self, price: u64, quantity: u32) {
        self.bids.entry(price).or_insert_with(Vec::new).push(quantity);
    }

    fn add_offer(&mut self, price: u64, quantity: u32) {
        self.offers.entry(price).or_insert_with(Vec::new).push(quantity);
    }

    fn remove_quantity(&mut self, side: char, price: u64, quantity: u32) {
        let price_map = match side {
            'B' => &mut self.bids,
            'S' => &mut self.offers,
            _ => {
                warn!("Unknown order side: {}", side);
                return;
            }
        };

        if let Some(quantities) = price_map.get_mut(&price) {
            if let Some(pos) = quantities.iter().position(|&q| q == quantity) {
                quantities.remove(pos);
            }
            if quantities.is_empty() {
                price_map.remove(&price);
            }
        }
    }

    /// Returns the highest bid as (price, total_quantity), or None if no bids.
    fn highest_bid(&self) -> Option<(u64, u32)> {
        self.bids.iter().rev().next().map(|(&price, quantities)| {
            (price, quantities.iter().sum())
        })
    }

    /// Returns the lowest offer as (price, total_quantity), or None if no offers.
    fn lowest_offer(&self) -> Option<(u64, u32)> {
        self.offers.iter().next().map(|(&price, quantities)| {
            (price, quantities.iter().sum())
        })
    }    
}

#[derive(Serialize, Deserialize)]
struct MarketState {
    order_map: OrderMap,
    #[serde(serialize_with = "serialize_btreemap", deserialize_with = "deserialize_btreemap")]
    instrument_markets: BTreeMap<[u8; 6], InstrumentMarket>, // mapping from symbol id to the bids and offers
    unique_symbols: HashSet<[u8; 6]>,
    symbol_map: HashMap<[u8; 6], SymbolMapping>, // mapping from symbol id to SymbolMapping
    filtering: bool, // whether to filter symbols based on the OSI root symbol
    filtered_symbols: HashSet<[u8; 6]>, // symbols that match the OSI root symbol, if provided
    unsupported_message_types: HashSet<u8>, // unsupported message types encountered
    #[serde(skip)]
    pub tob_writer: Option<Writer<std::fs::File>>,
}

impl MarketState {
    fn new() -> Self {
        MarketState {
            order_map: OrderMap::new(),
            instrument_markets: BTreeMap::new(),
            unique_symbols: HashSet::new(),
            symbol_map: HashMap::new(),
            filtering: false,
            filtered_symbols: HashSet::new(),
            unsupported_message_types: HashSet::new(),
            tob_writer: None,
        }
    }

    fn save_to_file(&self, file_path: &str, format: SerializationFormat) -> Result<(), Box<dyn Error>> {
        let file = File::create(file_path)?;
        match format {
            SerializationFormat::Json => to_writer(file, &self)?,
            SerializationFormat::MsgPack => write_named(&mut &file, &self)?,
        }
        Ok(())
    }

    fn _load_from_file(file_path: &str, format: SerializationFormat) -> Result<Self, Box<dyn Error>> {
        let file = File::open(file_path)?;
        let market_state = match format {
            SerializationFormat::Json => serde_json::from_reader(file)?,
            SerializationFormat::MsgPack => from_read(file)?,
        };
        Ok(market_state)
    }
}


fn serialize_btreemap<S>(map: &BTreeMap<[u8; 6], InstrumentMarket>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let mut map_serializer = serializer.serialize_map(Some(map.len()))?;
    for (k, v) in map {
        let key = hex::encode(k);
        map_serializer.serialize_entry(&key, v)?;
    }
    map_serializer.end()
}

fn deserialize_btreemap<'de, D>(deserializer: D) -> Result<BTreeMap<[u8; 6], InstrumentMarket>, D::Error>
where
    D: Deserializer<'de>,
{
    struct BTreeMapVisitor;

    impl<'de> Visitor<'de> for BTreeMapVisitor {
        type Value = BTreeMap<[u8; 6], InstrumentMarket>;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("a map with string keys and InstrumentMarket values")
        }

        fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
        where
            A: MapAccess<'de>,
        {
            let mut btreemap = BTreeMap::new();
            while let Some((key, value)) = map.next_entry::<String, InstrumentMarket>()? {
                let key_bytes = hex::decode(&key).map_err(de::Error::custom)?;
                if key_bytes.len() != 6 {
                    return Err(de::Error::custom("key length must be 6 bytes"));
                }
                let mut key_array = [0u8; 6];
                key_array.copy_from_slice(&key_bytes);
                btreemap.insert(key_array, value);
            }
            Ok(btreemap)
        }
    }

    deserializer.deserialize_map(BTreeMapVisitor)
}

fn process_add_order(
    market_state: &mut MarketState,
    order_id: u64,
    side: char,
    quantity: u32,
    symbol: [u8; 6],
    price: u64,
    config: &Config,
) {
    market_state.unique_symbols.insert(symbol);

    let order = Order::new(symbol, side, quantity, price);
    market_state.order_map.add_order(order_id, order);

    if let Some(symbol_mapping) = market_state.symbol_map.get(&symbol) {
        trace!(
            "Adding order: id = {}, symbol = {}, osi_root = {}, side = {}, quantity = {}, price = {}",
            order_id,
            String::from_utf8_lossy(&symbol),
            symbol_mapping.osi_root,
            side,
            quantity,
            price
        );
    } else {
        trace!(
            "Adding order: id = {}, symbol = {}, osi_root = <unknown>, side = {}, quantity = {}, price = {}",
            order_id,
            String::from_utf8_lossy(&symbol),
            side,
            quantity,
            price
        );
    }

    if !config.summary_only {
        if !market_state.filtering || market_state.filtered_symbols.contains(&symbol) {
            let instrument_market = market_state
                .instrument_markets
                .entry(symbol)
                .or_insert_with(InstrumentMarket::new);

            let old_best_bid = instrument_market.highest_bid();
            let old_best_offer = instrument_market.lowest_offer();

            match side {
                'B' => instrument_market.add_bid(price, quantity),
                'S' => instrument_market.add_offer(price, quantity),
                _ => warn!("Unknown order side: {}", side),
            }

            let new_best_bid = instrument_market.highest_bid();
            let new_best_offer = instrument_market.lowest_offer();

            if new_best_bid != old_best_bid || new_best_offer != old_best_offer {
                let instrument = market_state.symbol_map.get(&symbol);
                if instrument.is_none() {
                    trace!(
                        "No symbol mapping found for symbol: {}",
                        String::from_utf8_lossy(&symbol)
                    );
                } else {
                    let instrument = instrument.unwrap();
                    let osi_root = instrument.osi_root.clone();
                    let expiry = instrument.expiry.clone();
                    let call_put = instrument.call_put.unwrap_or(' ');
                    let strike = instrument.strike;

                    // Log the market state update
                    if let Some(tob_writer) = &mut market_state.tob_writer {
                        let bid_qty = new_best_bid.map_or(0, |(_, qty)| qty);
                        let bid_price = new_best_bid.map_or(0, |(price, _)| price);
                        let offer_qty = new_best_offer.map_or(0, |(_, qty)| qty);
                        let offer_price = new_best_offer.map_or(0, |(price, _)| price);

                        tob_writer.write_record(&[
                            String::from_utf8_lossy(&symbol).to_string(),
                            "AddOrder*".to_string(),
                            bid_qty.to_string(),
                            bid_price.to_string(),
                            offer_qty.to_string(),
                            offer_price.to_string(),
                        ]).unwrap();
                    }

                    trace!(
                        "Market state updated for OSI root: {}, expiry: {}, p/c: {}, strike: {}, best bid: {}, best offer: {}",
                        osi_root,
                        expiry,
                        call_put,
                        strike,
                        new_best_bid.map_or("None".to_string(), |(price, qty)| format!("{}@{}", qty, price)),
                        new_best_offer.map_or("None".to_string(), |(price, qty)| format!("{}@{}", qty, price))
                    );
                }
            }
        }
    }
}

fn handle_add_order_long(market_state: &mut MarketState, message_payload: &[u8], config: &Config) {
    let _time_offset = u32::from_le_bytes(message_payload[0..4].try_into().unwrap());
    let order_id = u64::from_le_bytes(message_payload[4..12].try_into().unwrap());
    let side = message_payload[12] as char;
    let quantity = u32::from_le_bytes(message_payload[13..17].try_into().unwrap());
    let symbol: [u8; 6] = message_payload[17..23].try_into().unwrap();
    let price = u64::from_le_bytes(message_payload[23..32].try_into().unwrap());

    process_add_order(market_state, order_id, side, quantity, symbol, price, config);
}

fn handle_add_order_short(market_state: &mut MarketState, message_payload: &[u8], config: &Config) {
    let _time_offset = u32::from_le_bytes(message_payload[0..4].try_into().unwrap());
    let order_id = u64::from_le_bytes(message_payload[4..12].try_into().unwrap());
    let side = message_payload[12] as char;
    let quantity: u32 = u16::from_le_bytes(message_payload[13..15].try_into().unwrap()) as u32;
    let symbol: [u8; 6] = message_payload[17..23].try_into().unwrap();
    let price: u64 = u16::from_le_bytes(message_payload[21..23].try_into().unwrap()) as u64;

    process_add_order(market_state, order_id, side, quantity, symbol, price, config);
}
fn handle_delete_order(market_state: &mut MarketState, message_payload: &[u8], config: &Config) {
    let _time_offset = u32::from_le_bytes(message_payload[0..4].try_into().unwrap());
    let order_id = u64::from_le_bytes(message_payload[4..12].try_into().unwrap());

    if !config.summary_only {
        if let Some(order) = market_state.order_map.remove_order(order_id) {
            let symbol = order.symbol;
            let price = order.price;
            let quantity = order.quantity;
            let side = order.side;

            if let Some(instrument_market) = market_state.instrument_markets.get_mut(&symbol) {
                let old_best_bid = instrument_market.highest_bid();
                let old_best_offer = instrument_market.lowest_offer();

                instrument_market.remove_quantity(side, price, quantity);

                let new_best_bid = instrument_market.highest_bid();
                let new_best_offer = instrument_market.lowest_offer();

                if new_best_bid != old_best_bid || new_best_offer != old_best_offer  {
                    let instrument = market_state.symbol_map.get(&symbol);
                    if Some(instrument).is_none() {
                        trace!("No symbol mapping found for symbol: {}", String::from_utf8_lossy(&symbol));
                    }
                    else {
                        // let osi_root = instrument.unwrap().osi_root.clone();
                        // let expiry = instrument.unwrap().expiry.clone();
                        // let call_put = instrument.unwrap().call_put.unwrap_or(' ');
                        // let strike = instrument.unwrap().strike;                    

                        // // Log the market state update
                        // trace!("Market state updated for OSI root: {}, expiry: {}, p/c: {}, strike: {}, best bid: {}, best offer: {}",
                        //     osi_root, expiry, call_put, strike, new_best_bid.map_or("None".to_string(), |(price, qty)| format!("{}@{}", qty, price)),
                        //     new_best_offer.map_or("None".to_string(), |(price, qty)| format!("{}@{}", qty, price)));
                    }
                }
            }
        }
    }
}

fn handle_symbol_mapping(_market_state: &mut MarketState, message_payload: &[u8], config: &Config) {
    let feed_symbol: [u8; 6] = message_payload[0..6].try_into().unwrap();
    let osi_symbol: [u8; 21] = message_payload[6..27].try_into().unwrap();
    let underlying: [u8; 8] = message_payload[27..35].try_into().unwrap();

    if !config.summary_only {
        debug!("feed_symbol: {}, osi_symbol: {}, underlying: {}", String::from_utf8_lossy(&feed_symbol), String::from_utf8_lossy(&osi_symbol), String::from_utf8_lossy(&underlying));
    }
}

fn dispatch_message(market_state: &mut MarketState, message_type: &u8, message_payload: &[u8], config: &Config) {
    match message_type {
        0x21 => handle_add_order_long(market_state, &message_payload, config),
        0x22 => handle_add_order_short(market_state, &message_payload, config),
        0x29 => handle_delete_order(market_state, &message_payload, config),
        0x2e => handle_symbol_mapping(market_state, &message_payload, config),
        _ => { market_state.unsupported_message_types.insert(*message_type); },
    }
}

fn parse(data: &[u8], symbol_mapping: &HashMap<[u8; 6], SymbolMapping>, filtered_symbols: &HashSet<[u8; 6]>, config: &Config, dump_json: &mut Map<String, Value>) -> io::Result<()> {
    let mut offset = 0;
    let mut market_state = MarketState::new();
    let mut message_stats: HashMap<u8, MessageStats> = HashMap::new();

    market_state.filtered_symbols = filtered_symbols.clone();
    market_state.symbol_map = symbol_mapping.clone();

    if config.top_of_book_provided {
        let mut wtr = csv::Writer::from_path(&config.top_of_book_file)?;
        wtr.write_record(&["symbol", "event", "bid_qty", "bid_price", "offer_qty", "offer_price"])?;
        market_state.tob_writer = Some(wtr);
    }

    debug!("Filtered symbols count: {}", market_state.filtered_symbols.len());
    debug!("Symbol mapping count: {}", market_state.symbol_map.len());
    if market_state.filtered_symbols.len() > 0 {
        market_state.filtering = true;
        info!("Filtering enabled for symbols matching the OSI root: {}", config.osi_root);
    } else {
        info!("No symbols match the provided OSI root, filtering is disabled.");
    }

    'outer: while offset < data.len() {
        // Parse Sequenced Unit Header
        if offset + 8 > data.len() {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "Incomplete header"));
        }

        let message_count = data[offset + 2];

        offset += 8;

        for _ in 0..message_count {
            if offset >= data.len() {
                break;
            }

            let message_length = data[offset] as usize;
            if message_length == 0 || offset + message_length > data.len() {
                if offset + 2 > data.len() {
                    error!("Invalid message length: {} at offset {} and data.len() = {}", message_length, offset, data.len());
                }
                else {
                    error!("Invalid message length: {} with message_type = 0x{:02X} at offset {} and data.len() = {}", message_length, &data[offset+1], offset, data.len());
                }
                break 'outer;
            }

            if message_length > 1 {
                let message_type = &data[offset+1];
                if message_length > 2 {
                    let stats = message_stats.entry(*message_type).or_insert(MessageStats {
                        message_count: 0,
                        first_offset: offset,
                        total_length: 0,
                    });
                    stats.message_count += 1;
                    stats.total_length += message_length;
                    // if !config.summary_only {
                        // eprintln!("Processing message type: 0x{:02X} with length: {}", message_type, message_length);
                        dispatch_message(&mut market_state, &message_type, &data[offset+2..offset+message_length], config);
                    // }
                }
                else {
                    warn!("Short message: message_length = {} and message_type = 0x{:02X}", message_length, message_type);
                }
            }
            else {
                warn!("Short message: message_length = {}", message_length);
            }
            offset += message_length;
        }
    }

    if let Some(mut writer) = market_state.tob_writer.take() {
        writer.flush()?;
    }

    let mut sorted_stats: Vec<_> = message_stats.iter().collect();
    sorted_stats.sort_by_key(|&(message_type, _)| *message_type);

    for (message_type, stats) in &sorted_stats {
        info!(
            "Message type: 0x{:02X} ({:?}), count: {}, first offset: {}, total length: {}",
            message_type,
            MessageType::from_byte(**message_type).name(),
            stats.message_count,
            stats.first_offset,
            stats.total_length
        );
    }

    // Insert into dump_json if dumping is enabled
    if config.dump_file_provided {
        let mut stats_json = Vec::new();
        for (message_type, stats) in &sorted_stats {
            stats_json.push(serde_json::json!({
                "message_type": format!("0x{:02X}", message_type),
                "name": MessageType::from_byte(**message_type).name(),
                "count": stats.message_count,
                "first_offset": stats.first_offset,
                "total_length": stats.total_length
            }));
        }
        dump_json.insert("message_stats".to_string(), serde_json::Value::Array(stats_json));
    }


    if config.dump_file_provided {
        debug!("Inserting unique symbols to JSON dump");
        // As hex strings for [u8; 6] representation, sorted alphabetically
        let mut unique_symbols_hex: Vec<String> = market_state
            .unique_symbols
            .iter()
            .map(|s| hex::encode(s))
            .collect();
        unique_symbols_hex.sort();

        // As trimmed ASCII strings, sorted alphabetically
        let mut unique_symbols_ascii: Vec<String> = market_state
            .unique_symbols
            .iter()
            .map(|s| String::from_utf8_lossy(s).trim_end().to_string())
            .collect();
        unique_symbols_ascii.sort();

        dump_json.insert("unique_symbols_hex".to_string(), json!(unique_symbols_hex));
        dump_json.insert("unique_symbols_ascii".to_string(), json!(unique_symbols_ascii));
    }

    if !market_state.unsupported_message_types.is_empty() {
        warn!(
            "Encountered unsupported message types: {}",
            market_state.unsupported_message_types
                .iter()
                .map(|t| format!("0x{:02X}", t))
                .collect::<Vec<_>>()
                .join(", ")
        );
    }

    if !config.output_file_path.is_empty() {
        if let Err(e) = market_state.save_to_file(&config.output_file_path, config.format.clone()) {
            error!("Error saving market state: {}", e);
        }
    }
    Ok(())
}

pub fn run(config: &Config) -> Result<(), Box<dyn Error>> {
    // Prepare a JSON map to collect variables for dumping
    let mut dump_json = Map::new();

    // load up the instruments mapping table if provided
    let symbol_mapping = if config.instruments_file_provided {
        match read_symbol_mapping_from_csv(&config.instruments_file_path) {
            Ok(map) => map,
            Err(e) => {
                error!("Error reading instruments file: {}", e);
                std::process::exit(1);
            }
        }
    } else {
        HashMap::new()
    };

    // Insert symbol_mapping into the JSON map
    if config.dump_file_provided {
        debug!("Inserting symbol mapping to JSON dump");
        let symbol_mapping_json: serde_json::Map<String, serde_json::Value> = symbol_mapping
            .iter()
            .map(|(k, v)| (hex::encode(k), serde_json::to_value(v).unwrap()))
            .collect();
        dump_json.insert(
            "symbol_mapping".to_string(),
            serde_json::Value::Object(symbol_mapping_json),
        );
    }

    info!("Loaded {} instruments from file: {}", symbol_mapping.len(), config.instruments_file_path);

    // build a set of symbols that match the underlying, if provided
    let filtered_symbols: HashSet<[u8; 6]> = if config.osi_root_provided {
        let underlying = &config.osi_root;
        let set: HashSet<[u8; 6]> = symbol_mapping
            .values()
            .filter(|row| row.osi_root == *underlying)
            .map(|row| row.bats_symbol)
            .collect();

        if set.is_empty() {
            error!("No symbols found for the provided underlying: {}", config.osi_root);
            return Err(Box::new(io::Error::new(io::ErrorKind::NotFound, "No matching symbols found")));
        }
        set
    } else {
        HashSet::new()
    };

    // Insert filtered_symbols into the JSON map (as Vec<String> for readability)
    if config.dump_file_provided {
        debug!("Inserting filtered symbols to JSON dump");
        let filtered_vec: Vec<String> = filtered_symbols
            .iter()
            .map(|s| String::from_utf8_lossy(s).trim_end().to_string())
            .collect();
        dump_json.insert("filtered_symbols".to_string(), json!(filtered_vec));
    }
    
    info!("Filtered symbols count: {}", filtered_symbols.len());

    // Open the data file
    let file = File::open(config.file_path.to_string()).map_err(|err| {
        error!("Error opening file: {}", err);
        err
    })?;

    // Memory-map the data file
    let mmap = unsafe { Mmap::map(&file).map_err(|err| {
        error!("Error memory mapping file: {}", err);
        err
    })? };

    // Advise the OS about sequential access
    unsafe {
        let result = madvise(mmap.as_ptr() as *mut _, mmap.len(), MADV_SEQUENTIAL);
        if result != 0 {
            error!("madvise failed with error code: {}", result);
        }
    }
    
    // Parse and collect stats
    let parse_result = parse(&mmap, &symbol_mapping, &filtered_symbols, &config, &mut dump_json);

    // Dump the parse status
    if config.dump_file_provided {
        debug!("Inserting parse status to JSON dump");
        dump_json.insert(
            "parse_status".to_string(),
            json!(parse_result.is_ok()),
        );
    }

    // Write the JSON dump at the end
    if config.dump_file_provided {
        debug!("Writing JSON to dump file");
        if let Err(e) = serde_json::to_writer_pretty(
            File::create(&config.dump_file)?,
            &dump_json,
        ) {
            error!("Error writing dump file: {}", e);
            return Err(Box::new(e));
        }
        info!("Dumped run variables to file: {}", config.dump_file);
    }

    if let Err(e) = parse_result {
        error!("Error parsing data: {}", e);
        return Err(Box::new(e));
    }

    Ok(())
}