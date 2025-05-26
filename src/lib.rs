use libc::{madvise, MADV_SEQUENTIAL};
use log::{error,warn};
use memmap2::Mmap;
use hashbrown::HashMap;
use lazy_static::lazy_static;
use strum::IntoEnumIterator; // Import the trait for iterating over enums
use strum_macros::{EnumIter, EnumString, IntoStaticStr}; // Derive macro for generating iterator
use std::collections::{BTreeMap,HashSet};
use std::error::Error;
use std::fs::File;
use std::io;
use serde::{Serialize, Deserialize, Serializer, Deserializer};
use serde::ser::SerializeMap;
use serde::de::{self, MapAccess, Visitor};
use std::fmt;
use rmp_serde::{encode::write_named, decode::from_read};
use serde_json::to_writer;

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
}

impl Config {
    // Constructor to create a new Config instance
    pub fn new(file_path: &str, format: SerializationFormat, output_file_path: &str,  summary_only: bool) -> Self {
        Self {
            file_path: file_path.to_string(),
            format,
            output_file_path: output_file_path.to_string(),
            summary_only,
        }
    }
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

    fn modify_order(&mut self, order_id: u64, new_order: Order) {
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
}

#[derive(Serialize, Deserialize)]
struct MarketState {
    order_map: OrderMap,
    #[serde(serialize_with = "serialize_btreemap", deserialize_with = "deserialize_btreemap")]
    instrument_markets: BTreeMap<[u8; 6], InstrumentMarket>, // mapping from symbol id to the bids and offers
    unique_symbols: HashSet<[u8; 6]>,
}

impl MarketState {
    fn new() -> Self {
        MarketState {
            order_map: OrderMap::new(),
            instrument_markets: BTreeMap::new(),
            unique_symbols: HashSet::new(),
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

    fn load_from_file(file_path: &str, format: SerializationFormat) -> Result<Self, Box<dyn Error>> {
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

fn handle_add_order_long(market_state: &mut MarketState, message_payload: &[u8], config: &Config) {
    let _time_offset = u32::from_le_bytes(message_payload[0..4].try_into().unwrap());
    let order_id = u64::from_le_bytes(message_payload[4..12].try_into().unwrap());
    let side = message_payload[12] as char;
    let quantity = u32::from_le_bytes(message_payload[13..17].try_into().unwrap());
    let symbol: [u8; 6] = message_payload[17..23].try_into().unwrap();
    let price = u64::from_le_bytes(message_payload[23..32].try_into().unwrap());

    market_state.unique_symbols.insert(symbol);

    if !config.summary_only {
        let order = Order::new(symbol, side, quantity, price);
        market_state.order_map.add_order(order_id, order);
    }
}

fn handle_add_order_short(market_state: &mut MarketState, message_payload: &[u8], config: &Config) {
    let _time_offset = u32::from_le_bytes(message_payload[0..4].try_into().unwrap());
    let order_id = u64::from_le_bytes(message_payload[4..12].try_into().unwrap());
    let side = message_payload[12] as char;
    let quantity: u32 = u16::from_le_bytes(message_payload[13..15].try_into().unwrap()) as u32;
    let symbol: [u8; 6] = message_payload[17..23].try_into().unwrap();
    let price: u64 = u16::from_le_bytes(message_payload[21..23].try_into().unwrap()) as u64;

    market_state.unique_symbols.insert(symbol);

    if !config.summary_only {
        let order = Order::new(symbol, side, quantity, price);
        market_state.order_map.add_order(order_id, order);
        match side {
            'B' => market_state.instrument_markets.entry(symbol).or_insert_with(InstrumentMarket::new).add_bid(price, quantity),
            'S' => market_state.instrument_markets.entry(symbol).or_insert_with(InstrumentMarket::new).add_offer(price, quantity),
            _ => warn!("Unknown order side: {}", side),
        }
    }
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
                instrument_market.remove_quantity(side, price, quantity);
            }
        }
    }
}

fn handle_symbol_mapping(market_state: &mut MarketState, message_payload: &[u8], config: &Config) {
    let feed_symbol: [u8; 6] = message_payload[0..6].try_into().unwrap();
    let osi_symbol: [u8; 21] = message_payload[6..27].try_into().unwrap();
    let underlying: [u8; 8] = message_payload[27..35].try_into().unwrap();

    if !config.summary_only {
        eprintln!("feed_symbol: {}, osi_symbol: {}, underlying: {}", String::from_utf8_lossy(&feed_symbol), String::from_utf8_lossy(&osi_symbol), String::from_utf8_lossy(&underlying));
    }
}

fn dispatch_message(market_state: &mut MarketState, message_type: &u8, message_payload: &[u8], config: &Config) {
    match message_type {
        0x21 => handle_add_order_long(market_state, &message_payload, config),
        0x22 => handle_add_order_short(market_state, &message_payload, config),
        0x29 => handle_delete_order(market_state, &message_payload, config),
        0x2e => handle_symbol_mapping(market_state, &message_payload, config),
        _ => warn!("Unknown or unsupported message type: 0x{:02X}", message_type),
    }
}

fn parse(data: &[u8], config: &Config) -> io::Result<()> {
    let mut offset = 0;
    let mut market_state = MarketState::new();
    let mut message_stats: HashMap<u8, MessageStats> = HashMap::new();

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

    if config.summary_only {
        let mut sorted_stats: Vec<_> = message_stats.iter().collect();
        sorted_stats.sort_by_key(|&(message_type, _)| *message_type);

        for (message_type, stats) in sorted_stats {
            eprintln!(
                "Message type: 0x{:02X} ({:?}), count: {}, first offset: {}, total length: {}",
                message_type,
                MessageType::from_byte(*message_type).name(),
                stats.message_count,
                stats.first_offset,
                stats.total_length
            );
        }
        // Sort and print unique symbols
        let mut sorted_symbols: Vec<_> = market_state.unique_symbols.iter().collect();
        sorted_symbols.sort();

        eprintln!("Unique symbols encountered:");
        for symbol in sorted_symbols {
            eprintln!("{}", String::from_utf8_lossy(symbol));
        }
    }

    if let Err(e) = market_state.save_to_file(&config.output_file_path, config.format.clone()) {
        eprintln!("Error saving market state: {}", e);
    }
    Ok(())
}

pub fn run(config: &Config) -> Result<(), Box<dyn Error>> {
    // Open the file
    let file = File::open(config.file_path.to_string()).map_err(|err| {
        eprintln!("Error opening file: {}", err);
        err
    })?;

    // Memory-map the file
    let mmap = unsafe { Mmap::map(&file).map_err(|err| {
        eprintln!("Error memory mapping file: {}", err);
        err
    })? };

    // Advise the OS about sequential access
    unsafe {
        let result = madvise(mmap.as_ptr() as *mut _, mmap.len(), MADV_SEQUENTIAL);
        if result != 0 {
            eprintln!("madvise failed with error code: {}", result);
        }
    }

    if let Err(e) = parse(&mmap, &config) {
        eprintln!("Error parsing data: {}", e);
        return Err(Box::new(e));
    }

    Ok(())
}