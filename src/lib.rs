use libc::{madvise, MADV_SEQUENTIAL};
use log::{error,warn,info,debug,trace};
use memmap2::Mmap;
use hashbrown::HashMap;
use lazy_static::lazy_static;
use num_format::{Locale, ToFormattedString};
use strum::IntoEnumIterator; // Import the trait for iterating over enums
use strum_macros::{EnumIter, EnumString, IntoStaticStr}; use core::time;
// Derive macro for generating iterator
use std::collections::{BTreeMap,HashSet};
use std::collections::btree_map::Entry;
use std::error::Error;
use std::fs::File;
use std::{io};
use csv::Writer;
use serde::{Serialize, Deserialize, Serializer, Deserializer};
use serde::ser::SerializeMap;
use serde::de::{self, MapAccess, Visitor};
use std::fmt;
use rmp_serde::{encode::write_named, decode::from_read};
use serde_json::{to_writer, json, Map, Value};
use std::rc::Rc;
use std::cell::RefCell;

// TODO: paramaterised levels of top-of-book depth
// TODO: include the relevant details of the message that triggers the row being output
// DONE: differentiate the messages that trigger the TOB change
// TODO: paramaterise the top-of-book trigger to be the levels of depth displayed
// TODO: still a few unimplemented message types


#[derive(Clone)]
pub enum SerializationFormat {
    Json,
    MsgPack,
}

#[derive(Debug)]
pub enum OptionType {
    None,
    Call,
    Put,
    Both,
}

impl std::str::FromStr for OptionType {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "c" | "call" => Ok(OptionType::Call),
            "p" | "put" => Ok(OptionType::Put),
            "both" => Ok(OptionType::Both),
            _ => Err(()),
        }
    }
}

pub struct Config {
    pub pitch_data_file_path: String,
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
    pub option_type: OptionType,
    pub option_strike_prices: Vec<f64>,
    pub option_expiry_date: String,
    pub limit: usize, // 0 means no limit
}

impl Config {
    // Constructor to create a new Config instance
    pub fn new(
        pitch_data_file_path: &str,
        format: SerializationFormat,
        output_file_path: &str,
        summary_only: bool,
        instruments_file_provided: bool,
        instruments_file_path: String,
        osi_root_provided: bool,
        osi_root: String,
        dump_file_provided: bool,
        dump_file: String,
        top_of_book_provided: bool,
        top_of_book_file: String,
        input_file_provided: bool,
        input_file_path: String,
        verbose: bool,
        option_type: OptionType,
        option_strike_prices: Vec<f64>,
        option_expiry_date: String,
        limit: usize,
    ) -> Self {
        Self {
            pitch_data_file_path: pitch_data_file_path.to_string(),
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
            input_file_path: input_file_path.to_string(),
            verbose,
            option_type,
            option_strike_prices,
            option_expiry_date,
            limit,
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
    bids: BTreeMap<u64, Vec<u32>>,
    offers: BTreeMap<u64, Vec<u32>>,
    #[serde(skip)]
    tob_writer: Option<Rc<RefCell<Writer<std::fs::File>>>>,
    #[serde(skip)]
    best_bid: Option<(u64, u32)>,
    #[serde(skip)]
    best_offer: Option<(u64, u32)>,
}

impl InstrumentMarket {
    // fn new() -> Self {
    //     InstrumentMarket {
    //         bids: BTreeMap::new(),
    //         offers: BTreeMap::new(),
    //         tob_writer: None,
    //     }
    // }

    fn new_with_writer(tob_writer: Option<Rc<RefCell<Writer<std::fs::File>>>>) -> Self {
        InstrumentMarket {
            bids: BTreeMap::new(),
            offers: BTreeMap::new(),
            tob_writer,
            best_bid: None,
            best_offer: None,
        }
    }

    // Return the highest bid as (price, total_quantity) or None if no bids
    fn highest_bid(&self) -> Option<(u64, u32)> {
        self.bids
            .iter()
            .next_back()
            .map(|(price, qtys)| (*price, qtys.iter().copied().sum()))
    }

    // Return the lowest offer as (price, total_quantity) or None if no offers
    fn lowest_offer(&self) -> Option<(u64, u32)> {
        self.offers
            .iter()
            .next()
            .map(|(price, qtys)| (*price, qtys.iter().copied().sum()))
    }

    // Recompute bests from the books
    fn recompute_best(&self) -> (Option<(u64, u32)>, Option<(u64, u32)>) {
        (self.highest_bid(), self.lowest_offer())
    }

    // Centralized TOB emission helper. Also updates stored bests.
    // TODO: split into separate emit and update functions?
    // TODO: output time in CSV as full date time + fractional seconds
    fn maybe_emit_tob_change(
        &mut self,
        time_reference: u32,
        time_offset: u32,
        old_best_bid: Option<(u64, u32)>,
        old_best_offer: Option<(u64, u32)>,
        new_best_bid: Option<(u64, u32)>,
        new_best_offer: Option<(u64, u32)>,
        symbol: &[u8; 6],
        instrument: Option<&SymbolMapping>,
        event: &str,
        reason: &str,
    ) {
        if new_best_bid != old_best_bid || new_best_offer != old_best_offer {
            if let Some(instr) = instrument {
                let (bid_price_str, bid_qty_str) = if let Some((price, qty)) = new_best_bid {
                    (price.to_string(), qty.to_string())
                } else {
                    (String::new(), String::new())
                };
                let (offer_price_str, offer_qty_str) = if let Some((price, qty)) = new_best_offer {
                    (price.to_string(), qty.to_string())
                } else {
                    (String::new(), String::new())
                };

                if let Some(rc) = &self.tob_writer {
                    let mut wtr = rc.borrow_mut();
                    let osi_root = instr.osi_root.clone();
                    let expiry = instr.expiry.clone();
                    let call_put = instr.call_put.unwrap_or(' ');
                    let strike = instr.strike;

                    wtr.write_record(&[
                        time_reference.to_string(),
                        time_offset.to_string(),
                        String::from_utf8_lossy(symbol).to_string(),
                        event.to_string(),
                        osi_root.to_string(),
                        expiry.to_string(),
                        call_put.to_string(),
                        strike.to_string(),
                        bid_qty_str,
                        bid_price_str,
                        offer_price_str,
                        offer_qty_str,
                        reason.to_string(),
                    ]).unwrap();
                }

                trace!(
                    "Market state updated for OSI root: {}, expiry: {}, p/c: {}, strike: {}, best bid: {}, best offer: {}",
                    instr.osi_root,
                    instr.expiry,
                    instr.call_put.unwrap_or(' '),
                    instr.strike,
                    new_best_bid.map_or("None".to_string(), |(p, q)| format!("{}@{}", q, p)),
                    new_best_offer.map_or("None".to_string(), |(p, q)| format!("{}@{}", q, p))
                );
            } else {
                trace!("No symbol mapping found for symbol: {}", String::from_utf8_lossy(symbol));
            }
        }

        // Persist the new bests
        self.best_bid = new_best_bid;
        self.best_offer = new_best_offer;
    }

    fn add_bid(
        &mut self,
        time_reference: u32,
        time_offset: u32,
        price: u64,
        quantity: u32,
        symbol: &[u8; 6],
        instrument: Option<&SymbolMapping>,
        event: &str,
    ) {
        let old_best_bid = self.best_bid;
        let old_best_offer = self.best_offer;

        self.bids.entry(price).or_insert_with(Vec::new).push(quantity);

        let (new_best_bid, new_best_offer) = self.recompute_best();
        let reason = format!("bid {}@{}", quantity, price);
        self.maybe_emit_tob_change(
            time_reference,
            time_offset,
            old_best_bid,
            old_best_offer,
            new_best_bid,
            new_best_offer,
            symbol,
            instrument,
            event,
            &reason,
        );
    }

    fn add_offer(
        &mut self,
        time_reference: u32,
        time_offset: u32,
        price: u64,
        quantity: u32,
        symbol: &[u8; 6],
        instrument: Option<&SymbolMapping>,
        event: &str,
    ) {
        let old_best_bid = self.best_bid;
        let old_best_offer = self.best_offer;

        self.offers.entry(price).or_insert_with(Vec::new).push(quantity);

        let (new_best_bid, new_best_offer) = self.recompute_best();
        let reason = format!("bid {}@{}", quantity, price);
        self.maybe_emit_tob_change(
            time_reference,
            time_offset,
            old_best_bid,
            old_best_offer,
            new_best_bid,
            new_best_offer,
            symbol,
            instrument,
            event,
            &reason,
        );
    }

    // Renamed from `modify_quantity` and extended to support price changes via `prev_price`
    fn modify_order(
        &mut self,
        time_reference: u32,
        time_offset: u32,
        side: char,
        prev_price: u64,
        new_price: u64,
        prev_quantity: u32,
        new_quantity: u32,
        symbol: &[u8; 6],
        instrument: Option<&SymbolMapping>,
        event: &str,
    ) {
        let old_best_bid = self.best_bid;
        let old_best_offer = self.best_offer;

        let price_map = match side {
            'B' => &mut self.bids,
            'S' => &mut self.offers,
            _ => {
                warn!("Unknown order side in modify_order: {}", side);
                return;
            }
        };

        if prev_price == new_price {
            // Quantity-only change at the same price level
            if let Some(level) = price_map.get_mut(&new_price) {
                if let Some(pos) = level.iter().position(|&q| q == prev_quantity) {
                    if new_quantity > 0 {
                        level[pos] = new_quantity;
                    } else {
                        level.remove(pos);
                        if level.is_empty() {
                            price_map.remove(&new_price);
                        }
                    }
                } else {
                    trace!(
                        "modify_order(same price): no matching prev_quantity {} at price {} for side {}",
                        prev_quantity, new_price, side
                    );
                }
            } else {
                trace!(
                    "modify_order(same price): no price level {} found for side {}",
                    new_price, side
                );
            }
        } else {
            // Price change: remove from prev_price level, then (optionally) insert at new price
            let mut removed = false;
            if let Some(level) = price_map.get_mut(&prev_price) {
                if let Some(pos) = level.iter().position(|&q| q == prev_quantity) {
                    level.remove(pos);
                    removed = true;
                    if level.is_empty() {
                        price_map.remove(&prev_price);
                    }
                } else {
                    trace!(
                        "modify_order(move): no matching prev_quantity {} at prev_price {} for side {}",
                        prev_quantity, prev_price, side
                    );
                }
            } else {
                trace!(
                    "modify_order(move): no prev_price level {} found for side {}",
                    prev_price, side
                );
            }

            if removed && new_quantity > 0 {
                price_map.entry(new_price).or_insert_with(Vec::new).push(new_quantity);
            }
        }

        let (new_best_bid, new_best_offer) = self.recompute_best();
        let reason = format!("modify {}@{} to {}@{}", prev_quantity, prev_price, new_quantity, new_price);
        self.maybe_emit_tob_change(
            time_reference,
            time_offset,
            old_best_bid,
            old_best_offer,
            new_best_bid,
            new_best_offer,
            symbol,
            instrument,
            event,
            &reason,
        );
    }

    fn remove_quantity(
        &mut self,
        time_reference: u32,
        time_offset: u32,
        side: char,
        price: u64,
        quantity: u32,
        symbol: &[u8; 6],
        instrument: Option<&SymbolMapping>,
        event: &str,
    ) {
        let old_best_bid = self.best_bid;
        let old_best_offer = self.best_offer;

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

        let (new_best_bid, new_best_offer) = self.recompute_best();
        let reason = format!("remove {}@{}", quantity, price);
        self.maybe_emit_tob_change(
            time_reference,
            time_offset,
            old_best_bid,
            old_best_offer,
            new_best_bid,
            new_best_offer,
            symbol,
            instrument,
            event,
            &reason,
        );
    }
}
#[derive(Serialize, Deserialize)]
struct MarketState {
    order_map: OrderMap,
    #[serde(serialize_with = "serialize_btreemap", deserialize_with = "deserialize_btreemap")]
    instrument_markets: BTreeMap<[u8; 6], InstrumentMarket>,
    unique_symbols: HashSet<[u8; 6]>,
    symbol_map: HashMap<[u8; 6], SymbolMapping>,
    filtering: bool,
    filtered_symbols: HashSet<[u8; 6]>,
    unsupported_message_types: HashSet<u8>,
    #[serde(skip)]
    pub tob_writer: Option<Rc<RefCell<Writer<std::fs::File>>>>, // changed to shared handle
    add_messages_count: usize,
    time_reference: u32,
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
            add_messages_count: 0,
            time_reference: 0,
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
    time_offset: u32,
    order_id: u64,
    side: char,
    quantity: u32,
    symbol: [u8; 6],
    price: u64,
    config: &Config,
    event: &str,
) {
    market_state.unique_symbols.insert(symbol);

    if !config.summary_only && (!market_state.filtering || market_state.filtered_symbols.contains(&symbol)) {
        if market_state.add_messages_count > config.limit && config.limit > 0 {
            return;
        }
        market_state.add_messages_count += 1;

        let order = Order::new(symbol, side, quantity, price);
        market_state.order_map.add_order(order_id, order);
        
        // Look up instrument mapping before mutable borrow of instrument_markets
        let instrument_info = market_state.symbol_map.get(&symbol);

        let instrument_market = match market_state.instrument_markets.entry(symbol) {
            Entry::Occupied(e) => {
                e.into_mut()
            }
            Entry::Vacant(v) => {
                trace!("Inserting new InstrumentMarket for symbol hex={}", hex::encode(symbol));
                v.insert(InstrumentMarket::new_with_writer(market_state.tob_writer.clone()))
            }
        };

        match side {
            'B' => instrument_market.add_bid(market_state.time_reference, time_offset, price, quantity, &symbol, instrument_info, event, ),
            'S' => instrument_market.add_offer(market_state.time_reference, time_offset, price, quantity, &symbol, instrument_info, event, ),
            _ => warn!("Unknown order side: {}", side),
        }
    }
}

// TODO: handle time offset
fn handle_time(market_state: &mut MarketState, message_payload: &[u8], _config: &Config) {
    // Time message handling can be implemented here if needed
    let time = u32::from_le_bytes(message_payload[0..4].try_into().unwrap());

    market_state.time_reference = time;
}

fn handle_add_order_long(market_state: &mut MarketState, message_payload: &[u8], config: &Config) {
    let time_offset = u32::from_le_bytes(message_payload[0..4].try_into().unwrap());
    let order_id = u64::from_le_bytes(message_payload[4..12].try_into().unwrap());
    let side = message_payload[12] as char;
    let quantity = u32::from_le_bytes(message_payload[13..17].try_into().unwrap());
    let symbol: [u8; 6] = message_payload[17..23].try_into().unwrap();
    let price = u64::from_le_bytes(message_payload[23..31].try_into().unwrap());

    process_add_order(market_state, time_offset, order_id, side, quantity, symbol, price, config, "AddOrderLong");
}

fn handle_add_order_short(market_state: &mut MarketState, message_payload: &[u8], config: &Config) {
    let time_offset = u32::from_le_bytes(message_payload[0..4].try_into().unwrap());
    let order_id = u64::from_le_bytes(message_payload[4..12].try_into().unwrap());
    let side = message_payload[12] as char;
    let quantity: u32 = u16::from_le_bytes(message_payload[13..15].try_into().unwrap()) as u32;
    let symbol: [u8; 6] = message_payload[15..21].try_into().unwrap();
    let price: u64 = u16::from_le_bytes(message_payload[21..23].try_into().unwrap()) as u64;

    process_add_order(market_state, time_offset, order_id, side, quantity, symbol, price, config, "AddOrderShort");
}

// TODO: with all of these, if the order is reduced to 0 quantity, it needs to be removed from the order map too.
fn handle_order_executed(market_state: &mut MarketState, message_payload: &[u8], config: &Config) {
    // Offsets per your current stub
    let time_offset = u32::from_le_bytes(message_payload[0..4].try_into().unwrap());
    let order_id = u64::from_le_bytes(message_payload[4..12].try_into().unwrap());
    let executed_quantity = u32::from_le_bytes(message_payload[12..16].try_into().unwrap());
    let _execution_id = u64::from_le_bytes(message_payload[16..24].try_into().unwrap());
    let _trade_condition = message_payload[24];

    if config.summary_only {
        return;
    }

    if let Some(order) = market_state.order_map.orders.get_mut(&order_id) {
        let symbol = order.symbol;
        let side = order.side;
        let price = order.price;
        let prev_qty = order.quantity;
        let exec = executed_quantity.min(prev_qty);
        let new_qty = prev_qty.saturating_sub(exec);

        if !market_state.filtering || market_state.filtered_symbols.contains(&symbol) {
            if let Some(instrument_market) = market_state.instrument_markets.get_mut(&symbol) {
                // Ensure writer is wired through
                if instrument_market.tob_writer.is_none() {
                    instrument_market.tob_writer = market_state.tob_writer.clone();
                }
                let instrument_info = market_state.symbol_map.get(&symbol);

                // Execution reduces quantity at the same resting price
                instrument_market.modify_order(
                    market_state.time_reference,
                    time_offset,
                    side,
                    price,       // prev_price
                    price,       // new_price (unchanged)
                    prev_qty,
                    new_qty,
                    &symbol,
                    instrument_info,
                    "OrderExecuted",
                );
            } else {
                trace!("OrderExecuted: no InstrumentMarket for {}", String::from_utf8_lossy(&symbol));
            }
        }

        if new_qty == 0 {
            std::mem::drop(order); // release mutable borrow before removing from the map
            market_state.order_map.remove_order(order_id);
        } else {
            order.quantity = new_qty;
        }
    } else {
        trace!("OrderExecuted for unknown order_id: {}", order_id);
    }
}
// ...existing code...

fn handle_order_executed_at_price_size(market_state: &mut MarketState, message_payload: &[u8], config: &Config) {
    // Offsets per your current stub
    let time_offset = u32::from_le_bytes(message_payload[0..4].try_into().unwrap());
    let order_id = u64::from_le_bytes(message_payload[4..12].try_into().unwrap());
    let executed_quantity = u32::from_le_bytes(message_payload[12..16].try_into().unwrap());
    let remaining_quantity = u32::from_le_bytes(message_payload[16..20].try_into().unwrap());
    let _execution_id = u64::from_le_bytes(message_payload[20..28].try_into().unwrap());
    let _exec_price = u64::from_le_bytes(message_payload[28..36].try_into().unwrap());
    let _trade_condition = message_payload[36];

    if config.summary_only {
        return;
    }

    if let Some(order) = market_state.order_map.orders.get_mut(&order_id) {
        let symbol = order.symbol;
        let side = order.side;
        let price = order.price;
        let prev_qty = order.quantity;

        // Prefer remaining_quantity if sane; fall back to computed
        let computed_new = prev_qty.saturating_sub(executed_quantity);
        let new_qty = if remaining_quantity <= prev_qty {
            remaining_quantity
        } else {
            warn!(
                "OrderExecutedAtPriceSize: remaining_quantity {} > prev_qty {} for order_id {}. Using computed {}.",
                remaining_quantity, prev_qty, order_id, computed_new
            );
            computed_new
        };

        if !market_state.filtering || market_state.filtered_symbols.contains(&symbol) {
            if let Some(instrument_market) = market_state.instrument_markets.get_mut(&symbol) {
                // Ensure writer is wired through
                if instrument_market.tob_writer.is_none() {
                    instrument_market.tob_writer = market_state.tob_writer.clone();
                }
                let instrument_info = market_state.symbol_map.get(&symbol);

                // Reduce resting order at its current price
                instrument_market.modify_order(
                    market_state.time_reference,
                    time_offset,
                    side,
                    price,     // prev_price
                    price,     // new_price (unchanged)
                    prev_qty,
                    new_qty,
                    &symbol,
                    instrument_info,
                    "OrderExecutedAtPriceSize",
                );
            } else {
                trace!("OrderExecutedAtPriceSize: no InstrumentMarket for {}", String::from_utf8_lossy(&symbol));
            }
        }

        if new_qty == 0 {
            std::mem::drop(order); // release mutable borrow before removal
            market_state.order_map.remove_order(order_id);
        } else {
            order.quantity = new_qty;
        }
    } else {
        trace!("OrderExecutedAtPriceSize for unknown order_id: {}", order_id);
    }
}

fn handle_reduce_size_short(_market_state: &mut MarketState, _message_payload: &[u8], _config: &Config) {
    let _time_offset = u32::from_le_bytes(_message_payload[0..4].try_into().unwrap());
    let _order_id = u64::from_le_bytes(_message_payload[4..12].try_into().unwrap());
    let _cancelled_quantity = u16::from_le_bytes(_message_payload[12..14].try_into().unwrap()) as u32;

}

fn handle_modify_order_long(market_state: &mut MarketState, message_payload: &[u8], config: &Config) {
    let time_offset = u32::from_le_bytes(message_payload[0..4].try_into().unwrap());
    let order_id = u64::from_le_bytes(message_payload[4..12].try_into().unwrap());
    let new_quantity = u32::from_le_bytes(message_payload[12..16].try_into().unwrap());
    let new_price = u64::from_le_bytes(message_payload[16..24].try_into().unwrap());
    let _modify_flags = message_payload[24];

    if config.summary_only {
        return;
    }

    if let Some(order) = market_state.order_map.orders.get_mut(&order_id) {
        let symbol = order.symbol;
        let side = order.side;
        let prev_price = order.price;
        let prev_quantity = order.quantity;

        if !market_state.filtering || market_state.filtered_symbols.contains(&symbol) {
            let instrument_info = market_state.symbol_map.get(&symbol);
            let instrument_market = match market_state.instrument_markets.entry(symbol) {
                Entry::Occupied(e) => {
                    let im = e.into_mut();
                    if im.tob_writer.is_none() {
                        im.tob_writer = market_state.tob_writer.clone();
                    }
                    im
                }
                Entry::Vacant(v) => v.insert(InstrumentMarket::new_with_writer(market_state.tob_writer.clone())),
            };

            instrument_market.modify_order(
                market_state.time_reference,
                time_offset,
                side,
                prev_price,
                new_price,
                prev_quantity,
                new_quantity,
                &symbol,
                instrument_info,
                "ModifyOrderLong",
            );
        }

        if new_quantity == 0 {
            drop(order);
            market_state.order_map.remove_order(order_id);
        } else {
            order.price = new_price;
            order.quantity = new_quantity;
        }
    } else {
        trace!("ModifyOrderLong for unknown order_id: {}", order_id);
    }
}

fn handle_modify_order_short(market_state: &mut MarketState, message_payload: &[u8], config: &Config) {
    let time_offset = u32::from_le_bytes(message_payload[0..4].try_into().unwrap());
    let order_id = u64::from_le_bytes(message_payload[4..12].try_into().unwrap());
    let new_quantity = u16::from_le_bytes(message_payload[12..14].try_into().unwrap()) as u32;
    let new_price = u16::from_le_bytes(message_payload[14..16].try_into().unwrap()) as u64;
    let _modify_flags = message_payload[16];

    if config.summary_only {
        return;
    }

    if let Some(order) = market_state.order_map.orders.get_mut(&order_id) {
        let symbol = order.symbol;
        let side = order.side;
        let prev_price = order.price;
        let prev_quantity = order.quantity;

        if !market_state.filtering || market_state.filtered_symbols.contains(&symbol) {
            let instrument_info = market_state.symbol_map.get(&symbol);
            let instrument_market = match market_state.instrument_markets.entry(symbol) {
                Entry::Occupied(e) => {
                    let im = e.into_mut();
                    if im.tob_writer.is_none() {
                        im.tob_writer = market_state.tob_writer.clone();
                    }
                    im
                }
                Entry::Vacant(v) => v.insert(InstrumentMarket::new_with_writer(market_state.tob_writer.clone())),
            };

            instrument_market.modify_order(
                market_state.time_reference,
                time_offset,
                side,
                prev_price,
                new_price,
                prev_quantity,
                new_quantity,
                &symbol,
                instrument_info,
                "ModifyOrderShort",
            );
        }

        if new_quantity == 0 {
            drop(order);
            market_state.order_map.remove_order(order_id);
        } else {
            order.price = new_price;
            order.quantity = new_quantity;
        }
    } else {
        trace!("ModifyOrderShort for unknown order_id: {}", order_id);
    }
}
//

fn handle_delete_order(market_state: &mut MarketState, message_payload: &[u8], config: &Config) {
    let time_offset = u32::from_le_bytes(message_payload[0..4].try_into().unwrap());
    let order_id = u64::from_le_bytes(message_payload[4..12].try_into().unwrap());

    if !config.summary_only {
        if let Some(order) = market_state.order_map.remove_order(order_id) {
            let symbol = order.symbol;
            let price = order.price;
            let quantity = order.quantity;
            let side = order.side;

            if !market_state.filtering || market_state.filtered_symbols.contains(&symbol) {
                if let Some(instrument_market) = market_state.instrument_markets.get_mut(&symbol) {
                    let instrument_info = market_state.symbol_map.get(&symbol);
                    // Ensure writer is set if available
                    if instrument_market.tob_writer.is_none() {
                        instrument_market.tob_writer = market_state.tob_writer.clone();
                    }
                    instrument_market.remove_quantity(market_state.time_reference, time_offset, side, price, quantity, &symbol, instrument_info, "DeleteOrder");
                }
            }
        }
    }
}

fn handle_trade_short(_market_state: &mut MarketState, _message_payload: &[u8], _config: &Config) {
    // Trade Short message handling can be implemented here if needed
}

fn handle_symbol_mapping(_market_state: &mut MarketState, message_payload: &[u8], config: &Config) {
    let feed_symbol: [u8; 6] = message_payload[0..6].try_into().unwrap();
    let osi_symbol: [u8; 21] = message_payload[6..27].try_into().unwrap();
    let underlying: [u8; 8] = message_payload[27..35].try_into().unwrap();

    if !config.summary_only {
        debug!("feed_symbol: {}, osi_symbol: {}, underlying: {}", String::from_utf8_lossy(&feed_symbol), String::from_utf8_lossy(&osi_symbol), String::from_utf8_lossy(&underlying));
    }
}

fn handle_add_order_expanded(_market_state: &mut MarketState, _message_payload: &[u8], _config: &Config) {
    // Extended Add Order message handling can be implemented here if needed
    // For now, we will just log that this message type is encountered
    debug!("Encountered Add Order Extended message (0x2F), handling not implemented.");
}

fn handle_time_reference(_market_state: &mut MarketState, _message_payload: &[u8], _config: &Config) {
    // Time Reference message handling can be implemented here if needed
}

// TODO: handle AuctionCancel (0xAE) and AuctionTrade (0xAF) messages if needed?
fn dispatch_message(market_state: &mut MarketState, message_type: &u8, message_payload: &[u8], config: &Config) {
    match message_type {
        0x20 => handle_time(market_state, &message_payload, config),
        0x21 => handle_add_order_long(market_state, &message_payload, config),
        0x22 => handle_add_order_short(market_state, &message_payload, config),
        0x23 => handle_order_executed(market_state, &message_payload, config),
        0x24 => handle_order_executed_at_price_size(market_state, &message_payload, config),
        0x26 => handle_reduce_size_short(market_state, &message_payload, config),
        0x27 => handle_modify_order_long(market_state, &message_payload, config),
        0x28 => handle_modify_order_short(market_state, &message_payload, config),
        0x29 => handle_delete_order(market_state, &message_payload, config),
        0x2b => handle_trade_short(market_state, &message_payload, config),
        0x2e => handle_symbol_mapping(market_state, &message_payload, config),
        0x2f => handle_add_order_expanded(market_state, &message_payload, config),
        0xb1 => handle_time_reference(market_state, &message_payload, config),
        _ => { market_state.unsupported_message_types.insert(*message_type); },
    }
}

fn parse(
    data: &[u8],
    symbol_mapping: &HashMap<[u8; 6], SymbolMapping>,
    filtered_symbols: &HashSet<[u8; 6]>,
    config: &Config,
    dump_json: &mut Map<String, Value>
) -> io::Result<()> {
    let mut offset = 0;
    let mut market_state = MarketState::new();
    let mut message_stats: HashMap<u8, MessageStats> = HashMap::new();

    market_state.filtered_symbols = filtered_symbols.clone();
    market_state.symbol_map = symbol_mapping.clone();

    if config.top_of_book_provided {
        let mut wtr = csv::Writer::from_path(&config.top_of_book_file)?;
        wtr.write_record(&["time_ref", "time_offset", "symbol", "event", "underlying", "type", "strike", "expiry", "bid4_qty", "bid4_price", "bid3_qty", "bid3_price", "bid2_qty", "bid2_price", "bid1_qty", "bid1_price", "bid0_qty", "bid0_price", "offer0_price", "offer0_qty", "offer1_price", "offer1_qty", "offer2_price", "offer2_qty", "offer3_price", "offer3_qty", "offer4_price", "offer4_qty","reason"])?;
        // share writer with all InstrumentMarket instances
        market_state.tob_writer = Some(Rc::new(RefCell::new(wtr)));
    }

    debug!("Symbol mapping count: {}", market_state.symbol_map.len());
    if market_state.filtered_symbols.len() > 0 {
        market_state.filtering = true;
        info!("Filtering enabled for symbols matching the OSI root: {}", config.osi_root);
        if market_state.filtered_symbols.len() < 20 {
            info!("Symbols: {}", market_state.filtered_symbols.iter().map(|s| String::from_utf8_lossy(s).to_string()).collect::<Vec<_>>().join(", "));
        }
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

    if let Some(rc) = &market_state.tob_writer {
        rc.borrow_mut().flush()?;
    }

    let mut sorted_stats: Vec<_> = message_stats.iter().collect();
    sorted_stats.sort_by_key(|&(message_type, _)| *message_type);

    if config.verbose {
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
                .map(|t| format!("0x{:02X} ({:?})", t, MessageType::from_byte(*t).name()))
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

fn build_filtered_symbols(
    symbol_mapping: &HashMap<[u8; 6], SymbolMapping>,
    config: &Config,
) -> Result<HashSet<[u8; 6]>, Box<dyn Error>> {
    if config.osi_root_provided {
        let underlying = &config.osi_root;
        let option_type = &config.option_type;
        let expiry = config.option_expiry_date.trim();

        let set: HashSet<[u8; 6]> = symbol_mapping
            .values()
            .filter(|row| {
                // OSI root must match
                row.osi_root == *underlying &&
                // Option type must match if not None or Both
                (matches!(option_type, OptionType::None | OptionType::Both)
                    || row.call_put.map(|c| {
                        match option_type {
                            OptionType::Call => c.eq_ignore_ascii_case(&'C'),
                            OptionType::Put => c.eq_ignore_ascii_case(&'P'),
                            _ => true,
                        }
                    }).unwrap_or(false)
                ) &&
                // Expiry must match if provided (non-empty)
                (expiry.is_empty() || row.expiry.trim() == expiry) &&
                // Strike price must match if provided (non-empty Vec)
                (config.option_strike_prices.is_empty() || config.option_strike_prices.contains(&row.strike))
            })
            .map(|row| row.bats_symbol)
            .collect();

        if set.is_empty() {
            error!(
                "No symbols found for the provided underlying: {}, option_type: {:?}, expiry: {}",
                config.osi_root, config.option_type, config.option_expiry_date
            );
            return Err(Box::new(io::Error::new(io::ErrorKind::NotFound, "No matching symbols found")));
        }
        Ok(set)
    } else {
        Ok(HashSet::new())
    }
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
    let filtered_symbols = build_filtered_symbols(&symbol_mapping, config)?;

    // Insert filtered_symbols into the JSON map (as Vec<String> for readability)
    if config.dump_file_provided {
        debug!("Inserting filtered symbols to JSON dump");
        let filtered_vec: Vec<String> = filtered_symbols
            .iter()
            .map(|s| String::from_utf8_lossy(s).trim_end().to_string())
            .collect();
        dump_json.insert("filtered_symbols".to_string(), json!(filtered_vec));
    }
    
    info!("Filtered symbols count: {}", filtered_symbols.len().to_formatted_string(&Locale::en));

    // Open the data file
    let file = File::open(config.pitch_data_file_path.to_string()).map_err(|err| {
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