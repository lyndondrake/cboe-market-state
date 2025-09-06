use nom::{
    IResult,
    bytes::complete::take,
    number::complete::{le_u16, le_u32, le_u64, u8 as be_u8},
    multi::count,
};

#[derive(Debug, Clone)]
pub struct SequencedUnitHeader {
    pub raw: [u8; 8],
    pub message_count: u8, // matches your current usage (byte 2)
}

#[derive(Debug, Clone)]
pub struct MessageFrame<'a> {
    pub ty: u8,
    pub payload: &'a [u8],
}

#[derive(Debug, Clone)]
pub struct SequencedUnit<'a> {
    pub header: SequencedUnitHeader,
    pub frames: Vec<MessageFrame<'a>>,
}

// High-level parse for a single Sequenced Unit
pub fn parse_sequenced_unit(input: &[u8]) -> IResult<&[u8], SequencedUnit> {
    let (rest, hdr_bytes) = take(8usize)(input)?;
    let mut raw = [0u8; 8];
    raw.copy_from_slice(hdr_bytes);

    // Per your existing code, message_count is at byte 2 of the SU header
    let message_count = raw[2];

    // Parse message_count frames in sequence
    let (rest, frames) = count(parse_message_frame, message_count as usize)(rest)?;

    Ok((rest, SequencedUnit {
        header: SequencedUnitHeader { raw, message_count },
        frames,
    }))
}

// Parse a single message frame: [len: u8][ty: u8][payload...(len-2)]
pub fn parse_message_frame(input: &[u8]) -> IResult<&[u8], MessageFrame> {
    let (rest, len) = be_u8(input)?;
    // Length includes type byte and length byte; must be >= 2
    let (rest, ty) = be_u8(rest)?;
    let payload_len = len.saturating_sub(2) as usize;
    let (rest, payload) = take(payload_len)(rest)?;
    Ok((rest, MessageFrame { ty, payload }))
}

// Optional: parsed/typed messages (template)
#[derive(Debug, Clone)]
pub enum PitchMessage<'a> {
    AddOrderLong(AddOrderLong),
    AddOrderShort(AddOrderShort),
    Raw { ty: u8, payload: &'a [u8] },
}

#[derive(Debug, Clone)]
pub struct AddOrderLong {
    pub time_offset: u32,
    pub order_id: u64,
    pub side: char,
    pub quantity: u32,
    pub symbol: [u8; 6],
    pub price: u64,
}

#[derive(Debug, Clone)]
pub struct AddOrderShort {
    pub time_offset: u32,
    pub order_id: u64,
    pub side: char,
    pub quantity: u32,
    pub symbol: [u8; 6],
    pub price: u64,
}

pub fn decode_frame<'a>(frame: &MessageFrame<'a>) -> PitchMessage<'a> {
    match frame.ty {
        0x21 => parse_add_order_long(frame.payload).map(PitchMessage::AddOrderLong)
                  .unwrap_or(PitchMessage::Raw { ty: frame.ty, payload: frame.payload }),
        0x22 => parse_add_order_short(frame.payload).map(PitchMessage::AddOrderShort)
                  .unwrap_or(PitchMessage::Raw { ty: frame.ty, payload: frame.payload }),
        _ => PitchMessage::Raw { ty: frame.ty, payload: frame.payload },
    }
}

// Parsers for specific message payloads (match your current handlers)
fn parse_add_order_long(input: &[u8]) -> Result<AddOrderLong, ()> {
    // Offsets: time_offset[0..4], order_id[4..12], side[12], qty[13..17], symbol[17..23], price[23..31]
    let (_, time_offset) = le_u32(input).map_err(|_| ())?;
    let input = &input[4..];
    let (_, order_id) = le_u64(input).map_err(|_| ())?;
    let side = input[8] as char;
    let (_, quantity) = le_u32(&input[9..]).map_err(|_| ())?;
    let mut symbol = [0u8; 6];
    symbol.copy_from_slice(&input[13..19]); // 17..23 overall
    let (_, price) = le_u64(&input[19..]).map_err(|_| ())?;
    Ok(AddOrderLong { time_offset, order_id, side, quantity, symbol, price })
}

fn parse_add_order_short(input: &[u8]) -> Result<AddOrderShort, ()> {
    // Offsets: time_offset[0..4], order_id[4..12], side[12], qty[13..15], symbol[15..21], price[21..23]
    let (_, time_offset) = le_u32(input).map_err(|_| ())?;
    let input = &input[4..];
    let (_, order_id) = le_u64(input).map_err(|_| ())?;
    let side = input[8] as char;
    let (_, qty16) = le_u16(&input[9..]).map_err(|_| ())?;
    let quantity = qty16 as u32;
    let mut symbol = [0u8; 6];
    symbol.copy_from_slice(&input[11..17]); // 15..21 overall
    let (_, price16) = le_u16(&input[17..]).map_err(|_| ())?;
    let price = price16 as u64;
    Ok(AddOrderShort { time_offset, order_id, side, quantity, symbol, price })
}

// Parse the entire buffer into a Vec of Sequenced Units
pub fn parse_stream(mut input: &[u8]) -> IResult<&[u8], Vec<SequencedUnit>> {
    let mut units = Vec::new();
    while !input.is_empty() {
        match parse_sequenced_unit(input) {
            Ok((rest, su)) => {
                units.push(su);
                input = rest;
            }
            Err(_) => break,
        }
    }
    Ok((input, units))
}