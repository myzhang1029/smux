use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

use std::io::Cursor;

use crate::{Error, Result};

pub const SMUX_VERSION: u8 = 1;
pub const HEADER_SIZE: usize = 8;
// Max of u16
pub const MAX_PAYLOAD_SIZE: usize = u16::MAX as usize;

#[derive(Eq, PartialEq, Debug, Clone, Copy)]
pub(crate) enum MuxCommand {
    Sync = 0,
    Finish = 1,
    Push = 2,
    Nop = 3,
}

impl TryFrom<u8> for MuxCommand {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            0 => Ok(MuxCommand::Sync),
            1 => Ok(MuxCommand::Finish),
            2 => Ok(MuxCommand::Push),
            3 => Ok(MuxCommand::Nop),
            _ => Err(Error::InvalidCommand(value)),
        }
    }
}

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub(crate) struct MuxFrameHeader {
    pub version: u8,
    pub command: MuxCommand,
    pub length: u16,
    pub stream_id: u32,
}

impl MuxFrameHeader {
    #[inline]
    fn encode(self, buf: &mut BytesMut) {
        buf.put_u8(self.version);
        buf.put_u8(self.command as u8);
        buf.put_u16(self.length);
        buf.put_u32(self.stream_id);
    }

    #[inline]
    fn decode(buf: &[u8]) -> Result<Self> {
        let mut cursor = Cursor::new(buf);
        let version = cursor.get_u8();
        if version != SMUX_VERSION {
            return Err(Error::InvalidVersion(version));
        }
        let command = MuxCommand::try_from(cursor.get_u8())?;
        let length = cursor.get_u16();
        let stream_id = cursor.get_u32();
        Ok(Self {
            version,
            command,
            length,
            stream_id,
        })
    }
}

#[derive(Clone)]
pub(crate) struct MuxFrame {
    pub header: MuxFrameHeader,
    pub payload: Bytes,
}

impl MuxFrame {
    pub fn new(command: MuxCommand, stream_id: u32, payload: Bytes) -> Self {
        assert!(payload.len() <= MAX_PAYLOAD_SIZE);
        Self {
            header: MuxFrameHeader {
                version: SMUX_VERSION,
                command,
                // Make sure the payload length is less than MAX_PAYLOAD_SIZE
                length: u16::try_from(payload.len()).unwrap(),
                stream_id,
            },
            payload,
        }
    }
}

pub(crate) struct MuxCodec {}

impl Decoder for MuxCodec {
    type Item = MuxFrame;
    type Error = Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>> {
        if src.len() < HEADER_SIZE {
            return Ok(None);
        }
        let header = MuxFrameHeader::decode(src)?;
        let len = header.length as usize;
        if src.len() < HEADER_SIZE + len {
            return Ok(None);
        }
        src.advance(HEADER_SIZE);
        let payload = src.split_to(len).freeze();

        debug_assert!(payload.len() == len);
        let frame = MuxFrame { header, payload };

        Ok(Some(frame))
    }
}

impl Encoder<MuxFrame> for MuxCodec {
    type Error = Error;

    fn encode(&mut self, item: MuxFrame, dst: &mut BytesMut) -> Result<()> {
        if item.header.version != SMUX_VERSION {
            return Err(Error::InvalidVersion(item.header.version));
        }

        if item.payload.len() > MAX_PAYLOAD_SIZE {
            return Err(Error::PayloadTooLarge(item.payload.len()));
        }

        item.header.encode(dst);
        dst.put_slice(&item.payload);

        Ok(())
    }
}
