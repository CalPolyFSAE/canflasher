#![allow(non_snake_case)]
use std::time::{Duration, Instant};

use can_hal::{CanFrame, CanId, Timestamped};
use can_hal_kvaser::Classic;

pub mod web;

const CHUNK_SIZE: usize = 6;
const ACK_MAGIC: u16 = 0xA55A;
const ACK_TIMEOUT: Duration = Duration::from_secs(1);

#[repr(C)]
pub struct DataHeader {
    can_id: CanId,
    message_size: u32,
}

#[repr(C)]
pub struct DataFrame {
    seq_num: u16,
    payload: [u8; CHUNK_SIZE],
}

#[repr(C)]
pub struct AckFrame {
    magic: u16,
    next_seq_num: u16,
}

impl DataHeader {
    pub fn to_can_frame(&self, can_id: CanId) -> CanFrame {
        let mut data = [0; 8];
        data[..4].copy_from_slice(&self.can_id.raw().to_le_bytes());
        data[4..].copy_from_slice(&self.message_size.to_le_bytes());
        CanFrame::new(can_id, &data).expect("logic has failed us")
    }
}

impl DataFrame {
    /// Encodes one binary chunk as a classic CAN frame. The total message size
    /// is sent separately in the transfer header.
    pub fn to_can_frame(&self, can_id: CanId) -> CanFrame {
        let mut data = [0; 8];
        data[..(8 - CHUNK_SIZE)].copy_from_slice(&self.seq_num.to_le_bytes());
        data[(8 - CHUNK_SIZE)..].copy_from_slice(&self.payload);
        CanFrame::new(can_id, &data).expect("logic has failed us")
    }
}

impl AckFrame {
    pub fn new_from_frame(frame: CanFrame) -> Result<Self, &'static str> {
        let data = frame.data();
        if data.len() != 4 {
            return Err("ack frame must be 4 bytes");
        }
        let magic = u16::from_le_bytes(data[..2].try_into().unwrap());
        if magic != ACK_MAGIC {
            return Err("ack frame has invalid magic number");
        }
        let next_seq_num = u16::from_le_bytes(data[2..].try_into().unwrap());
        Ok(AckFrame {
            magic,
            next_seq_num,
        })
    }
}

#[derive(thiserror::Error, Debug)]
pub enum UploadError {
    #[error("the sliding-window size must be greater than zero")]
    InvalidWindowSize,
    #[error("binary is too large for 16-bit chunk sequence numbers")]
    BinaryTooLarge,
    #[error("failed to transmit a CAN frame")]
    Transmit,
    #[error("failed to receive a CAN frame")]
    Receive,
    #[error("timed out waiting for an acknowledgement")]
    AcknowledgementTimeout,
    #[error("received an acknowledgement outside the current window")]
    InvalidAcknowledgement,
}

pub struct Manager<C>
where
    C: can_hal::channel::Receive + can_hal::channel::Transmit,
{
    channel: C,
}

impl<C> Manager<C>
where
    C: can_hal::channel::Receive + can_hal::channel::Transmit,
{
    pub fn new(channel: C) -> Self {
        Manager { channel }
    }

    pub fn receive(
        &mut self,
    ) -> Result<Timestamped<CanFrame, C::Timestamp>, <C as can_hal::channel::Receive>::Error> {
        self.channel.receive()
    }

    pub fn transmit(
        &mut self,
        message: &CanFrame,
    ) -> Result<(), <C as can_hal::channel::Transmit>::Error> {
        self.channel.transmit(message)
    }

    pub fn upload_data_frame(
        &mut self,
        data_frame: &DataFrame,
        can_id: CanId,
    ) -> Result<(), UploadError> {
        let can_frame = data_frame.to_can_frame(can_id);
        self.transmit(&can_frame)
            .map_err(|_| UploadError::Transmit)?;
        Ok(())
    }

    pub fn upload_binary(
        &mut self,
        self_can_id: CanId,
        target_can_id: CanId,
        binary: &[u8],
        window_size: usize,
    ) -> Result<(), UploadError> {
        if window_size == 0 {
            return Err(UploadError::InvalidWindowSize);
        }

        if !(binary.len().div_ceil(CHUNK_SIZE) <= u16::MAX as usize) {
            return Err(UploadError::BinaryTooLarge);
        }

        let message_size = u32::try_from(binary.len()).map_err(|_| UploadError::BinaryTooLarge)?;
        let chunk_count = binary.len().div_ceil(CHUNK_SIZE);

        let header_payload = DataHeader {
            can_id: target_can_id,
            message_size,
        };
        self.transmit(&header_payload.to_can_frame(self_can_id))
            .map_err(|_| UploadError::Transmit)?;

        let mut first_unacked = 0usize;
        while first_unacked < chunk_count {
            let window_end = first_unacked.saturating_add(window_size).min(chunk_count);

            for sequence in first_unacked..window_end {
                let start = sequence * CHUNK_SIZE;
                let end = start.saturating_add(CHUNK_SIZE).min(binary.len());
                let mut payload = [0; CHUNK_SIZE];
                payload[..end - start].copy_from_slice(&binary[start..end]);

                self.upload_data_frame(
                    &DataFrame {
                        seq_num: sequence as u16,
                        payload,
                    },
                    self_can_id,
                )?;
            }

            let next_expected = self.receive_ack(target_can_id, first_unacked, window_end)?;
            if next_expected <= first_unacked || next_expected > window_end {
                return Err(UploadError::InvalidAcknowledgement);
            }
            first_unacked = next_expected;
        }

        Ok(())
    }

    fn receive_ack(
        &mut self,
        target_can_id: CanId,
        first_unacked: usize,
        window_end: usize,
    ) -> Result<usize, UploadError> {
        let deadline = Instant::now() + ACK_TIMEOUT;
        let response = loop {
            let remaining = deadline
                .checked_duration_since(Instant::now())
                .ok_or(UploadError::AcknowledgementTimeout)?;
            let response = self
                .channel
                .receive_timeout(remaining)
                .map_err(|_| UploadError::Receive)?
                .ok_or(UploadError::AcknowledgementTimeout)?
                .into_frame();

            if response.id() == target_can_id {
                break response;
            }
        };

        let ack: AckFrame =
            AckFrame::new_from_frame(response).map_err(|_| UploadError::InvalidAcknowledgement)?;
        if ack.next_seq_num < first_unacked as u16 || ack.next_seq_num > window_end as u16 {
            return Err(UploadError::InvalidAcknowledgement);
        }

        Ok(ack.next_seq_num as usize)
    }
}

impl Manager<can_hal_kvaser::KvaserChannel<Classic>> {
    pub fn new_kvaser() -> Result<Self, Box<dyn std::error::Error>> {
        let driver = can_hal_kvaser::KvaserDriver::new()?;
        let channel = driver.channel(0).classic(1_000_000)?.connect()?;
        Ok(Manager::new(channel))
    }
}

#[cfg(test)]
mod tests;
