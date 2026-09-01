#![allow(non_snake_case)]
use can_hal::{CanFrame, CanId, Timestamped};
use can_hal_kvaser::Classic;

pub mod web;

#[repr(C)]
pub struct DataHeader {
    can_id: CanId,
    message_size: u32,
}

impl DataHeader {
    pub fn to_can_frame(&self, can_id: CanId) -> CanFrame {
        let mut data = [0; 8];
        data[..4].copy_from_slice(&self.can_id.raw().to_le_bytes());
        data[4..].copy_from_slice(&self.message_size.to_le_bytes());
        CanFrame::new(can_id, &data).expect("logic has failed us")
    }
}

#[repr(C)]
pub struct DataFrame {
    seq_num: u32,
    payload: [u8; 4],
}

impl DataFrame {
    /// Encodes one binary chunk as a classic CAN frame. The total message size
    /// is sent separately in the transfer header.
    pub fn to_can_frame(&self, can_id: CanId) -> CanFrame {
        let mut data = [0; 8];
        data[..4].copy_from_slice(&self.seq_num.to_le_bytes());
        data[4..].copy_from_slice(&self.payload);
        CanFrame::new(can_id, &data).expect("logic has failed us")
    }
}

#[derive(thiserror::Error, Debug)]
pub enum UploadError {
    #[error("the sliding-window size must be greater than zero")]
    InvalidWindowSize,
    #[error("binary is too large for 32-bit chunk sequence numbers")]
    BinaryTooLarge,
    #[error("failed to transmit a CAN frame")]
    Transmit,
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

        let message_size = u32::try_from(binary.len()).map_err(|_| UploadError::BinaryTooLarge)?;
        let chunk_count = binary.len().div_ceil(4);

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
                let start = sequence * 4;
                let end = start.saturating_add(4).min(binary.len());
                let mut payload = [0; 4];
                payload[..end - start].copy_from_slice(&binary[start..end]);

                self.upload_data_frame(
                    &DataFrame {
                        seq_num: sequence as u32,
                        payload,
                    },
                    self_can_id,
                )?;
            }

            let next_expected = self.receive_acks_dummy(first_unacked, window_end)?;
            if next_expected <= first_unacked || next_expected > window_end {
                return Err(UploadError::InvalidAcknowledgement);
            }
            first_unacked = next_expected;
        }

        Ok(())
    }

    // todo: implement real acks - parse cumulative ack and return next seq num
    fn receive_acks_dummy(
        &mut self,
        _first_unacked: usize,
        window_end: usize,
    ) -> Result<usize, UploadError> {
        Ok(window_end)
    }
}

impl Manager<can_hal_kvaser::KvaserChannel<Classic>> {
    pub fn new_kvaser() -> Result<Self, Box<dyn std::error::Error>> {
        let driver = can_hal_kvaser::KvaserDriver::new()?;
        let channel = driver.channel(0).classic(1_000_000)?.connect()?;
        Ok(Manager::new(channel))
    }
}
