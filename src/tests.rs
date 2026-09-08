use super::*;
use std::collections::VecDeque;

const SENDER: CanId = CanId::Standard(0x123);
const TARGET: CanId = CanId::Standard(0x456);

// A strict script verifies bus ordering as well as bytes. Unexpected calls,
// extra sends, and reads before the whole window has been sent fail the test.
enum Step {
    Send(CanFrame),
    SendError,
    Receive(CanFrame),
    ReceiveError,
    Timeout,
}

struct ScriptedChannel {
    steps: VecDeque<Step>,
    timeouts: Vec<Duration>,
}

impl can_hal::channel::Transmit for ScriptedChannel {
    type Error = std::io::Error;

    fn transmit(&mut self, frame: &CanFrame) -> Result<(), Self::Error> {
        match self.steps.pop_front().expect("unexpected transmission") {
            Step::Send(expected) => {
                assert_eq!(frame.id(), expected.id());
                assert_eq!(frame.data(), expected.data());
                Ok(())
            }
            Step::SendError => Err(std::io::Error::other("transmit failed")),
            _ => panic!("transmitted before expected receive"),
        }
    }
}

//review this
impl can_hal::channel::Receive for ScriptedChannel {
    type Error = std::io::Error;
    type Timestamp = ();

    fn receive(&mut self) -> Result<Timestamped<CanFrame, ()>, Self::Error> {
        panic!("upload must use bounded receive")
    }

    fn try_receive(&mut self) -> Result<Option<Timestamped<CanFrame, ()>>, Self::Error> {
        panic!("upload must use bounded receive")
    }

    fn receive_timeout(
        &mut self,
        timeout: Duration,
    ) -> Result<Option<Timestamped<CanFrame, ()>>, Self::Error> {
        assert!(!timeout.is_zero() && timeout <= ACK_TIMEOUT);
        self.timeouts.push(timeout);
        match self.steps.pop_front().expect("unexpected receive") {
            Step::Receive(frame) => Ok(Some(Timestamped::new(frame, ()))),
            Step::ReceiveError => Err(std::io::Error::other("receive failed")),
            Step::Timeout => Ok(None),
            _ => panic!("received before expected transmission"),
        }
    }
}

fn manager(steps: Vec<Step>) -> Manager<ScriptedChannel> {
    Manager::new(ScriptedChannel {
        steps: steps.into(),
        timeouts: vec![],
    })
}

fn frame(id: CanId, bytes: &[u8]) -> CanFrame {
    CanFrame::new(id, bytes).unwrap()
}

fn header(size: u32) -> Step {
    let mut bytes = vec![0x56, 0x04, 0, 0];
    bytes.extend_from_slice(&size.to_le_bytes());
    Step::Send(frame(SENDER, &bytes))
}

fn data(sequence: u16, payload: [u8; 6]) -> Step {
    let mut bytes = sequence.to_le_bytes().to_vec();
    bytes.extend_from_slice(&payload);
    Step::Send(frame(SENDER, &bytes))
}

fn ack(next: u16) -> Step {
    let [lo, hi] = next.to_le_bytes();
    Step::Receive(frame(TARGET, &[0x5a, 0xa5, lo, hi]))
}

fn upload(steps: Vec<Step>, binary: &[u8], window: usize) -> Result<(), UploadError> {
    let mut manager = manager(steps);
    let result = manager.upload_binary(SENDER, TARGET, binary, window);
    assert!(
        manager.channel.steps.is_empty(),
        "upload stopped before script finished"
    );
    result
}

#[test]
fn wire_formats_are_little_endian() {
    let header = DataHeader {
        can_id: CanId::Extended(0x1234567),
        message_size: 0x12345678,
    };
    let encoded = header.to_can_frame(SENDER);
    assert_eq!(encoded.id(), SENDER);
    assert_eq!(
        encoded.data(),
        &[0x67, 0x45, 0x23, 0x01, 0x78, 0x56, 0x34, 0x12]
    );
    let encoded = DataFrame {
        seq_num: 0x1234,
        payload: [1, 2, 3, 4, 5, 6],
    }
    .to_can_frame(SENDER);
    assert_eq!(encoded.id(), SENDER);
    assert_eq!(encoded.data(), &[0x34, 0x12, 1, 2, 3, 4, 5, 6]);
    let decoded = AckFrame::new_from_frame(frame(TARGET, &[0x5a, 0xa5, 0x34, 0x12])).unwrap();
    assert_eq!(decoded.magic, 0xa55a);
    assert_eq!(decoded.next_seq_num, 0x1234);
}

#[test]
fn ack_rejects_wrong_lengths_and_magic() {
    for len in 0..=8 {
        if len != 4 {
            assert!(AckFrame::new_from_frame(frame(TARGET, &vec![0; len])).is_err());
        }
    }
    for bytes in [[0xa5, 0x5a, 1, 0], [0, 0, 1, 0]] {
        assert!(AckFrame::new_from_frame(frame(TARGET, &bytes)).is_err());
    }
}

#[test]
fn empty_binary_sends_only_header() {
    upload(vec![header(0)], &[], 8).unwrap();
}

#[test]
fn splitting_round_trips_lengths_and_window_boundaries() {
    for len in 1..=80 {
        let binary: Vec<u8> = (0..len).map(|i| (i * 37) as u8).collect();
        for window in [1, 2, 3, 8, usize::MAX] {
            let mut steps = vec![header(len as u32)];
            let chunks: Vec<_> = binary.chunks(6).collect();
            for (index, chunk) in chunks.iter().enumerate() {
                let mut payload = [0; 6];
                payload[..chunk.len()].copy_from_slice(chunk);
                steps.push(data(index as u16, payload));
                if (index + 1) % window == 0 || index + 1 == chunks.len() {
                    steps.push(ack((index + 1) as u16));
                }
            }
            upload(steps, &binary, window).unwrap();
        }
    }
}

#[test]
fn partial_ack_retransmits_unacknowledged_chunks_and_advances_window() {
    upload(
        vec![
            header(24),
            data(0, [1; 6]),
            data(1, [2; 6]),
            data(2, [3; 6]),
            ack(1),
            data(1, [2; 6]),
            data(2, [3; 6]),
            data(3, [4; 6]),
            ack(4),
        ],
        &[vec![1; 6], vec![2; 6], vec![3; 6], vec![4; 6]].concat(),
        3,
    )
    .unwrap();
}

#[test]
fn unrelated_ids_are_ignored_within_same_ack_deadline() {
    let mut manager = manager(vec![
        header(1),
        data(0, [7, 0, 0, 0, 0, 0]),
        Step::Receive(frame(SENDER, &[0])),
        Step::Receive(frame(CanId::Extended(0x456), &[0x5a, 0xa5, 1, 0])),
        ack(1),
    ]);
    manager.upload_binary(SENDER, TARGET, &[7], 1).unwrap();
    assert!(manager.channel.steps.is_empty());
    assert_eq!(manager.channel.timeouts.len(), 3);
    assert!(
        manager
            .channel
            .timeouts
            .windows(2)
            .all(|pair| pair[1] <= pair[0])
    );
}

#[test]
fn invalid_inputs_do_not_touch_bus() {
    assert!(matches!(
        upload(vec![], &[1], 0),
        Err(UploadError::InvalidWindowSize)
    ));
    assert!(matches!(
        upload(vec![], &vec![0; u16::MAX as usize * 6 + 1], 8),
        Err(UploadError::BinaryTooLarge)
    ));
}

#[test]
fn maximum_binary_uses_last_sequence_without_wraparound() {
    let binary = vec![0xab; u16::MAX as usize * 6];
    let mut steps = vec![header(binary.len() as u32)];
    for seq in 0..u16::MAX {
        steps.push(data(seq, [0xab; 6]));
    }
    steps.push(ack(u16::MAX));
    upload(steps, &binary, usize::MAX).unwrap();
}

#[test]
fn transmit_failures_stop_at_header_or_mid_window() {
    assert!(matches!(
        upload(vec![Step::SendError], &[1; 12], 2),
        Err(UploadError::Transmit)
    ));
    assert!(matches!(
        upload(vec![header(12), Step::SendError], &[1; 12], 2),
        Err(UploadError::Transmit)
    ));
    assert!(matches!(
        upload(
            vec![header(12), data(0, [1; 6]), Step::SendError],
            &[1; 12],
            2
        ),
        Err(UploadError::Transmit)
    ));
}

#[test]
fn receive_error_and_timeout_abort_upload() {
    for (step, timeout) in [(Step::ReceiveError, false), (Step::Timeout, true)] {
        let result = upload(vec![header(12), data(0, [1; 6]), step], &[1; 12], 1);
        assert!(matches!(
            (result, timeout),
            (Err(UploadError::Receive), false) | (Err(UploadError::AcknowledgementTimeout), true)
        ));
    }
}

#[test]
fn malformed_nonadvancing_and_future_acks_abort_upload() {
    for response in [
        ack(0),
        ack(3),
        Step::Receive(frame(TARGET, &[0x5a, 0xa5, 1])),
        Step::Receive(frame(TARGET, &[0, 0, 1, 0])),
    ] {
        let result = upload(
            vec![header(18), data(0, [1; 6]), data(1, [1; 6]), response],
            &[1; 18],
            2,
        );
        assert!(matches!(result, Err(UploadError::InvalidAcknowledgement)));
    }
}

#[test]
fn stale_and_duplicate_acks_after_progress_abort_upload() {
    for next in [0, 1] {
        let result = upload(
            vec![
                header(12),
                data(0, [1; 6]),
                ack(1),
                data(1, [1; 6]),
                ack(next),
            ],
            &[1; 12],
            1,
        );
        assert!(matches!(result, Err(UploadError::InvalidAcknowledgement)));
    }
}
