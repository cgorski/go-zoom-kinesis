use crate::processor::{CheckpointMetadata, RecordMetadata};
use crate::{
    client::KinesisClientTrait, error::ProcessingError, processor::RecordProcessor, retry::Backoff,
    store::CheckpointStore, ProcessorConfig,
};
use async_trait::async_trait;
use aws_sdk_kinesis::types::{Record, Shard, ShardIteratorType};
use chrono::{DateTime, Utc};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
    time::Duration,
};

use crate::client::KinesisClientError;
use crate::error::BeforeCheckpointError;
use anyhow::Result;
use parking_lot;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio::sync::{mpsc::Sender, Mutex, RwLock};
use tokio::time::Instant;
use tracing::debug;

/// Mock Kinesis client for testing

#[derive(Debug, Default, Clone)]
pub struct MockKinesisClient {
    #[allow(clippy::type_complexity)]
    list_shards_responses: Arc<Mutex<VecDeque<Result<Vec<Shard>, KinesisClientError>>>>,
    #[allow(clippy::type_complexity)]
    get_iterator_responses: Arc<Mutex<VecDeque<Result<String, KinesisClientError>>>>,
    #[allow(clippy::type_complexity)]
    get_records_responses:
        Arc<Mutex<VecDeque<Result<(Vec<Record>, Option<String>), KinesisClientError>>>>,
    iterator_request_count: Arc<AtomicUsize>,
    get_records_delay: Arc<RwLock<Option<Duration>>>,
}

impl MockKinesisClient {
    pub async fn mock_error(&self, error: KinesisClientError) {
        self.get_records_responses
            .lock()
            .await
            .push_back(Err(error));
    }
    pub async fn mock_expired_iterator(&self) {
        // Clear any existing responses
        self.get_records_responses.lock().await.clear();
        // Add the expired iterator error
        self.get_records_responses
            .lock()
            .await
            .push_back(Err(KinesisClientError::ExpiredIterator));
    }

    pub async fn mock_throughput_exceeded(&self) {
        self.mock_error(KinesisClientError::ThroughputExceeded)
            .await;
    }
    pub async fn mock_default_responses(&self) {
        self.get_records_responses
            .lock()
            .await
            .push_back(Ok((vec![], Some("default-iterator".to_string()))));
    }
    pub fn new() -> Self {
        Self::default()
    }

    // Add new constructor for tests that need delays
    pub fn new_with_delay(delay: Duration) -> Self {
        Self {
            get_records_delay: Arc::new(RwLock::new(Some(delay))),
            ..Default::default()
        }
    }

    pub async fn get_iterator_request_count(&self) -> usize {
        self.iterator_request_count.load(Ordering::SeqCst)
    }

    async fn increment_iterator_count(&self) {
        self.iterator_request_count.fetch_add(1, Ordering::SeqCst);
    }
    pub async fn mock_list_shards(&self, response: Result<Vec<Shard>, KinesisClientError>) {
        self.list_shards_responses.lock().await.push_back(response);
    }

    pub async fn mock_get_iterator(&self, response: Result<String, KinesisClientError>) {
        self.get_iterator_responses.lock().await.push_back(response);
    }

    pub async fn mock_get_records(
        &self,
        response: Result<(Vec<Record>, Option<String>), KinesisClientError>,
    ) {
        self.get_records_responses.lock().await.push_back(response);
    }
}

#[async_trait]
impl KinesisClientTrait for MockKinesisClient {
    async fn list_shards(&self, _stream_name: &str) -> Result<Vec<Shard>, KinesisClientError> {
        self.list_shards_responses
            .lock()
            .await
            .pop_front()
            .unwrap_or_else(|| Ok(vec![]))
    }

    async fn get_shard_iterator(
        &self,
        _stream_name: &str,
        _shard_id: &str,
        _iterator_type: ShardIteratorType,
        _sequence_number: Option<&str>,
        _timestamp: Option<&DateTime<Utc>>,
    ) -> Result<String, KinesisClientError> {
        self.increment_iterator_count().await;
        self.get_iterator_responses
            .lock()
            .await
            .pop_front()
            .unwrap_or_else(|| Ok("mock-iterator".to_string()))
    }
    async fn get_records(
        &self,
        _iterator: &str,
        _limit: i32,
        retry_count: u32,
        max_retries: Option<u32>,
        shutdown: &mut tokio::sync::watch::Receiver<bool>,
    ) -> Result<(Vec<Record>, Option<String>), KinesisClientError> {
        if *shutdown.borrow() {
            return Err(KinesisClientError::Other("Shutdown requested".to_string()));
        }

        // Add delay if configured
        if let Some(delay) = *self.get_records_delay.read().await {
            tokio::time::sleep(delay).await;
        }

        match self.get_records_responses.lock().await.pop_front() {
            Some(result) => {
                match result {
                    Ok(response) => Ok(response),
                    Err(e) => {
                        // Immediately return ExpiredIterator error
                        if matches!(e, KinesisClientError::ExpiredIterator) {
                            return Err(e);
                        }

                        if retry_count >= max_retries.unwrap_or(3) {
                            Err(e)
                        } else {
                            let delay = Duration::from_millis(100 * (2_u64.pow(retry_count)));
                            tokio::time::sleep(delay).await;
                            self.get_records_responses.lock().await.push_front(Err(e));
                            self.get_records(
                                _iterator,
                                _limit,
                                retry_count + 1,
                                max_retries,
                                shutdown,
                            )
                                .await
                        }
                    }
                }
            }
            None => Ok((vec![], None)),
        }
    }
}
#[derive(Debug, Clone)]
pub struct MockRecordProcessor {
    processed_records: Arc<RwLock<Vec<Record>>>,
    process_count: Arc<RwLock<usize>>,
    error_count: Arc<RwLock<usize>>,
    pre_process_delay: Arc<RwLock<Option<Duration>>>,
    error_tx: Arc<Mutex<Option<Sender<String>>>>,
    should_fail: Arc<RwLock<bool>>,
    failure_sequences: Arc<RwLock<HashSet<String>>>,
    failure_types: Arc<RwLock<HashMap<String, String>>>,
    failure_attempts: Arc<RwLock<HashMap<String, usize>>>,
    expected_attempts: Arc<RwLock<HashMap<String, u32>>>,
    config: Arc<RwLock<Option<ProcessorConfig>>>,
    processing_times: Arc<RwLock<HashMap<String, Duration>>>,
    pub before_checkpoint_results: Arc<RwLock<VecDeque<Result<(), BeforeCheckpointError>>>>,
    never_complete: Arc<AtomicBool>,
}
impl Default for MockRecordProcessor {
    fn default() -> Self {
        Self::new()
    }
}

impl MockRecordProcessor {
    pub fn new() -> Self {
        Self {
            processed_records: Arc::new(RwLock::new(Vec::new())),
            process_count: Arc::new(RwLock::new(0)),
            error_count: Arc::new(RwLock::new(0)),
            pre_process_delay: Arc::new(RwLock::new(None)),
            error_tx: Arc::new(Mutex::new(None)),
            should_fail: Arc::new(RwLock::new(false)),
            failure_sequences: Arc::new(RwLock::new(HashSet::new())),
            failure_types: Arc::new(RwLock::new(HashMap::new())),
            failure_attempts: Arc::new(RwLock::new(HashMap::new())),
            expected_attempts: Arc::new(RwLock::new(HashMap::new())),
            config: Arc::new(RwLock::new(None)),
            processing_times: Arc::new(RwLock::new(HashMap::new())),
            before_checkpoint_results: Arc::new(RwLock::new(VecDeque::new())),
            never_complete: Arc::new(AtomicBool::new(false)),
        }
    }

    pub async fn set_never_complete(&self, value: bool) {
        self.never_complete.store(value, Ordering::SeqCst);
    }

    pub async fn get_processing_times(&self) -> HashMap<String, Duration> {
        self.processing_times.read().await.clone()
    }

    pub async fn configure_before_checkpoint_behavior(&self, behavior: BeforeCheckpointError) {
        self.before_checkpoint_results
            .write()
            .await
            .push_back(Err(behavior));
    }

    async fn record_attempt(
        &self,
        sequence: &str,
        success: bool,
        attempt: u32,
        error: Option<String>,
        duration: Duration,
    ) {
        let max_attempts = self
            .expected_attempts
            .read()
            .await
            .get(sequence)
            .copied()
            .unwrap_or(3);

        let is_final = !success && attempt >= max_attempts;

        if let Some(tx) = &*self.error_tx.lock().await {
            let msg = if success {
                format!(
                    "Successfully processed sequence {} on attempt {}",
                    sequence, attempt
                )
            } else {
                format!(
                    "Failed to process sequence {} on attempt {} (final: {}): {:?}",
                    sequence, attempt, is_final, error
                )
            };
            let _ = tx.send(msg).await;
        }

        if !success {
            *self.error_count.write().await += 1;
        }

        self.processing_times
            .write()
            .await
            .insert(sequence.to_string(), duration);
    }

    pub async fn get_processing_time(&self, sequence: &str) -> Option<Duration> {
        self.processing_times.read().await.get(sequence).copied()
    }
    pub async fn set_config(&self, config: ProcessorConfig) {
        *self.config.write().await = Some(config);
    }

    pub async fn set_failure_sequences(&self, sequences: Vec<String>) {
        debug!(
            sequences = ?sequences,
            "Setting multiple failure sequences"
        );

        let mut failure_seqs = self.failure_sequences.write().await;
        let mut failure_types = self.failure_types.write().await;
        let mut expected_attempts = self.expected_attempts.write().await;

        for sequence in sequences {
            failure_seqs.insert(sequence.clone());
            failure_types.insert(sequence.clone(), "soft".to_string()); // Default to soft failure
            expected_attempts.insert(sequence.clone(), 3); // Default to 3 attempts

            debug!(
                sequence = %sequence,
                "Configured failure sequence"
            );
        }
    }

    // Add a more explicit configuration method
    pub async fn configure_failure(&self, sequence: String, failure_type: &str, max_attempts: u32) {
        self.failure_sequences
            .write()
            .await
            .insert(sequence.clone());
        self.failure_types
            .write()
            .await
            .insert(sequence.clone(), failure_type.to_string());
        self.expected_attempts
            .write()
            .await
            .insert(sequence.clone(), max_attempts);

        debug!(
            sequence = %sequence,
            failure_type = %failure_type,
            max_attempts = max_attempts,
            "Configured failure sequence"
        );
    }

    pub async fn set_should_fail(&self, should_fail: bool) {
        *self.should_fail.write().await = should_fail;
        if should_fail {
            // Add a failure sequence for testing
            self.set_failure_sequence("test-sequence".to_string(), "soft".to_string(), 1)
                .await;
        }
    }

    pub async fn set_pre_process_delay(&self, delay: Option<Duration>) {
        *self.pre_process_delay.write().await = delay;
    }

    pub async fn set_error_sender(&self, sender: Sender<String>) {
        *self.error_tx.lock().await = Some(sender);
    }

    pub async fn set_failure_sequence(
        &self,
        sequence: String,
        failure_type: String,
        expected_attempts: u32,
    ) {
        tracing::info!(
            "Setting failure sequence - sequence: {}, type: {}, expected_attempts: {}",
            sequence,
            failure_type,
            expected_attempts
        );

        self.failure_sequences
            .write()
            .await
            .insert(sequence.clone());
        self.failure_types
            .write()
            .await
            .insert(sequence.clone(), failure_type);
        self.expected_attempts
            .write()
            .await
            .insert(sequence, expected_attempts);
    }

    pub async fn configure_failures(&self, failures: Vec<(String, String, u32)>) {
        for (sequence, failure_type, attempts) in failures {
            self.set_failure_sequence(sequence, failure_type, attempts)
                .await;
        }
    }

    pub async fn get_process_count(&self) -> usize {
        *self.process_count.read().await
    }

    pub async fn get_error_count(&self) -> usize {
        *self.error_count.read().await
    }

    pub async fn get_processed_records(&self) -> Vec<Record> {
        self.processed_records.read().await.clone()
    }

    pub async fn was_record_processed(&self, sequence: &str) -> bool {
        let processed_records = self.processed_records.read().await;
        processed_records
            .iter()
            .any(|r| r.sequence_number() == sequence)
    }

    pub async fn get_failure_attempts(&self, sequence: &str) -> usize {
        self.failure_attempts
            .read()
            .await
            .get(sequence)
            .copied()
            .unwrap_or(0)
    }

    pub async fn get_expected_attempts(&self, sequence: &str) -> Option<u32> {
        self.expected_attempts.read().await.get(sequence).copied()
    }

    pub async fn verify_failure_handling(&self, sequence: &str) -> anyhow::Result<()> {
        let failure_type = self.failure_types.read().await.get(sequence).cloned();
        let actual_attempts = self.get_failure_attempts(sequence).await;
        let expected_attempts = self.get_expected_attempts(sequence).await;

        match (failure_type, expected_attempts) {
            (Some(ftype), Some(expected)) => {
                match ftype.as_str() {
                    "hard" => {
                        assert_eq!(
                            actual_attempts, 1,
                            "Hard failure sequence {} had {} attempts, expected 1",
                            sequence, actual_attempts
                        );
                    }
                    "soft" => {
                        assert_eq!(
                            actual_attempts, expected as usize,
                            "Soft failure sequence {} had {} attempts, expected {}",
                            sequence, actual_attempts, expected
                        );
                    }
                    "partial" => {
                        assert!(
                            actual_attempts <= expected as usize,
                            "Partial failure sequence {} had {} attempts, expected <= {}",
                            sequence,
                            actual_attempts,
                            expected
                        );
                    }
                    _ => anyhow::bail!("Unknown failure type: {}", ftype),
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }
}
#[async_trait]
impl RecordProcessor for MockRecordProcessor {
    type Item = Record;

    async fn process_record<'a>(
        &self,
        record: &'a Record,
        metadata: RecordMetadata<'a>,
    ) -> std::result::Result<Option<Self::Item>, ProcessingError> {
        if self.never_complete.load(Ordering::SeqCst) {
            // This future will never complete
            futures::future::pending::<()>().await;
            unreachable!();
        }
        let start_time = Instant::now();
        let sequence = record.sequence_number().to_string();
        let attempt_number = metadata.attempt_number();

        // Record this attempt
        {
            let mut attempts = self.failure_attempts.write().await;
            attempts.insert(sequence.clone(), attempt_number as usize);
        }

        // Check if this sequence is configured to fail
        if self.failure_sequences.read().await.contains(&sequence) {
            let failure_type = self
                .failure_types
                .read()
                .await
                .get(&sequence)
                .cloned()
                .unwrap_or_else(|| "soft".to_string());

            let max_attempts = self
                .expected_attempts
                .read()
                .await
                .get(&sequence)
                .copied()
                .unwrap_or(2);

            let duration = start_time.elapsed();

            debug!(
                sequence = %sequence,
                attempt = attempt_number,
                max_attempts = max_attempts,
                "Processing attempt"
            );

            match failure_type.as_str() {
                "hard" => {
                    let error = format!("Simulated hard failure for sequence {}", sequence);
                    self.record_attempt(
                        &sequence,
                        false,
                        attempt_number,
                        Some(error.clone()),
                        duration,
                    )
                    .await;
                    return Err(ProcessingError::HardFailure(anyhow::anyhow!(error)));
                }
                "soft" => {
                    if attempt_number < max_attempts {
                        let error = format!(
                            "Simulated soft failure for sequence {} (attempt {} of {})",
                            sequence, attempt_number, max_attempts
                        );
                        self.record_attempt(
                            &sequence,
                            false,
                            attempt_number,
                            Some(error.clone()),
                            duration,
                        )
                        .await;
                        return Err(ProcessingError::SoftFailure(anyhow::anyhow!(error)));
                    }
                }
                "hang" => {
                    // Create a future that never completes to test timeout
                    std::future::pending::<()>().await;
                    unreachable!()
                }
                _ => {}
            }
        }

        // Check for configured delay
        if let Some(delay) = *self.pre_process_delay.read().await {
            tokio::time::sleep(delay).await;
        }

        // Record successful processing
        let duration = start_time.elapsed();
        self.processed_records.write().await.push(record.clone());
        *self.process_count.write().await += 1;

        self.record_attempt(&sequence, true, attempt_number, None, duration)
            .await;

        debug!(
            sequence = %sequence,
            attempt = attempt_number,
            "Successfully processed record"
        );

        Ok(Some(record.clone()))
    }

    async fn before_checkpoint(
        &self,
        _processed_items: Vec<Self::Item>,
        _metadata: CheckpointMetadata<'_>,
    ) -> std::result::Result<(), BeforeCheckpointError> {
        if let Some(result) = self.before_checkpoint_results.write().await.pop_front() {
            result
        } else {
            Ok(())
        }
    }
}
/// Mock checkpoint store for testing
#[derive(Debug, Default, Clone)]
pub struct MockCheckpointStore {
    checkpoints: Arc<RwLock<HashMap<String, String>>>,
    get_checkpoint_responses: Arc<Mutex<VecDeque<Result<Option<String>>>>>,
    save_checkpoint_responses: Arc<Mutex<VecDeque<Result<()>>>>,
    save_count: Arc<RwLock<usize>>,
}

impl MockCheckpointStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn mock_get_checkpoint(&self, response: Result<Option<String>>) {
        self.get_checkpoint_responses
            .lock()
            .await
            .push_back(response);
    }

    pub async fn mock_save_checkpoint(&self, response: Result<()>) {
        self.save_checkpoint_responses
            .lock()
            .await
            .push_back(response);
    }

    pub async fn get_save_count(&self) -> usize {
        *self.save_count.read().await
    }

    pub async fn get_stored_checkpoints(&self) -> HashMap<String, String> {
        self.checkpoints.read().await.clone()
    }

    pub async fn get_all_checkpoints(&self) -> anyhow::Result<HashMap<String, String>> {
        Ok(self.checkpoints.read().await.clone())
    }
}

#[async_trait]
impl CheckpointStore for MockCheckpointStore {
    async fn get_checkpoint(&self, shard_id: &str) -> Result<Option<String>> {
        if let Some(response) = self.get_checkpoint_responses.lock().await.pop_front() {
            response
        } else {
            Ok(self.checkpoints.read().await.get(shard_id).cloned())
        }
    }

    async fn save_checkpoint(&self, shard_id: &str, sequence_number: &str) -> Result<()> {
        if let Some(response) = self.save_checkpoint_responses.lock().await.pop_front() {
            response
        } else {
            self.checkpoints
                .write()
                .await
                .insert(shard_id.to_string(), sequence_number.to_string());
            *self.save_count.write().await += 1;
            Ok(())
        }
    }
}

/// Mock backoff for testing
pub struct MockBackoff {
    delays: Arc<parking_lot::Mutex<Vec<Duration>>>,
    current_index: Arc<parking_lot::Mutex<usize>>,
}

impl MockBackoff {
    pub fn new(delays: Vec<Duration>) -> Self {
        Self {
            delays: Arc::new(parking_lot::Mutex::new(delays)),
            current_index: Arc::new(parking_lot::Mutex::new(0)),
        }
    }

    pub fn get_call_count(&self) -> usize {
        *self.current_index.lock()
    }
}

impl Backoff for MockBackoff {
    fn next_delay(&self, _attempt: u32) -> Duration {
        let mut index = self.current_index.lock();
        let delays = self.delays.lock();

        let delay = delays
            .get(*index)
            .cloned()
            .unwrap_or(Duration::from_millis(100));

        *index += 1;
        delay
    }

    fn reset(&mut self) {
        *self.current_index.lock() = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_smithy_types::Blob;
    #[tokio::test]
    async fn test_mock_kinesis_client() -> Result<(), KinesisClientError> {
        let client = MockKinesisClient::new();

        // Test list_shards
        client
            .mock_list_shards(Ok(vec![Shard::builder()
                .shard_id("shard-1")
                .build()
                .expect("Failed to build test shard")]))
            .await;

        let shards = client.list_shards("test-stream").await?;
        assert_eq!(shards.len(), 1);
        assert_eq!(shards[0].shard_id(), "shard-1");

        // Test get_iterator
        client
            .mock_get_iterator(Ok("test-iterator".to_string()))
            .await;
        let iterator = client
            .get_shard_iterator(
                "test-stream",
                "shard-1",
                ShardIteratorType::TrimHorizon,
                None,
                None,
            )
            .await?;
        assert_eq!(iterator, "test-iterator");

        // Test get_records
        let test_record = Record::builder()
            .sequence_number("seq-1")
            .data(Blob::new("test data".as_bytes().to_vec()))
            .partition_key("test-partition-key")
            .build()
            .expect("Failed to build test record");

        client
            .mock_get_records(Ok((vec![test_record], Some("next-iterator".to_string()))))
            .await;

        let (tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
        let (records, next_iterator) = client
            .get_records("test-iterator", 100, 0, Some(3), &mut shutdown_rx)
            .await?;

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].sequence_number(), "seq-1");
        assert_eq!(records[0].partition_key(), "test-partition-key");
        assert_eq!(next_iterator, Some("next-iterator".to_string()));

        // Test expired iterator error
        client.mock_expired_iterator().await;
        let result = client
            .get_records("test-iterator", 100, 0, Some(1), &mut shutdown_rx)
            .await;

        assert!(
            matches!(result, Err(KinesisClientError::ExpiredIterator)),
            "Expected ExpiredIterator error, got {:?}",
            result
        );

        drop(tx);
        Ok(())
    }

    // Add separate test for iterator expiration
    #[tokio::test]
    async fn test_iterator_expiration() -> Result<(), KinesisClientError> {
        let client = MockKinesisClient::new();

        client.mock_expired_iterator().await;

        let (tx, mut rx) = tokio::sync::watch::channel(false);
        let result = client
            .get_records("test-iterator", 100, 0, Some(1), &mut rx)
            .await;

        assert!(
            matches!(result, Err(KinesisClientError::ExpiredIterator)),
            "Expected ExpiredIterator error, got {:?}",
            result
        );

        drop(tx);
        Ok(())
    }

    #[tokio::test]
    async fn test_mock_record_processor() -> Result<()> {
        let processor = MockRecordProcessor::new();

        // Configure a test failure
        processor
            .set_failure_sequence("test-seq".to_string(), "hard".to_string(), 1)
            .await;

        let record = Record::builder()
            .sequence_number("test-seq")
            .data(Blob::new(vec![]))
            .partition_key("test-partition")
            .build()
            .expect("Failed to build record");

        // Create metadata for the test
        let metadata = RecordMetadata::new(
            &record,
            "test-shard-1".to_string(),
            1, // first attempt
        );

        // Test failure handling
        let result = processor.process_record(&record, metadata).await;
        assert!(matches!(result, Err(ProcessingError::HardFailure(_))));
        assert_eq!(processor.get_failure_attempts("test-seq").await, 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_mock_checkpoint_store() -> Result<()> {
        let store = MockCheckpointStore::new();

        // Test normal operation
        store.save_checkpoint("shard-1", "seq-1").await?;
        assert_eq!(store.get_save_count().await, 1);

        // Test mocked responses
        store
            .mock_save_checkpoint(Err(anyhow::anyhow!("Simulated failure")))
            .await;
        assert!(store.save_checkpoint("shard-1", "seq-2").await.is_err());

        Ok(())
    }

    #[tokio::test]
    async fn test_mock_backoff() -> Result<()> {
        let delays = vec![
            Duration::from_millis(100),
            Duration::from_millis(200),
            Duration::from_millis(300),
        ];

        let mut backoff = MockBackoff::new(delays.clone());

        // Test next_delay returns expected values
        for expected in &delays {
            let delay = backoff.next_delay(0);
            assert_eq!(&delay, expected);
        }

        // Test call count
        assert_eq!(backoff.get_call_count(), delays.len());

        // Test reset
        backoff.reset();
        assert_eq!(backoff.get_call_count(), 0);

        Ok(())
    }
}
