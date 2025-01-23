use crate::monitoring::IteratorEventType;
use crate::monitoring::ShardEventType;
use crate::monitoring::ProcessingEventType;
use crate::monitoring::MonitoringConfig;
use crate::ProcessorError;
use crate::client::KinesisClientError;
use crate::processor::{InitialPosition, ProcessorConfig, KinesisProcessor};
use crate::test::mocks::{MockKinesisClient, MockRecordProcessor};
use crate::test::TestUtils;
use crate::InMemoryCheckpointStore;
use crate::error::Result;
use chrono::{TimeZone, Utc};
use std::time::Duration;
use crate::store::CheckpointStore;

async fn setup_basic_test_context(
    initial_position: InitialPosition,
) -> (MockKinesisClient, MockRecordProcessor, InMemoryCheckpointStore, ProcessorConfig) {
    let config = ProcessorConfig {
        stream_name: "test-stream".to_string(),
        batch_size: 100,
        api_timeout: Duration::from_secs(1),
        processing_timeout: Duration::from_secs(1),
        initial_position,
        ..Default::default()
    };

    let client = MockKinesisClient::new();
    let processor = MockRecordProcessor::new();
    let store = InMemoryCheckpointStore::new();

    (client, processor, store, config)
}

#[tokio::test]
async fn test_trim_horizon_position() -> Result<()> {
    let (client, processor, store, config) = setup_basic_test_context(InitialPosition::TrimHorizon).await;

    client.mock_list_shards(Ok(vec![TestUtils::create_test_shard("shard-1")])).await;
    client.mock_get_iterator(Ok("test-iterator".to_string())).await;
    client.mock_get_records(Ok((
        vec![TestUtils::create_test_record("seq-1", b"test data")],
        None,
    ))).await;

    let (tx, rx) = tokio::sync::watch::channel(false);
    let (processor_instance, _) = KinesisProcessor::new(config, processor.clone(), client.clone(), store);

    let handle = tokio::spawn(async move {
        processor_instance.run(rx).await
    });

    // Give it some time to process
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify the iterator type used
    assert_eq!(
        client.get_iterator_request_count().await,
        1,
        "Should make exactly one iterator request"
    );

    // Shutdown the processor
    tx.send(true)?;
    handle.await??;

    Ok(())
}

#[tokio::test]
async fn test_at_sequence_number_position() -> Result<()> {
    let initial_sequence = "sequence-100".to_string();
    let (client, processor, store, config) = setup_basic_test_context(
        InitialPosition::AtSequenceNumber(initial_sequence.clone())
    ).await;

    client.mock_list_shards(Ok(vec![TestUtils::create_test_shard("shard-1")])).await;
    client.mock_get_iterator(Ok("test-iterator".to_string())).await;
    client.mock_get_records(Ok((
        vec![TestUtils::create_test_record("seq-101", b"test data")],
        None,
    ))).await;

    let (tx, rx) = tokio::sync::watch::channel(false);
    let (processor_instance, _) = KinesisProcessor::new(config, processor.clone(), client.clone(), store);

    let handle = tokio::spawn(async move {
        processor_instance.run(rx).await
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify we got the expected sequence
    let processed_records = processor.get_processed_records().await;
    assert!(!processed_records.is_empty(), "Should have processed at least one record");
    assert_eq!(
        processed_records[0].sequence_number(),
        "seq-101",
        "Should process records after the specified sequence"
    );

    tx.send(true)?;
    handle.await??;

    Ok(())
}

#[tokio::test]
async fn test_config_validation() {
    // Test empty sequence number
    let config = ProcessorConfig {
        initial_position: InitialPosition::AtSequenceNumber("".to_string()),
        ..Default::default()
    };
    assert!(config.validate().is_err(), "Should reject empty sequence number");

    // Test invalid timestamp
    let invalid_time = Utc.timestamp_opt(-1, 0).unwrap();
    let config = ProcessorConfig {
        initial_position: InitialPosition::AtTimestamp(invalid_time),
        ..Default::default()
    };
    assert!(config.validate().is_err(), "Should reject invalid timestamp");

    // Test valid cases
    let valid_cases = vec![
        InitialPosition::TrimHorizon,
        InitialPosition::Latest,
        InitialPosition::AtSequenceNumber("valid-sequence".to_string()),
        InitialPosition::AtTimestamp(Utc::now()),
    ];

    for position in valid_cases {
        let config = ProcessorConfig {
            initial_position: position,
            ..Default::default()
        };
        assert!(config.validate().is_ok(), "Should accept valid position");
    }
}

#[tokio::test]
async fn test_latest_position() -> Result<()> {
    let (client, processor, store, config) = setup_basic_test_context(InitialPosition::Latest).await;

    // Setup mock responses
    client.mock_list_shards(Ok(vec![TestUtils::create_test_shard("shard-1")])).await;
    client.mock_get_iterator(Ok("test-iterator-latest".to_string())).await;

    // Mock some "new" records that would appear after Latest position
    let test_records = vec![
        TestUtils::create_test_record("new-seq-1", b"new data 1"),
        TestUtils::create_test_record("new-seq-2", b"new data 2"),
    ];
    client.mock_get_records(Ok((test_records.clone(), None))).await;

    let (tx, rx) = tokio::sync::watch::channel(false);
    let (processor_instance, _) = KinesisProcessor::new(config, processor.clone(), client.clone(), store);

    let handle = tokio::spawn(async move {
        processor_instance.run(rx).await
    });

    // Wait for processing
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify processed records
    let processed_records = processor.get_processed_records().await;
    assert_eq!(
        processed_records.len(),
        test_records.len(),
        "Should process all new records"
    );

    // Verify we're processing the expected sequences
    for (i, record) in processed_records.iter().enumerate() {
        assert_eq!(
            record.sequence_number(),
            format!("new-seq-{}", i + 1),
            "Should process records in order"
        );
    }

    tx.send(true)?;
    handle.await??;

    Ok(())
}

#[tokio::test]
async fn test_at_timestamp_position() -> Result<()> {
    let timestamp = Utc::now();
    let (client, processor, store, config) = setup_basic_test_context(
        InitialPosition::AtTimestamp(timestamp)
    ).await;

    client.mock_list_shards(Ok(vec![TestUtils::create_test_shard("shard-1")])).await;
    client.mock_get_iterator(Ok("test-iterator-timestamp".to_string())).await;

    // Mock records that would appear after the timestamp
    let test_records = vec![
        TestUtils::create_test_record("timestamp-seq-1", b"timestamp data 1"),
        TestUtils::create_test_record("timestamp-seq-2", b"timestamp data 2"),
    ];
    client.mock_get_records(Ok((test_records.clone(), None))).await;

    let (tx, rx) = tokio::sync::watch::channel(false);
    let (processor_instance, _) = KinesisProcessor::new(config, processor.clone(), client.clone(), store);

    let handle = tokio::spawn(async move {
        processor_instance.run(rx).await
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    let processed_records = processor.get_processed_records().await;
    assert_eq!(
        processed_records.len(),
        test_records.len(),
        "Should process all records after timestamp"
    );

    tx.send(true)?;
    handle.await??;

    Ok(())
}
#[tokio::test]
async fn test_checkpoint_preference_override() -> Result<()> {
    // Test with checkpoint preference enabled
    let config_with_preference = ProcessorConfig {
        stream_name: "test-stream".to_string(),
        initial_position: InitialPosition::Latest,
        prefer_stored_checkpoint: true,
        ..Default::default()
    };

    let client = MockKinesisClient::new();
    let processor = MockRecordProcessor::new();
    let store = InMemoryCheckpointStore::new();

    // Save a checkpoint
    let checkpoint_sequence = "checkpoint-seq-100";
    store.save_checkpoint("shard-1", checkpoint_sequence).await?;

    // Setup mock responses
    client.mock_list_shards(Ok(vec![TestUtils::create_test_shard("shard-1")])).await;
    client.mock_get_iterator(Ok("test-iterator-checkpoint".to_string())).await;

    // Mock records that would appear after the checkpoint
    let test_records = vec![
        TestUtils::create_test_record("checkpoint-seq-101", b"after checkpoint 1"),
        TestUtils::create_test_record("checkpoint-seq-102", b"after checkpoint 2"),
    ];
    client.mock_get_records(Ok((test_records.clone(), None))).await;

    let (tx, rx) = tokio::sync::watch::channel(false);
    let (processor_instance, _) = KinesisProcessor::new(
        config_with_preference,
        processor.clone(),
        client.clone(),
        store
    );

    let handle = tokio::spawn(async move {
        processor_instance.run(rx).await
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify we're processing records after the checkpoint
    let processed_records = processor.get_processed_records().await;
    assert!(!processed_records.is_empty(), "Should process records");

    // Verify the sequence numbers are the ones we expect
    assert_eq!(
        processed_records[0].sequence_number(),
        "checkpoint-seq-101",
        "First record should be checkpoint-seq-101"
    );
    assert_eq!(
        processed_records[1].sequence_number(),
        "checkpoint-seq-102",
        "Second record should be checkpoint-seq-102"
    );

    // Test with checkpoint preference disabled
    let config_without_preference = ProcessorConfig {
        stream_name: "test-stream".to_string(),
        initial_position: InitialPosition::Latest,
        prefer_stored_checkpoint: false,
        ..Default::default()
    };

    let client2 = MockKinesisClient::new();
    let processor2 = MockRecordProcessor::new();
    let store2 = InMemoryCheckpointStore::new();

    // Save the same checkpoint
    store2.save_checkpoint("shard-1", checkpoint_sequence).await?;

    client2.mock_list_shards(Ok(vec![TestUtils::create_test_shard("shard-1")])).await;
    client2.mock_get_iterator(Ok("test-iterator-latest".to_string())).await;

    // These records simulate what we'd get from Latest position
    let latest_records = vec![
        TestUtils::create_test_record("latest-seq-1", b"latest data")
    ];
    client2.mock_get_records(Ok((latest_records.clone(), None))).await;

    let (tx2, rx2) = tokio::sync::watch::channel(false);
    let (processor_instance2, _) = KinesisProcessor::new(
        config_without_preference,
        processor2.clone(),
        client2.clone(),
        store2
    );

    let handle2 = tokio::spawn(async move {
        processor_instance2.run(rx2).await
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify we're using Latest position instead of checkpoint
    let processed_records2 = processor2.get_processed_records().await;
    assert!(!processed_records2.is_empty(), "Should process records");
    assert_eq!(
        processed_records2[0].sequence_number(),
        "latest-seq-1",
        "Should use Latest position instead of checkpoint"
    );

    tx.send(true)?;
    tx2.send(true)?;
    handle.await??;
    handle2.await??;

    Ok(())
}
#[tokio::test]
async fn test_initial_position_error_handling() -> Result<()> {
    // Test invalid configurations directly
    let test_cases = vec![
        (
            "invalid_sequence",
            InitialPosition::AtSequenceNumber("".to_string()),
            "AtSequenceNumber position requires a non-empty sequence number",
        ),
        (
            "invalid_timestamp",
            InitialPosition::AtTimestamp(Utc.timestamp_opt(-1, 0).unwrap()),
            "AtTimestamp position requires a valid timestamp",
        ),
    ];

    for (case_name, position, expected_error) in test_cases {
        let config = ProcessorConfig {
            stream_name: "test-stream".to_string(),
            initial_position: position,
            ..Default::default()
        };

        let validation_result = config.validate();
        assert!(validation_result.is_err(),
                "Case '{}' should fail validation", case_name);

        let error = validation_result.unwrap_err();
        let error_str = error.to_string();
        assert!(error_str.contains(expected_error),
                "Case '{}' should fail with error containing '{}', got '{}'",
                case_name, expected_error, error_str);
    }

    // Test iterator acquisition failures
    let config = ProcessorConfig {
        stream_name: "test-stream".to_string(),
        initial_position: InitialPosition::TrimHorizon,
        ..Default::default()
    };

    let client = MockKinesisClient::new();
    let processor = MockRecordProcessor::new();
    let store = InMemoryCheckpointStore::new();

    client.mock_list_shards(Ok(vec![TestUtils::create_test_shard("shard-1")])).await;
    client.mock_get_iterator(Err(KinesisClientError::AccessDenied)).await;

    let (tx, rx) = tokio::sync::watch::channel(false);
    let (processor_instance, _) = KinesisProcessor::new(config, processor.clone(), client.clone(), store);

    let result = processor_instance.run(rx).await;
    assert!(result.is_err(), "Should fail when iterator acquisition fails");
    assert!(matches!(result, Err(ProcessorError::GetIteratorFailed(_))),
            "Should return GetIteratorFailed error");

    Ok(())
}

#[tokio::test]
async fn test_initial_position_validation() -> Result<()> {
    // Test valid configurations
    let valid_cases = vec![
        (
            "trim_horizon",
            InitialPosition::TrimHorizon,
        ),
        (
            "latest",
            InitialPosition::Latest,
        ),
        (
            "valid_sequence",
            InitialPosition::AtSequenceNumber("valid-sequence".to_string()),
        ),
        (
            "valid_timestamp",
            InitialPosition::AtTimestamp(Utc::now()),
        ),
    ];

    for (case_name, position) in valid_cases {
        let config = ProcessorConfig {
            stream_name: "test-stream".to_string(),
            initial_position: position,
            ..Default::default()
        };

        assert!(config.validate().is_ok(),
                "Valid case '{}' should pass validation", case_name);
    }

    Ok(())
}

#[tokio::test]
async fn test_initial_position_behavior() -> Result<()> {
    let test_cases = vec![
        (
            "trim_horizon",
            InitialPosition::TrimHorizon,
            "test-seq-1",
        ),
        (
            "at_sequence",
            InitialPosition::AtSequenceNumber("test-seq-100".to_string()),
            "test-seq-101",
        ),
    ];

    for (case_name, position, expected_first_sequence) in test_cases {
        let config = ProcessorConfig {
            stream_name: "test-stream".to_string(),
            initial_position: position,
            ..Default::default()
        };

        let client = MockKinesisClient::new();
        let processor = MockRecordProcessor::new();
        let store = InMemoryCheckpointStore::new();

        client.mock_list_shards(Ok(vec![TestUtils::create_test_shard("shard-1")])).await;
        client.mock_get_iterator(Ok("test-iterator".to_string())).await;
        client.mock_get_records(Ok((
            vec![TestUtils::create_test_record(expected_first_sequence, b"test data")],
            None,
        ))).await;

        let (tx, rx) = tokio::sync::watch::channel(false);
        let (processor_instance, _) = KinesisProcessor::new(
            config,
            processor.clone(),
            client.clone(),
            store,
        );

        let handle = tokio::spawn(async move {
            processor_instance.run(rx).await
        });

        tokio::time::sleep(Duration::from_millis(100)).await;

        let processed_records = processor.get_processed_records().await;
        assert!(!processed_records.is_empty(),
                "Case '{}' should process records", case_name);
        assert_eq!(
            processed_records[0].sequence_number(),
            expected_first_sequence,
            "Case '{}' should process expected sequence", case_name
        );

        tx.send(true)?;
        handle.await??;
    }

    Ok(())
}

