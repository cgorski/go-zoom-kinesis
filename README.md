# go-zoom-kinesis 🐊

[![CI](https://github.com/cgorski/go-zoom-kinesis/actions/workflows/ci.yml/badge.svg)](https://github.com/cgorski/go-zoom-kinesis/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/cgorski/go-zoom-kinesis/branch/main/graph/badge.svg)](https://codecov.io/gh/cgorski/go-zoom-kinesis)
[![Crates.io](https://img.shields.io/crates/v/go-zoom-kinesis.svg)](https://crates.io/crates/go-zoom-kinesis)
[![Documentation](https://img.shields.io/badge/docs-github.io-blue)](https://cgorski.github.io/go-zoom-kinesis/go_zoom_kinesis/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A robust, production-ready AWS Kinesis stream processor with checkpointing and retry capabilities. Built with reliability and performance in mind.

## Features 🚀

- ✨ Automatic checkpointing with multiple storage backends
- 🔄 Configurable retry logic with exponential backoff
- 🛼️ Comprehensive error handling
- 😊 Multiple shard processing
- 🕥 DynamoDB checkpoint storage support
- 📘 Detailed tracing and monitoring
- 📦 Graceful shutdown handling
- 🢪 Production-ready with extensive test coverage
- 🎧 Configurable stream position initialization
- 🔄 Smart checkpoint recovery with fallback options

### Basic Usage 📓

```rust
use go_zoom_kinesis::{
    KinesisProcessor, ProcessorConfig, RecordProcessor,
    processor::RecordMetadata,
    store::InMemoryCheckpointStore,
    InitialPosition,
    monitoring::MonitoringConfig,
    error::{ProcessorError, ProcessingError},
};
use aws_sdk_kinesis::{Client, types::Record};
use std::time::Duration;
use async_trait::async_trait;

#[derive(Clone)]
struct MyProcessor;

#[async_trait]
impl RecordProcessor for MyProcessor {
    async fn process_record<'a>(
        &self,
        record: &'a Record,
        metadata: RecordMetadata<'a>,
    ) -> Result<(), ProcessingError> {
        println!("Processing record: {:?}", record);
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), ProcessorError> {
    // Configure AWS client
    let config = aws_config::load_from_env().await;
    let client = Client::new(&config);

    // Create processor configuration
    let config = ProcessorConfig {
        stream_name: "my-stream".to_string(),
        batch_size: 100,
        api_timeout: Duration::from_secs(30),
        processing_timeout: Duration::from_secs(300),
        max_retries: Some(3),
        shard_refresh_interval: Duration::from_secs(60),
        initial_position: InitialPosition::TrimHorizon,
        prefer_stored_checkpoint: true,
        monitoring: MonitoringConfig {
            enabled: true,
            ..Default::default()
        },
        ..Default::default()
    };

    // Initialize processor components
    let processor = MyProcessor;
    let store = InMemoryCheckpointStore::new();

    // Create and run processor
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let (processor, _monitoring_rx) = KinesisProcessor::new(
        config,
        processor,
        client,
        store,
    );

    processor.run(shutdown_rx).await
}
```

### Stream Position Configuration 🎃

The processor supports flexible stream position initialization:

```rust
use go_zoom_kinesis::{ProcessorConfig, InitialPosition};
use chrono::{DateTime, Utc};

// Start from oldest available record
let config = ProcessorConfig {
initial_position: InitialPosition::TrimHorizon,
prefer_stored_checkpoint: true,  // Will check checkpoint store first
..Default::default()
};

// Start from tip of the stream
let config = ProcessorConfig {
initial_position: InitialPosition::Latest,
..Default::default()
};

// Start from specific sequence number
let config = ProcessorConfig {
initial_position: InitialPosition::AtSequenceNumber(
"49579292999999999999999999".to_string()
),
..Default::default()
};

// Start from specific timestamp
let config = ProcessorConfig {
initial_position: InitialPosition::AtTimestamp(
Utc::now() - chrono::Duration::hours(1)
),
..Default::default()
};
```

### DynamoDB Checkpoint Store Example ###

```rust

use go_zoom_kinesis::store::DynamoDbCheckpointStore;


async fn example() -> anyhow::Result<()> {
    let config = aws_config::load_from_env().await;
    let dynamo_client = aws_sdk_dynamodb::Client::new(&config);
    let checkpoint_store = DynamoDbCheckpointStore::new(
        dynamo_client,
        "checkpoints-table".to_string(),
        "my-app-".to_string(),
    );
    Ok(())
}
```

### Custom Error Handling 📨

```rust
use go_zoom_kinesis::{RecordProcessor, error::ProcessingError};
use go_zoom_kinesis::processor::RecordMetadata;
use aws_sdk_kinesis::types::Record;
use async_trait::async_trait;
use anyhow::Result;

struct MyProcessor;

#[async_trait]
impl RecordProcessor for MyProcessor {
    async fn process_record<'a>(
        &self,
        record: &'a Record,
        metadata: RecordMetadata<'a>,
    ) -> Result<(), ProcessingError> {
        match process_data(record).await {
            Ok(_) => Ok(()),
            Err(e) => {
                // Custom error handling with metadata context
                tracing::error!(
                    error = %e,
                    shard_id = %metadata.shard_id(),
                    attempt = %metadata.attempt_number(),
                    "Failed to process record"
                );
                Err(ProcessingError::soft(e)) // Will be retried
            }
        }
    }
}

// Example processing function
async fn process_data(_record: &Record) -> Result<()> {
    Ok(())
}
```

### Configuring Retries 🔄

```rust
use go_zoom_kinesis::{ProcessorConfig, ExponentialBackoff, monitoring::MonitoringConfig};
use std::time::Duration;

let config = ProcessorConfig {
max_retries: Some(5),
monitoring: MonitoringConfig {
include_retry_details: true,
..Default::default()
},
..Default::default()
};

let backoff = ExponentialBackoff::builder()
.initial_delay(Duration::from_millis(100))
.max_delay(Duration::from_secs(30))
.multiplier(2.0)
.jitter_factor(0.1)
.build();
```

### Checkpoint Recovery Behavior 🔄

The processor provides flexible checkpoint recovery through the `prefer_stored_checkpoint` configuration:

- When `true` (default): Attempts to resume from the last checkpoint if available. Falls back to `initial_position` if no checkpoint exists.
- When `false`: Ignores existing checkpoints and starts from the configured `initial_position`.

This enables scenarios like:
- Development testing (ignore checkpoints, start from beginning)
- Production deployment (resume from checkpoint, fall back to latest)
- Disaster recovery (force start from specific timestamp)

### Common Configuration Scenarios

```rust
use go_zoom_kinesis::{ProcessorConfig, InitialPosition};
use chrono::{DateTime, Utc};

// Start from oldest available record
let config = ProcessorConfig {
initial_position: InitialPosition::TrimHorizon,
prefer_stored_checkpoint: true,  // Will check checkpoint store first
..Default::default()
};

// Start from tip of the stream
let config = ProcessorConfig {
initial_position: InitialPosition::Latest,
..Default::default()
};

// Start from specific sequence number
let config = ProcessorConfig {
initial_position: InitialPosition::AtSequenceNumber(
"49579292999999999999999999".to_string()
),
..Default::default()
};

// Start from specific timestamp
let config = ProcessorConfig {
initial_position: InitialPosition::AtTimestamp(
Utc::now() - chrono::Duration::hours(1)
),
..Default::default()
};
```


## Contributing 😪

Contributions are welcome! Please feel free to submit a Pull Request.

## License 📒

This project is licensed under the MIT License - see the [LICENSE](LICENSE-MIT) file for details.

## Support 🔠

If you have any questions or run into issues, please [open an issue](https://github.com/cgorski/go-zoom-kinesis/issues/new) on GitHub.