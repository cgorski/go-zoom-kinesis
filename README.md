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
    processor::RecordMetadata, processor::InitialPosition,
    store::InMemoryCheckpointStore,
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
    type Item = ();

    async fn process_record<'a>(
        &self,
        record: &'a Record,
        metadata: RecordMetadata<'a>,
    ) -> Result<Option<Self::Item>, ProcessingError> {
        println!("Processing record: {:?}", record);
        Ok(None)
    }
}

#[tokio::main]
async fn main() -> Result<(), ProcessorError> {
    let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    let client = Client::new(&config);

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

    let processor = MyProcessor;
    let store = InMemoryCheckpointStore::new();

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

## Contributing 😪

Contributions are welcome! Please feel free to submit a Pull Request.

## License 📒

This project is licensed under the MIT License - see the [LICENSE](LICENSE-MIT) file for details.

## Support 🔠

If you have any questions or run into issues, please [open an issue](https://github.com/cgorski/go-zoom-kinesis/issues/new) on GitHub.