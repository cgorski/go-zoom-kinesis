use anyhow::Result;
use async_trait::async_trait;
use aws_sdk_kinesis::{
    types::{Record, Shard, ShardIteratorType},
    Client,
};
use chrono::{DateTime, Utc};
use std::time::{Duration, SystemTime};
use tracing::warn;

#[async_trait]
pub trait KinesisClientTrait: Send + Sync {
    async fn list_shards(&self, stream_name: &str) -> Result<Vec<Shard>>;

    async fn get_shard_iterator(
        &self,
        stream_name: &str,
        shard_id: &str,
        iterator_type: ShardIteratorType,
        sequence_number: Option<&str>,
        timestamp: Option<&DateTime<Utc>>,
    ) -> Result<String>;

    async fn get_records(
        &self,
        iterator: &str,
        limit: i32,
        retry_count: u32,
        max_retries: Option<u32>,
        shutdown: &mut tokio::sync::watch::Receiver<bool>,
    ) -> Result<(Vec<Record>, Option<String>)>;
}

#[cfg(feature = "test-utils")]
pub trait KinesisClientTestExt: KinesisClientTrait {
    fn mock_list_shards(
        &self,
        _response: Result<Vec<Shard>>,
    ) -> impl std::future::Future<Output = ()> + Send {
        async {}
    }

    fn mock_get_iterator(
        &self,
        _response: Result<String>,
    ) -> impl std::future::Future<Output = ()> + Send {
        async {}
    }

    fn mock_get_records(
        &self,
        _response: Result<(Vec<Record>, Option<String>)>,
    ) -> impl std::future::Future<Output = ()> + Send {
        async {}
    }

    fn get_iterator_request_count(&self) -> impl std::future::Future<Output = usize> + Send {
        async { 0 }
    }
}

#[cfg(feature = "test-utils")]
impl<T: KinesisClientTrait> KinesisClientTestExt for T {}

#[async_trait]
impl KinesisClientTrait for Client {
    async fn list_shards(&self, stream_name: &str) -> Result<Vec<Shard>> {
        let response = self.list_shards().stream_name(stream_name).send().await?;
        Ok(response.shards.unwrap_or_default())
    }

    async fn get_shard_iterator(
        &self,
        stream_name: &str,
        shard_id: &str,
        iterator_type: ShardIteratorType,
        sequence_number: Option<&str>,
        timestamp: Option<&DateTime<Utc>>,
    ) -> Result<String> {
        let mut req = self
            .get_shard_iterator()
            .stream_name(stream_name)
            .shard_id(shard_id)
            .shard_iterator_type(iterator_type);

        if let Some(seq) = sequence_number {
            req = req.starting_sequence_number(seq);
        }

        if let Some(ts) = timestamp {
            let ts: chrono::DateTime<Utc> = *ts;
            let system_time: SystemTime = ts.into();
            let smithy_dt = aws_smithy_types::DateTime::from(system_time);
            req = req.timestamp(smithy_dt);
        }

        let response = req.send().await?;
        Ok(response.shard_iterator.unwrap_or_default())
    }
    async fn get_records(
        &self,
        iterator: &str,
        limit: i32,
        retry_count: u32,
        max_retries: Option<u32>,
        shutdown: &mut tokio::sync::watch::Receiver<bool>,
    ) -> Result<(Vec<Record>, Option<String>)> {
        let mut current_retry = retry_count;
        loop {
            if *shutdown.borrow() {
                return Err(anyhow::anyhow!("Shutdown requested"));
            }

            match self
                .get_records()
                .shard_iterator(iterator)
                .limit(limit)
                .send()
                .await
            {
                Ok(response) => {
                    return Ok((
                        response.records().to_vec(),
                        response.next_shard_iterator().map(String::from),
                    ));
                }
                Err(e) => {
                    current_retry += 1;
                    if current_retry > max_retries.unwrap_or(3) {
                        return Err(e.into());
                    }

                    warn!(
                        error = %e,
                        attempt = current_retry,
                        "Temporary failure getting records, will retry"
                    );

                    let delay = Duration::from_millis(100 * (2_u64.pow(current_retry - 1)));
                    tokio::time::sleep(delay).await;
                }
            }
        }
    }
}
