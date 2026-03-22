use crate::config::KafkaConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::ClientConfig;
use rdkafka::message::Message;
use serde::Deserialize;
use tracing::{info, warn};
use tokio_stream::StreamExt;

#[derive(Debug, Deserialize)]
pub struct ResultMessage {
    pub task_id: String,
    pub component_index: usize,
    pub status: String,
    pub output: Option<String>,
}

pub struct ResultConsumer {
    consumer: StreamConsumer,
}

impl ResultConsumer {
    pub fn new(config: &KafkaConfig) -> anyhow::Result<Self> {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", &config.brokers)
            .set("group.id", &config.group_id)
            .set("enable.auto.commit", "true")
            .set("auto.offset.reset", "earliest")
            .create()?;

        consumer.subscribe(&[&config.result_topic])?;

        Ok(Self { consumer })
    }

    pub async fn poll_results(&self) -> anyhow::Result<Option<ResultMessage>> {
        let mut stream = self.consumer.stream();

        match tokio::time::timeout(
            std::time::Duration::from_secs(5),
            stream.next(),
        ).await {
            Ok(Some(Ok(msg))) => {
                if let Some(payload) = msg.payload() {
                    let result: ResultMessage = serde_json::from_slice(payload)?;
                    info!(
                        task_id = %result.task_id,
                        component = result.component_index,
                        status = %result.status,
                        "received result from kafka"
                    );
                    Ok(Some(result))
                } else {
                    Ok(None)
                }
            }
            Ok(Some(Err(e))) => {
                warn!(error = %e, "kafka consumer error");
                Ok(None)
            }
            Ok(None) => Ok(None),
            Err(_) => {
                //timeout - no messages available
                Ok(None)
            }
        }
    }
}
