use crate::config::KafkaConfig;
use crate::task_parser::Component;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::ClientConfig;
use serde::Serialize;
use std::time::Duration;
use tracing::{info, error};

#[derive(Serialize)]
struct ComponentMessage {
    task_id: String,
    component_index: usize,
    component_title: String,
    agent: Option<String>,
    content: String,
}

pub struct TaskProducer {
    producer: FutureProducer,
    topic: String,
}

impl TaskProducer {
    pub fn new(config: &KafkaConfig) -> anyhow::Result<Self> {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", &config.brokers)
            .set("message.timeout.ms", "5000")
            .create()?;

        Ok(Self {
            producer,
            topic: config.task_topic.clone(),
        })
    }

    pub async fn send_component(
        &self,
        task_id: &str,
        component: &Component,
    ) -> anyhow::Result<()> {
        let msg = ComponentMessage {
            task_id: task_id.to_string(),
            component_index: component.index,
            component_title: component.title.clone(),
            agent: component.agent.clone(),
            content: component.content.clone(),
        };

        let payload = serde_json::to_string(&msg)?;
        let key = format!("{}:{}", task_id, component.index);

        let record = FutureRecord::to(&self.topic)
            .key(&key)
            .payload(&payload);

        match self.producer.send(record, Duration::from_secs(5)).await {
            Ok((partition, offset)) => {
                info!(
                    task_id = %task_id,
                    component = component.index,
                    partition = partition,
                    offset = offset,
                    "component enqueued to kafka"
                );
                Ok(())
            }
            Err((e, _)) => {
                error!(
                    task_id = %task_id,
                    component = component.index,
                    error = %e,
                    "failed to send to kafka"
                );
                Err(anyhow::anyhow!("kafka send failed: {}", e))
            }
        }
    }
}
