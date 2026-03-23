use crate::config::KafkaConfig;
use crate::task_parser::Component;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::ClientConfig;
use serde::Serialize;
use std::time::Duration;
use tracing::{info, error};

#[derive(Serialize)]
pub struct ComponentMessage {
    pub task_id: String,
    pub component_index: usize,
    pub component_title: String,
    pub agent: Option<String>,
    pub content: String,
    pub plan_file_path: Option<String>,
    pub plan_checksum: Option<String>,
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

    //publish a component during ingestion (bulk — all pending components)
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
            plan_file_path: None,
            plan_checksum: None,
        };
        self.send_message(task_id, component.index, &msg).await
    }

    //publish a component dispatch with plan context (used by scheduler)
    pub async fn send_dispatch(
        &self,
        task_id: &str,
        component: &Component,
        plan_file_path: &str,
        plan_checksum: &str,
    ) -> anyhow::Result<()> {
        let msg = ComponentMessage {
            task_id: task_id.to_string(),
            component_index: component.index,
            component_title: component.title.clone(),
            agent: component.agent.clone(),
            content: component.content.clone(),
            plan_file_path: Some(plan_file_path.to_string()),
            plan_checksum: Some(plan_checksum.to_string()),
        };
        self.send_message(task_id, component.index, &msg).await
    }

    async fn send_message(
        &self,
        task_id: &str,
        component_index: usize,
        msg: &ComponentMessage,
    ) -> anyhow::Result<()> {
        let payload = serde_json::to_string(msg)?;
        let key = format!("{}:{}", task_id, component_index);

        let record = FutureRecord::to(&self.topic)
            .key(&key)
            .payload(&payload);

        match self.producer.send(record, Duration::from_secs(5)).await {
            Ok((partition, offset)) => {
                info!(
                    task_id = %task_id,
                    component = component_index,
                    partition = partition,
                    offset = offset,
                    "component enqueued to kafka"
                );
                Ok(())
            }
            Err((e, _)) => {
                error!(
                    task_id = %task_id,
                    component = component_index,
                    error = %e,
                    "failed to send to kafka"
                );
                Err(anyhow::anyhow!("kafka send failed: {}", e))
            }
        }
    }
}
