//!Integration tests for Kafka produce/consume flow.
//!Requires a running Kafka broker at localhost:9092 with
//!topics `pulsar.tasks` and `pulsar.results` pre-created.
//!
//!Run: C_INCLUDE_PATH=/tmp/stub-headers cargo test --features kafka --test kafka_integration

#[cfg(feature = "kafka")]
mod kafka_tests {
    use rdkafka::consumer::{Consumer, StreamConsumer};
    use rdkafka::producer::{FutureProducer, FutureRecord};
    use rdkafka::message::Message;
    use rdkafka::ClientConfig;
    use serde::{Deserialize, Serialize};
    use std::time::Duration;
    use tokio_stream::StreamExt;

    const BROKERS: &str = "localhost:9092";
    const TASKS_TOPIC: &str = "pulsar.tasks";
    const RESULTS_TOPIC: &str = "pulsar.results";

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct ComponentMessage {
        task_id: String,
        component_index: usize,
        component_title: String,
        agent: Option<String>,
        content: String,
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct ResultMessage {
        task_id: String,
        component_index: usize,
        status: String,
        output: Option<String>,
    }

    fn create_producer() -> FutureProducer {
        ClientConfig::new()
            .set("bootstrap.servers", BROKERS)
            .set("message.timeout.ms", "5000")
            .create()
            .expect("failed to create producer")
    }

    fn create_consumer(group_id: &str, topic: &str) -> StreamConsumer {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", BROKERS)
            .set("group.id", group_id)
            .set("enable.auto.commit", "true")
            .set("auto.offset.reset", "earliest")
            .create()
            .expect("failed to create consumer");
        consumer.subscribe(&[topic]).expect("failed to subscribe");
        consumer
    }

    #[tokio::test]
    async fn test_produce_component_to_tasks_topic() {
        let producer = create_producer();

        let msg = ComponentMessage {
            task_id: "task_test_kafka_001".to_string(),
            component_index: 1,
            component_title: "Database Schema".to_string(),
            agent: Some("backend-developer".to_string()),
            content: "Create user and session tables".to_string(),
        };

        let payload = serde_json::to_string(&msg).unwrap();
        let key = format!("{}:{}", msg.task_id, msg.component_index);

        let result = producer
            .send(
                FutureRecord::to(TASKS_TOPIC)
                    .key(&key)
                    .payload(&payload),
                Duration::from_secs(5),
            )
            .await;

        match result {
            Ok((partition, offset)) => {
                println!(
                    "produced to partition={}, offset={}",
                    partition, offset
                );
                assert!(partition >= 0);
                assert!(offset >= 0);
            }
            Err((e, _)) => panic!("failed to produce: {}", e),
        }
    }

    #[tokio::test]
    async fn test_produce_and_consume_results_topic() {
        let producer = create_producer();
        let unique_id = format!("task_consume_test_{}", uuid::Uuid::new_v4().simple());

        //produce first
        let result_msg = ResultMessage {
            task_id: unique_id.clone(),
            component_index: 1,
            status: "COMPLETED".to_string(),
            output: Some("Created 5 tables successfully".to_string()),
        };

        let payload = serde_json::to_string(&result_msg).unwrap();
        let key = format!("{}:{}", result_msg.task_id, result_msg.component_index);

        producer
            .send(
                FutureRecord::to(RESULTS_TOPIC)
                    .key(&key)
                    .payload(&payload),
                Duration::from_secs(5),
            )
            .await
            .expect("failed to produce result message");

        //then consume with earliest offset and unique group
        let group_id = format!("test-consumer-{}", uuid::Uuid::new_v4());
        let consumer = create_consumer(&group_id, RESULTS_TOPIC);

        //scan messages looking for our unique_id
        let mut stream = consumer.stream();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);

        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                panic!("timed out waiting for message with task_id={}", unique_id);
            }

            match tokio::time::timeout(remaining, stream.next()).await {
                Ok(Some(Ok(msg))) => {
                    if let Some(payload_bytes) = msg.payload() {
                        if let Ok(received) = serde_json::from_slice::<ResultMessage>(payload_bytes) {
                            if received.task_id == unique_id {
                                assert_eq!(received.component_index, 1);
                                assert_eq!(received.status, "COMPLETED");
                                assert_eq!(
                                    received.output,
                                    Some("Created 5 tables successfully".to_string())
                                );
                                println!("consumed result: {:?}", received);
                                return;
                            }
                        }
                    }
                }
                Ok(Some(Err(e))) => panic!("consumer error: {}", e),
                Ok(None) => panic!("consumer stream ended unexpectedly"),
                Err(_) => panic!("timed out waiting for message with task_id={}", unique_id),
            }
        }
    }

    #[tokio::test]
    async fn test_end_to_end_task_flow() {
        let producer = create_producer();
        let unique_id = format!("task_e2e_{}", uuid::Uuid::new_v4().simple());

        //step 1: produce a component to pulsar.tasks (simulating scheduler dispatch)
        let component = ComponentMessage {
            task_id: unique_id.clone(),
            component_index: 1,
            component_title: "Build API".to_string(),
            agent: Some("backend-developer".to_string()),
            content: "Implement REST endpoints for user CRUD".to_string(),
        };

        let comp_payload = serde_json::to_string(&component).unwrap();
        let comp_key = format!("{}:{}", component.task_id, component.component_index);

        producer
            .send(
                FutureRecord::to(TASKS_TOPIC)
                    .key(&comp_key)
                    .payload(&comp_payload),
                Duration::from_secs(5),
            )
            .await
            .expect("failed to send component");

        //step 2: produce a result to pulsar.results (simulating agent completion)
        let result = ResultMessage {
            task_id: unique_id.clone(),
            component_index: 1,
            status: "COMPLETED".to_string(),
            output: Some("Implemented 4 REST endpoints: GET/POST/PUT/DELETE /api/users".to_string()),
        };

        let res_payload = serde_json::to_string(&result).unwrap();
        let res_key = format!("{}:{}", result.task_id, result.component_index);

        producer
            .send(
                FutureRecord::to(RESULTS_TOPIC)
                    .key(&res_key)
                    .payload(&res_payload),
                Duration::from_secs(5),
            )
            .await
            .expect("failed to send result");

        //step 3: consume the result (simulating scheduler reading completion)
        let group_id = format!("test-e2e-{}", uuid::Uuid::new_v4());
        let consumer = create_consumer(&group_id, RESULTS_TOPIC);

        //scan for our specific message
        let mut stream = consumer.stream();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);

        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                panic!("timed out waiting for e2e result with task_id={}", unique_id);
            }

            match tokio::time::timeout(remaining, stream.next()).await {
                Ok(Some(Ok(msg))) => {
                    if let Some(payload_bytes) = msg.payload() {
                        if let Ok(received) = serde_json::from_slice::<ResultMessage>(payload_bytes) {
                            if received.task_id == unique_id {
                                assert_eq!(received.status, "COMPLETED");
                                assert!(received.output.as_ref().unwrap().contains("4 REST endpoints"));
                                println!("e2e flow verified: component dispatched -> result received");
                                return;
                            }
                        }
                    }
                }
                Ok(Some(Err(e))) => panic!("consumer error: {}", e),
                Ok(None) => panic!("stream ended"),
                Err(_) => panic!("timed out waiting for e2e result with task_id={}", unique_id),
            }
        }
    }
}
