# ADR-004: Streaming Layer — Apache Kafka

## Date
2026-04-07

## Status
Accepted

## Context
The batch pipeline processes data once a day via Airflow. However, we also needed a near real-time data ingestion capability to simulate a production insurance system where new policies are created continuously. The streaming layer needed to:
- Decouple data producers from consumers
- Handle high-throughput message passing reliably
- Enable near real-time data availability in the Bronze layer
- Be a realistic alternative to Microsoft Fabric's Real-Time Analytics (Eventstream)

## Decision
We chose **Apache Kafka** as the streaming layer.

## Reasons
- Industry standard for real-time data streaming
- Decouples producer and consumer — they can scale independently
- Messages are persisted on disk, so consumers can replay events
- Widely used in insurance, banking and e-commerce for event-driven architectures
- Runs easily via Docker with Zookeeper

## Alternatives Considered
- **RabbitMQ** — better for task queues, not optimized for high-throughput streaming
- **Redis Streams** — lightweight but limited retention and replay capabilities
- **AWS Kinesis** — managed service, not suitable for local development
- **Direct file write** — no decoupling, no fault tolerance, not scalable

## Consequences
- Kafka producer generates a new policy event every 30 seconds
- Kafka consumer reads events and writes individual Parquet files to MinIO under `realtime/` prefix
- Each real-time policy is stored as a separate Parquet file with timestamp in the filename
- In production, Kafka should run as a multi-broker cluster with replication factor > 1
- Zookeeper dependency can be replaced with KRaft mode in newer Kafka versions