# Food Ordering System

A food ordering system built with 4 microservices, implementing Saga, Outbox, and CQRS patterns. Designed with Clean and Hexagonal Architecture principles, using Kafka for event-driven communication between services.

## How It Works

1. Customer creates an order → Order Service saves it
2. Payment request is sent to Payment Service via Kafka
3. If payment succeeds, an approval request is sent to Restaurant Service
4. If the restaurant approves, the order is completed

If any step fails, the Saga pattern triggers compensating transactions to keep the system consistent.

## Architecture

The project consists of 4 independent microservices: Order Service (order management and Saga orchestration), Payment Service (payment processing), Restaurant Service (restaurant approval), and Customer Service (customer management).

Each service is designed with Hexagonal Architecture — the domain layer has no dependency on any framework. The system follows DDD principles: aggregate roots, entities, value objects, and domain events all live in each service's own domain layer.

Services communicate asynchronously through Kafka. Messages are serialized with Apache Avro using Schema Registry for type safety. The Outbox pattern guarantees that events are written atomically to both the database and Kafka within the same transaction — this prevents message loss.

## Patterns Implemented

### Saga Pattern (Choreography)
```
Order Created → [Order Service]
    ↓ (Kafka: payment-request)
Make Payment → [Payment Service]
    ↓ (Kafka: payment-response — success)
Approve Restaurant → [Restaurant Service]
    ↓ (Kafka: restaurant-approval-response — success)
Complete Order → [Order Service]
```

Error case:
```
Restaurant Rejected → (Kafka: restaurant-approval-response — fail)
    ↓
Cancel Payment → [Payment Service] (compensating transaction)
    ↓
Cancel Order → [Order Service]
```

### Outbox Pattern
Services don't publish events directly to Kafka. Instead, they write events to an outbox table in the database within the same transaction. A scheduler (OutboxScheduler) periodically checks this table and publishes pending events to Kafka. This guarantees that "saved to database but failed to send to Kafka" never happens.

### CQRS
Within Order Service, order creation (command) and order tracking (query) are handled by separate handlers.

## Tech Stack

- **Java, Spring Boot** — foundation for all services
- **Hexagonal Architecture** (Ports & Adapters) — domain/application/dataaccess/messaging layers per service
- **DDD** (Domain-Driven Design) — aggregate roots, entities, value objects, domain events, domain services
- **Apache Kafka** — asynchronous event-driven communication between services
- **Apache Avro + Schema Registry** — type-safe message serialization with schema evolution support
- **Saga Pattern** — distributed transaction management (order → payment → approval flow)
- **Outbox Pattern** — atomic event publishing to database + Kafka
- **CQRS** — separation of command and query responsibilities
- **PostgreSQL** — each service has its own database (database-per-service pattern)
- **Zookeeper** — Kafka cluster coordination
- **Docker Compose** — orchestration of all infrastructure (Kafka, Zookeeper, Schema Registry, PostgreSQL)
- **Unit + Integration Tests** — including Saga flow tests (double payment idempotency)

## Project Structure

```
food-ordering-system/
├── common/                              # Shared modules
│   ├── common-domain/                   # Base entity, AggregateRoot, ValueObject, DomainEvent
│   ├── common-application/              # GlobalExceptionHandler, ErrorDTO
│   └── common-dataaccess/               # Shared Restaurant JPA entity
├── order-service/                       # Order Service (Saga orchestrator)
│   ├── order-domain/
│   │   ├── order-domain-core/           # Order entity, domain events, OrderDomainService
│   │   └── order-application-service/   # Use cases, Saga management, Outbox schedulers
│   ├── order-application/               # REST controller
│   ├── order-dataaccess/                # JPA entities, outbox tables, repositories
│   ├── order-messaging/                 # Kafka producer/consumer (Avro models)
│   └── order-container/                 # Spring Boot app, config, init-schema.sql
├── payment-service/                     # Payment Service
│   ├── payment-domain/                  # Payment, CreditEntry, CreditHistory, domain service
│   ├── payment-dataaccess/              # JPA entities, outbox tables
│   ├── payment-messaging/              # Kafka producer/consumer
│   └── payment-container/               # Spring Boot app
├── restaurant-service/                  # Restaurant Approval Service
│   ├── restaurant-domain/               # Restaurant, OrderApproval, domain service
│   ├── restaurant-dataaccess/           # JPA entities, outbox tables
│   ├── restaurant-messaging/            # Kafka producer/consumer
│   └── restaurant-container/            # Spring Boot app
├── customer-service/                    # Customer Service
│   ├── customer-domain/                 # Customer entity, domain service
│   ├── customer-dataaccess/             # JPA entities
│   ├── customer-messaging/              # Kafka producer (CustomerCreatedEvent)
│   └── customer-container/              # Spring Boot app
├── infrastructure/
│   ├── kafka/                           # Kafka infrastructure modules
│   │   ├── kafka-config-data/           # Kafka configuration classes
│   │   ├── kafka-model/                 # Avro models and schemas (.avsc)
│   │   ├── kafka-producer/              # Generic Kafka producer implementation
│   │   └── kafka-consumer/              # Generic Kafka consumer configuration
│   ├── outbox/                          # Outbox scheduler infrastructure
│   ├── saga/                            # Saga step and status definitions
│   └── docker-compose/                  # Docker Compose files
│       ├── common.yml                   # Shared service definitions
│       ├── kafka_cluster.yml            # Kafka broker configuration
│       ├── zookeeper.yml                # Zookeeper configuration
│       └── init_kafka.yml               # Kafka topic creation
└── json-files/                          # Test data and Postman collection
```

## Key Design Decisions

| Decision | Choice | Why |
|----------|--------|-----|
| Architecture | Hexagonal + DDD | Each service's domain is framework-independent, testable, and replaceable |
| Inter-Service Communication | Kafka (async, event-driven) | Loose coupling, high throughput, built-in retry mechanism |
| Distributed Transactions | Saga Pattern (choreography) | Instead of 2PC — each service manages its own transaction, compensating on failure |
| Message Reliability | Outbox Pattern | Prevents event loss — atomic writes to both database and Kafka |
| Message Format | Apache Avro + Schema Registry | Type-safe serialization, schema evolution support, small payload size |
| Data Separation | CQRS | Separate handlers for order creation (command) and order tracking (query) |
| Database | PostgreSQL per service | Database-per-service pattern — services never access each other's database |

## What I Learned

- Managing distributed transactions with the Saga pattern: order → payment → approval flow and compensating transactions on failure
- Publishing events safely within database transactions using the Outbox pattern
- Type-safe message serialization with Apache Avro and Schema Registry integration
- Applying Hexagonal Architecture consistently across 4 different microservices
- Implementing DDD in a real domain (ordering, payment, restaurant approval) — aggregate roots, value objects, domain events
- Building Outbox schedulers that periodically publish pending events to Kafka
- Writing Saga tests — including double payment idempotency and failure scenarios

## Getting Started

### Prerequisites
- Java 17+
- Docker and Docker Compose

### Run
```bash
# 1. Start Kafka infrastructure
cd infrastructure/docker-compose
docker-compose -f common.yml -f zookeeper.yml up -d
docker-compose -f common.yml -f kafka_cluster.yml up -d
docker-compose -f common.yml -f init_kafka.yml up -d

# 2. Start services (in separate terminals)
# Order Service → port 8181
# Payment Service → port 8182
# Restaurant Service → port 8183
# Customer Service → port 8184
```

## Status
🟢 Complete — serves as a reference implementation for Saga, Outbox, and CQRS patterns
