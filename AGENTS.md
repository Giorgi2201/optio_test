# AGENTS.md — AI Agent Operating Handbook & Architectural Directives

> **Authoritative Operational Guide**: This document establishes the non-negotiable engineering principles, operational invariants, structural boundaries, and verification protocols for any AI agent or software engineer modifying this repository.

---

## 1. Repository Mission & System Overview

### 1.1 Mission Statement
This platform is a **fault-tolerant, dual-sink data replication and synchronization engine** engineered for mission-critical, high-throughput environments. It extracts high-volume transactional data from **PostgreSQL** and reliably replicates it concurrently across two distinct downstream sinks:
1. **Elasticsearch**: Near-real-time analytical search index and document store.
2. **RabbitMQ**: Durable, decoupled event stream consumption pipeline, read by an independent consumer service.

### 1.2 The "Kill It Twice" Resilience Doctrine
The primary mandate of this system is unconditional crash and fault resilience:
- The system must withstand process termination (`SIGKILL`, container halts, out-of-memory restarts) at **any arbitrary microsecond** without data loss, document corruption, or duplicate side effects.
- Downstream outages (Elasticsearch connection drops, RabbitMQ broker node restarts, PostgreSQL connection pool exhaustion) must trigger controlled circuit breaker states rather than process death, unmanaged retry loops, or memory exhaustion.

### 1.3 Core Delivery Contract: Effectively-Once Semantics
True end-to-end distributed "Exactly-Once" delivery across heterogeneous non-XA storage layers is impossible without massive latency penalties. This system implements **Effectively-Once Processing** via:
- **At-Least-Once Ingestion & Transport**: Guaranteeing zero data loss across crashes and restarts via resilient replay from durable watermarks.
- **Deterministic Idempotency on Sinks**:
  - **Elasticsearch**: Deterministic document IDs (`_id` mapped directly to source primary key / UUID) and version-stamped upsert operations (`doc_as_upsert` with transactional sequencing).
  - **RabbitMQ & Downstream Consumer**: Deduplication keys (`message_id` = source entity PK + mutation sequence / updated timestamp) coupled with consumer-side atomic deduplication caches / sets.

---

## 2. Monorepo Map & Structural Boundaries

This repository is organized as a polyglot microservice monorepo with strict architectural boundaries. Cross-boundary dependencies must pass through validated package contracts.

```
OPTIO/
├── apps/
│   ├── pipeline/          # Replication daemon (Backfill & CDC incremental poller)
│   ├── consumer/          # Independent RabbitMQ event stream consumer
│   └── ui/                # Operational control dashboard & observability panel
├── packages/
│   └── shared/            # Shared domain contracts, DTOs, schemas, and utils
├── docker/                # Multi-service Compose setups, Dockerfiles, and init scripts
├── scripts/               # Verification harnesses, data seeders, and chaos scripts
└── docs/                  # Architecture Decision Records (ADRs) and benchmarks
```

### 2.1 Workspace Directory Responsibilities

| Directory | Scope & Responsibilities | Boundary Invariants |
| :--- | :--- | :--- |
| `apps/pipeline` | **The Replication Daemon**: Orchestrates initial historical backfill streaming, incremental CDC/watermark polling, batch aggregation, dual-sink fanout dispatch, circuit breaking, and persistent checkpoint management. | No direct UI rendering; communicates operational metrics via metrics endpoints/events. |
| `apps/consumer` | **Standalone Event Stream Consumer**: Independent background worker subscribing to RabbitMQ message queues, processing events, validating payload integrity, and maintaining consumer deduplication state. | Completely decoupled from the pipeline daemon; must tolerate pipeline pauses or bursts without backpressure stalls. |
| `apps/ui` | **Observability & Control Dashboard**: Real-time visualization of replication lag, ingestion throughput, sink health, circuit breaker state, error rates, and manual backfill trigger actions. | Read-only telemetry and authorized control actions via standardized APIs; no direct DB mutation. |
| `packages/shared` | **Shared Kernel**: Canonical TypeScript domain types, Zod schemas, validation contracts, serialization helpers, logger abstractions, and standardized error models. | Zero external transport side effects; pure types, schemas, and deterministic utility functions. |
| `docker/` | **Infrastructure as Code**: Production-parity Compose files, container definitions (PostgreSQL with logical replication / triggers, Elasticsearch, RabbitMQ with management plugins), volume mounts, and bootstrap SQL/scripts. | Must support deterministic clean-slate spin-up and teardown via reproducible environment configs. |
| `scripts/` | **Harness & Automation**: Automated verification harness (`verify.sh`), deterministic high-volume synthetic data seeder (`seed.sh`), and chaos injection testing scripts (network latency, SIGKILL triggers). | Verification scripts must remain authoritative and immutable against artificial green-washing. |
| `docs/` | **Knowledge Base**: Architectural Decision Records (ADRs), system topology diagrams, throughput benchmarks, and capacity planning models. | All design changes and deviation rationales must be documented here. |

---

## 3. Core Engineering & Architectural Conventions

### 3.1 Language & Runtime Rules
- **Runtime**: Node.js (LTS), executed with strict module boundaries.
- **Language**: **TypeScript** with strict mode enabled (`"strict": true`, `"noImplicitAny": true`, `"strictNullChecks": true`).
- **Zero `any` Policy**: The `any` type is strictly forbidden. Use `unknown` with narrowing, tagged unions, or Zod schema parsing.
- **Clean Architecture**: Domain logic must remain independent of specific external client libraries (abstract database adapters, Elasticsearch clients, and AMQP connection wrappers behind explicit interfaces).

### 3.2 High-Volume Streaming & Bounded Memory
- **No Unbounded Memory Buffers**: Never execute unpaged SQL queries (e.g., `SELECT * FROM large_table`). Never load large record sets into Node.js heap memory.
- **Bounded Batch Streaming**: Extraction from PostgreSQL must strictly utilize either:
  1. **Keyset Pagination**: Fast, index-backed seek queries (`WHERE id > :last_seen_id ORDER BY id ASC LIMIT :batch_size`).
  2. **Server-side Streaming Cursors**: Bounded cursor pipelines with backpressure propagation.
- **Batch Sizing**: Extraction batches must be bounded (e.g., 500 to 5,000 records per chunk), dynamically adjustable based on heap memory and sink acknowledgment latencies.

### 3.3 Downstream Resilience & Circuit Breaking
- **Dual-Sink Isolation**: Elasticsearch and RabbitMQ writes must occur concurrently using isolated adapters. A slowdown in one sink must not cause memory leaks in the other.
- **Retry Strategy**: All downstream network calls must employ bounded exponential backoff with full jitter to avoid the thundering herd problem.
- **Circuit Breaker States**: Every sink adapter must incorporate a 3-state circuit breaker (`CLOSED`, `OPEN`, `HALF-OPEN`):
  - In `OPEN` state, ingestion halts, polling pauses, and memory backpressure is applied immediately without busy-wait spinning.
- **Fail-Fast Timeouts**: Every network socket, HTTP request, and AMQP publish operation must specify a deterministic timeout.

### 3.4 Atomic Checkpoint Persistence
- **Post-Acknowledgment Commit Rule**: Checkpoints (watermarks, committed IDs, or LSN offsets) must strictly be persisted **ONLY AFTER** both downstream sinks (Elasticsearch AND RabbitMQ) have acknowledged the batch write.
- **No Speculative Checkpoints**: Under no circumstance may an offset be committed prior to dual confirmation.
- **Atomic State Storage**: The checkpoint store (Postgres table or dedicated key-value metadata store) must execute writes transactionally or via idempotent version checks.

---

## 4. Forbidden Actions ("Do Not Touch" Rules)

Any agent or engineer modifying this repository must strictly adhere to the following prohibitions:

1. **PROHIBITION: No Unbounded In-Memory Collections**
   - *Forbidden*: Reading entire tables or unbound result sets into Node.js memory arrays (`const rows = await db.query('SELECT * ...')`).
   - *Requirement*: Always use bounded keyset pagination or backpressure-governed streaming.

2. **PROHIBITION: No Silent Error Swallowing or Dropped Records**
   - *Forbidden*: Catching exceptions without logging or silently ignoring unmappable documents.
   - *Requirement*: Unparseable or malformed records must be routed to a dedicated Dead Letter Queue (DLQ) with error metadata; operational records must never vanish.

3. **PROHIBITION: No Total Batch Abort on Isolated Record Corruption**
   - *Forbidden*: Failing or rolling back an entire batch of 5,000 records because a single record contains invalid schema data.
   - *Requirement*: Isolate the poisonous payload to the DLQ, and process the remaining valid records to maintain pipeline throughput.

4. **PROHIBITION: No Manipulation of Verification Logic**
   - *Forbidden*: Modifying `verify.sh`, `make verify`, or test assertion criteria to bypass failures, create dummy mock outputs, or fabricate a green `PASS`.
   - *Requirement*: Every test and gate pass must reflect genuine, end-to-end data integrity and service behavior.

5. **PROHIBITION: No Hardcoded Secrets or Hostnames**
   - *Forbidden*: Hardcoding credentials, ports, or hostnames (e.g., `localhost:9200`, `amqp://guest:guest@localhost:5672`).
   - *Requirement*: Always parse and validate environmental configuration via typed config loaders with schema validation.

---

## 5. Self-Verification & Quality Gates Protocol

Before completing any task or committing changes, an agent must execute the following sequential verification gate:

```mermaid
flowchart LR
    A[Code Changes] --> B[1. Static Analysis & Typecheck]
    B --> C[2. Unit & Integration Sanity]
    C --> D[3. Automated Verification Harness]
    D --> E[4. Git Diff Inspection]
    E --> F[Ready to Commit]
```

### Step 1: Static Analysis & Type Checking
- Run linter and type-checker across all workspaces:
  ```bash
  npm run lint
  npm run typecheck
  ```
- Ensure zero errors, zero warnings, and zero implicit `any` violations.

### Step 2: Unit & Integration Tests
- Run unit test suites for individual packages and applications:
  ```bash
  npm run test
  ```
- Confirm all mocks accurately reflect external adapter contracts.

### Step 3: Verification Harness (`scripts/verify.sh`)
- Execute the authoritative verification script to validate end-to-end pipeline semantics:
  ```bash
  bash scripts/verify.sh
  ```
- Confirm:
  - Source data row count equals Elasticsearch indexed document count.
  - Source data row count equals RabbitMQ consumer acknowledged message count.
  - Deterministic idempotency preserves document consistency when pipeline is restarted mid-stream.
  - Zero unhandled exceptions or memory leaks.

### Step 4: Git Diff Audit
- Inspect the git diff to ensure no unexpected files, temporary artifacts, or forbidden edits were introduced:
  ```bash
  git diff --stat
  git status
  ```

---

## 6. AI Deviation Protocol

If an AI agent discovers during planning, prototyping, or implementation that an aspect of the technical specification is flawed, incomplete, or requires an architectural pivot:

1. **Do Not Silently Deviate**: Never implement an architectural deviation without explicit documentation and justification.
2. **Log the Deviation in ADR / Documentation**:
   - Create or update an Architectural Decision Record in `docs/adr/`.
   - Explicitly detail:
     * **Context**: The original requirement or specification constraint.
     * **Discovery**: The technical reality or operational edge case encountered (e.g., memory exhaustion, concurrency deadlock, framework limitation).
     * **Chosen Alternative**: The architecture pattern adopted instead.
     * **Impact & Trade-offs**: Latency, complexity, or resource implications.
3. **Register in README**:
   - Prepare a clear entry for the authoritative section in `README.md`:
     `### Where AI Deviated from the Specification`
   - Include the rationale, the alternative implemented, and why it is superior for the system's fault-tolerance invariants.

---

*This document represents the immutable operating contract for automated agents and engineers working on the OPTIO platform.*
