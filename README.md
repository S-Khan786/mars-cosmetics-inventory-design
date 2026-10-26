# Mars Cosmetics — Order & Inventory Sync Service

**Assignment:** Mars Assessment Order Inventory Sync  
**Role:** Software Engineer  
**Author:** Mohammad  

---

## What This Repository Contains

This is a **system design submission** — not a running application. The repository contains a detailed technical design document for building an automated Order & Inventory Sync Service for Mars Cosmetics.

| File | Description |
|------|-------------|
| `DESIGN.md` | Full technical design document |
| `README.md` | This file |

---

## Problem Summary

Mars Cosmetics sells across 4 channels (Website, Nykaa, Flipkart, Blinkit) from a single warehouse. Stock was managed manually via spreadsheets, causing 900 units to be oversold during a flash sale. This design proposes an automated service to fix that.

---

## My Approach

- **Single source of truth:** PostgreSQL holds all inventory data. No dual-write between systems.
- **Atomic stock reservation:** `SELECT ... FOR UPDATE` row locking in PostgreSQL guarantees zero overselling even under concurrent load (flash sale scenario).
- **Async, queue-based architecture:** BullMQ (Redis-backed) queues decouple order ingestion from channel sync, giving fault isolation and per-channel rate limiting out of the box.
- **Idempotent order processing:** A unique `idempotency_key` column (`channel:external_order_id`) with a DB-level `UNIQUE` constraint silently rejects duplicate webhooks at the database.
- **ERP downtime handled gracefully:** Exponential backoff retry queue accumulates jobs during Business Central's Sunday maintenance window and drains automatically when it comes back online.

---

## Design Document

See [`DESIGN.md`](./DESIGN.md) for the full design, including:

- Architecture diagram (Mermaid)
- PostgreSQL schema (DDL)
- Order ingestion flow (webhooks + Flipkart polling)
- Stock reservation pseudo-code
- Fan-out to all 4 channels with rate limiting
- ERP sync with retry + Dead Letter Queue
- Observability metrics and alerts
- 6-week MVP delivery plan
- AWS cost estimate (~₹17,000/month vs ₹50,000 budget)
- Trade-offs table

---

## Tech Stack

- **Backend:** Node.js / NestJS
- **Database:** PostgreSQL (AWS RDS Multi-AZ)
- **Cache & Queues:** Redis (AWS ElastiCache) + BullMQ
- **Infrastructure:** AWS (ECS Fargate, ALB, S3, CloudWatch)

---

## Contact

For any questions regarding this submission, feel free to reach out.
