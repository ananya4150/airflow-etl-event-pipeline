# Airflow ETL Event Pipeline

## Overview
This project implements an end-to-end ETL (Extract, Transform, Load) pipeline using Apache Airflow and PostgreSQL. The system simulates real-world event-driven data ingestion, processes semi-structured messages, and loads structured data into relational tables.

The pipeline handles multiple data types including customers, products, and reviews, while incorporating validation, error handling, and cleanup mechanisms.

---

## Problem Statement
Real-world data pipelines often need to process semi-structured or noisy event streams, validate incoming data, and transform it into structured formats suitable for analytics or downstream systems.

This project simulates such a system by:
- Generating synthetic event data
- Ingesting it into a database
- Categorizing and validating records
- Loading structured data into normalized tables
- Cleaning up processed events

---

## System Architecture
Event Simulator → PostgreSQL (events table) → Airflow DAG → Processing Tasks → Structured Tables → Cleanup

### Components
- **Event Generator**: Produces synthetic customer, product, and review events
- **PostgreSQL**: Stores raw and processed data
- **Airflow DAG**: Orchestrates pipeline tasks
- **Processing Tasks**: Validate and transform data
- **Cleanup Task**: Removes processed records

---

## Pipeline Workflow

### 1. Event Generation
A Python-based simulator generates synthetic events and inserts them into the `events` table.

Event types:
- `CUST` → Customer records
- `PROD` → Product records
- `VIEW` → Review records

---

### 2. Categorization Task
The DAG reads incoming events and:
- Identifies message types
- Filters invalid records
- Groups events into categories

---

### 3. Parallel Processing Tasks

#### Customer Processing
- Parses user data
- Validates required fields
- Inserts into `customers` table

#### Product Processing
- Extracts structured fields using fixed-width parsing
- Validates stock and pricing data
- Inserts into `products` table

#### Review Processing
- Parses JSON payloads
- Validates foreign key relationships
- Joins customer and product data
- Inserts into `reviews` table

---

### 4. Cleanup Task
- Removes successfully processed event records
- Logs processed vs invalid entries
- Ensures idempotency of pipeline runs

---

## Database Schema

### Customers
```sql
UserName VARCHAR(30) PRIMARY KEY,
Email VARCHAR(30),
JoinDate DATE
