# 🛒 Real-Time E-Commerce Clickstream Analytics Platform

<p align="center">

<img src="https://skillicons.dev/icons?i=python,azure,spark,docker,github&theme=light" alt="Technology Stack"/>

</p>

<p align="center">

<img src="https://img.shields.io/badge/Python-3.x-blue?logo=python" alt="Python"/>
<img src="https://img.shields.io/badge/Apache%20Spark-3.5.3-orange?logo=apachespark" alt="Apache Spark"/>
<img src="https://img.shields.io/badge/Azure-Cloud-blue?logo=microsoftazure" alt="Azure"/>
<img src="https://img.shields.io/badge/Docker-Containerized-blue?logo=docker" alt="Docker"/>
<img src="https://img.shields.io/badge/Power%20BI-Analytics-yellow?logo=powerbi" alt="Power BI"/>
<img src="https://img.shields.io/badge/Delta%20Lake-Data%20Lakehouse-00ADD8" alt="Delta Lake"/>

</p>

<p align="center">
  <b>End-to-end real-time e-commerce clickstream analytics using Python, Azure Event Hubs, Spark Structured Streaming, ADLS Gen2, Azure SQL and Power BI.</b>
</p>

---

## 📌 Project Overview

The **Real-Time E-Commerce Clickstream Analytics Platform** is an end-to-end Data Engineering project designed to simulate how an e-commerce organization can capture, process, clean, aggregate and analyze user clickstream activity in near real time.

The platform generates realistic user journeys such as:

* Homepage visits
* Product searches
* Category browsing
* Product views
* Add-to-cart events
* Checkout activity
* Purchases

These events are published to **Azure Event Hubs**, processed using **Dockerized Apache Spark Structured Streaming**, stored in an **ADLS Gen2 Bronze/Silver/Gold architecture**, and exposed through **Azure SQL Database** for consumption in **Power BI**.

The project also deliberately generates bad-quality data such as missing values, invalid timestamps, missing products and duplicate events to demonstrate practical data-quality and quarantine handling.

### Final pipeline

```text
Python Clickstream Generator
            │
            ▼
     Azure Event Hubs
            │
            ▼
Spark Structured Streaming
            │
            ├──────────────► Quarantine
            │
            ▼
        ADLS Gen2
     Bronze → Silver → Gold
            │
            ▼
      Azure SQL Database
            │
            ▼
          Power BI
```

---

# 🎯 Business Problem

Traditional e-commerce reporting often relies heavily on batch-oriented daily reports.

That makes it difficult to answer questions such as:

* Which products are currently receiving the most attention?
* How many users are active?
* Which products generate the most revenue?
* Where are users dropping out of the purchasing journey?
* How many users add products to their cart but do not purchase?
* Which traffic sources generate the most engagement?
* How does revenue change over time?
* Which categories contribute most to revenue?

This project demonstrates a streaming-oriented architecture capable of transforming raw clickstream events into business-ready analytical datasets.

---

# 🎯 Business Objectives

The platform was designed around the following analytical requirements:

### User Behaviour

* Track user activity
* Track sessions
* Understand product browsing behaviour
* Analyze user journeys

### Product Analytics

* Identify high-performing products
* Track product views
* Track add-to-cart activity
* Track purchases
* Analyze product-level revenue

### Sales Analytics

* Monitor revenue
* Monitor purchases
* Calculate conversion-related metrics
* Analyze revenue trends
* Compare category performance

### Data Engineering

* Process streaming data
* Handle malformed events
* Implement Bronze/Silver/Gold layers
* Implement checkpointing
* Implement event-time processing
* Support scalable distributed processing
* Separate raw, cleaned and business-ready datasets

---

# 🏗️ High-Level Architecture

![High-Level Architecture](docs/architecture/high_level_architecture.png)

> **Screenshot placeholder:** `docs/architecture/high_level_architecture.png`

The major pipeline stages are:

```text
Python Generator
      ↓
Azure Event Hubs
      ↓
Spark Structured Streaming
      ↓
ADLS Gen2
      ├── Bronze
      ├── Silver
      ├── Gold
      └── Quarantine
      ↓
Azure SQL Database
      ↓
Power BI
```

---

# 🔬 Low-Level Architecture

![Low-Level Architecture](docs/architecture/low_level_architecture.png)

> **Screenshot placeholder:** `docs/architecture/low_level_architecture.png`

### Detailed flow

```text
User Simulator
      │
      ▼
Session Generator
      │
      ▼
Event Generator
      │
      ├── User/session metadata
      ├── Product metadata
      ├── Event type
      ├── Traffic source
      ├── Event timestamp
      └── Data-quality injection
      │
      ▼
Azure Event Hubs
      │
      ▼
Spark Structured Streaming
      │
      ├── Ingestion
      ├── Parsing
      ├── Validation
      ├── Transformation
      ├── Event-time processing
      └── Aggregation
      │
      ├───────────────┐
      │               │
      ▼               ▼
   Valid data     Invalid data
      │               │
      ▼               ▼
    Silver       Quarantine
      │
      ▼
     Gold
      │
      ├── Product Trends
      └── Daily KPIs
      │
      ▼
Power BI Serving Data
      │
      ▼
Azure SQL
      │
      ▼
Power BI Dashboard
```

---

# 🧰 Technology Stack

| Technology                       | Purpose                                     |
| -------------------------------- | ------------------------------------------- |
| **Python**                       | Clickstream simulation and event generation |
| **Azure Event Hubs**             | Real-time event ingestion                   |
| **Apache Spark 3.5.3**           | Distributed stream processing               |
| **Spark Structured Streaming**   | Streaming ingestion and transformations     |
| **Delta Lake**                   | Lakehouse-oriented data processing          |
| **Azure Data Lake Storage Gen2** | Bronze/Silver/Gold storage                  |
| **Docker**                       | Containerized Spark execution               |
| **Azure SQL Database**           | BI serving layer                            |
| **Power BI**                     | Business analytics and visualization        |
| **Git/GitHub**                   | Version control and project collaboration   |
| **YAML / `.env`**                | Configuration and secret management         |

---

# ☁️ Azure Resources

The project uses the following Azure resources.

| Resource             | Name                     | Purpose                    |
| -------------------- | ------------------------ | -------------------------- |
| Resource Group       | `rg-clickstream-dev`     | Resource organization      |
| Storage Account      | `stclickstreamaa01`      | ADLS Gen2 data lake        |
| Event Hubs Namespace | `ehns-clickstream-dev`   | Streaming ingestion        |
| Event Hub            | `clickstream-events`     | Clickstream event stream   |
| SQL Server           | `sql-clickstream-dev-db` | SQL serving infrastructure |
| SQL Database         | `sqldb-clickstream`      | Power BI serving database  |

The Event Hub uses **2 partitions** to demonstrate partitioned event ingestion.

---

# 🧪 1. Clickstream Event Generation

The Python generator acts as a simulated e-commerce application.

Instead of manually inserting records, the generator creates user behaviour dynamically.

## User personas

The simulator contains multiple behavioural personas, including:

* Returning Customer
* Casual Browser
* Impulse Buyer
* Window Shopper
* Serious Buyer

Each persona has different probabilities for actions such as:

```text
Search
Product View
Add to Cart
Purchase
```

The simulator also incorporates:

* Session IDs
* User IDs
* Product selection
* Traffic sources
* Event delays
* Session journeys
* Randomized behaviour

This creates more realistic clickstream behaviour than simply generating random rows.

---

# 🧑‍💻 2. Data Quality Simulation

One of the main objectives was to make the pipeline behave like a real Data Engineering system rather than a perfect-data demonstration.

The generator can intentionally introduce:

| Data Issue         | Purpose                        |
| ------------------ | ------------------------------ |
| Missing user IDs   | Test required-field validation |
| Missing products   | Test product validation        |
| Invalid timestamps | Test timestamp handling        |
| Duplicate events   | Test duplicate handling        |
| Late events        | Test event-time processing     |
| Missing values     | Test data-quality rules        |

Example configuration:

```python
ENABLE_BAD_DATA = True

MISSING_USER_PROBABILITY = 0.01
DUPLICATE_EVENT_PROBABILITY = 0.01
INVALID_TIMESTAMP_PROBABILITY = 0.005
MISSING_PRODUCT_PROBABILITY = 0.005
LATE_EVENT_PROBABILITY = 0.05
```

The objective is not merely to generate data, but to demonstrate how a streaming pipeline behaves when the incoming data is imperfect.

---

# 🚀 3. Azure Event Hubs

Generated events are serialized as JSON and published to:

```text
Azure Event Hubs Namespace
        │
        └── clickstream-events
```

Event Hubs provides the ingestion buffer between the application layer and Spark.

### Flow

```text
Python
  │
  │ JSON events
  ▼
Azure Event Hubs
  │
  │ Streaming events
  ▼
Spark Structured Streaming
```

### Event Hub configuration

* Namespace: `ehns-clickstream-dev`
* Event Hub: `clickstream-events`
* Partition count: 2
* Retention: configured for the project environment
* Producer authentication: SAS policy
* Credentials: stored outside source code

---

# ⚡ 4. Dockerized Spark Structured Streaming

Spark Structured Streaming is the core processing engine.

The project runs multiple Dockerized Spark services.

### Spark services

```text
spark
silver
gold
```

The corresponding containers include:

```text
clickstream-spark
clickstream-silver
clickstream-gold
```

### Processing responsibilities

Spark handles:

* Event ingestion
* JSON parsing
* Schema enforcement
* Data validation
* Transformation
* Event-time processing
* Windowed aggregation
* Writing to ADLS
* Checkpointing
* Recovery/state management

---

# 🗂️ 5. ADLS Gen2 Data Lake

The project follows a Bronze/Silver/Gold architecture.

```text
ADLS Gen2
│
├── bronze/
│
├── silver/
│
├── gold/
│
├── quarantine/
│
└── checkpoints/
```

---

## 🥉 Bronze Layer

Location:

```text
bronze/clickstream/
```

Purpose:

> Store incoming clickstream data with minimal transformation.

Bronze provides a raw landing layer that can be used for downstream processing and troubleshooting.

---

## 🥈 Silver Layer

Location:

```text
silver/clickstream/
```

Purpose:

> Store cleaned, validated and transformed clickstream events.

Typical processing includes:

```text
Bronze
  ↓
Schema parsing
  ↓
Validation
  ↓
Data cleaning
  ↓
Transformation
  ↓
Silver
```

---

## 🥇 Gold Layer

The Gold layer contains business-oriented analytical datasets.

### Product Trends

```text
gold/product_trends/
```

Used for:

* Product performance
* Product revenue
* Product purchasing behaviour
* Product-level analysis

### Daily KPIs

```text
gold/daily_kpis/
```

Used for:

* Daily events
* Unique users
* Sessions
* Purchases
* Revenue
* Product activity

---

# 🚨 6. Quarantine Layer

Invalid records are separated from valid analytical data.

```text
Invalid Event
     │
     ▼
Quarantine
```

Location:

```text
quarantine/clickstream/
```

This prevents malformed events from contaminating downstream analytical datasets while retaining them for inspection.

---

# 💾 7. Checkpointing

Spark streaming checkpoints are stored in ADLS.

Examples include:

```text
checkpoints/clickstream
checkpoints/clickstream_silver
checkpoints/clickstream_quarantine
checkpoints/gold_product_trends
checkpoints/gold_daily_kpis
```

Checkpointing allows streaming jobs to maintain processing state and recover from interruptions.

It is especially important for stateful operations such as event-time aggregations.

---

# 🗄️ 8. Azure SQL Serving Layer

Azure SQL was introduced as the final BI-serving layer.

This decision was made after testing direct Power BI connectivity with the ADLS data and encountering authentication/access complications with the personal Microsoft account used for the project.

Instead of weakening the architecture or relying on fragile authentication workarounds, a dedicated serving layer was introduced:

```text
ADLS Gold
    ↓
Power BI preparation
    ↓
Azure SQL
    ↓
Power BI
```

### Database

```text
sqldb-clickstream
```

### Main serving tables

```text
DailyKPIs
ProductTrends
```

This gives Power BI a clean relational interface to the business-ready datasets.

---

# 📊 9. Power BI Dashboard

The final dashboard provides a business-facing view of the streaming pipeline.

## KPI Cards

The dashboard contains:

* Total Events
* Unique Users
* Unique Sessions
* Purchases
* Revenue

## Analytical Visuals

### Revenue Over Time

Tracks revenue trends across the selected date range.

### Top 10 Products by Revenue

Identifies the products contributing the most revenue.

### Revenue by Category

Shows category-level revenue distribution.

### Purchases Over Time

Tracks purchasing activity by date.

### Add-to-Cart vs Purchases

Compares cart activity with completed purchases.

### Top 10 Products by Purchases

Shows products with the highest purchase volume.

## Interactive Slicers

The dashboard includes:

* Date
* Category
* Product

These allow users to interactively filter the analytical views.

---

# 📸 Dashboard Preview

![Final Power BI Dashboard](docs/screenshots/13_powerbi_dashboard.png)

> **Screenshot placeholder:** `docs/screenshots/13_powerbi_dashboard.png`

### Dashboard with filters

![Power BI Dashboard Filters](docs/screenshots/14_powerbi_filters.png)

> **Screenshot placeholder:** `docs/screenshots/14_powerbi_filters.png`

---

# 🧮 Key Analytical Measures

Several DAX measures were created for the dashboard.

### Total Add-to-Carts

```DAX
Total Add to Carts =
SUM(DailyKPIs[add_to_cart])
```

### Total Product Views

```DAX
Total Product Views =
SUM(DailyKPIs[product_views])
```

### Conversion Rate

```DAX
Conversion Rate =
DIVIDE(
    [Total Purchases],
    [Total Sessions],
    0
)
```

### Cart-to-Purchase Rate

```DAX
Cart to Purchase Rate =
DIVIDE(
    [Total Purchases],
    [Total Add to Carts],
    0
)
```

### Average Order Value

```DAX
Average Order Value =
DIVIDE(
    [Total Revenue],
    [Total Purchases],
    0
)
```

### Revenue per Session

```DAX
Revenue per Session =
DIVIDE(
    [Total Revenue],
    [Total Sessions],
    0
)
```

---

# 🧪 10. Load Testing

The project was tested with a dedicated load-test script.

The initial load test generated:

```text
Sessions : 4,000
Events   : 26,969
Elapsed  : ~636 seconds
Rate     : ~42.4 events/sec
```

A second historical enrichment test was performed to create a more useful analytical time series.

### Historical load test

```text
Date          Sessions       Events
---------------------------------------
2026-09-04       500         3,327
2026-09-05       800         5,297
2026-09-06       650         4,376
2026-09-07     1,000         6,729
---------------------------------------
Total          2,950        19,729
```

The historical events were still sent through the actual pipeline:

```text
Generator
    ↓
Event Hubs
    ↓
Spark
    ↓
Bronze
    ↓
Silver
    ↓
Gold
    ↓
Azure SQL
    ↓
Power BI
```

No direct insertion into the analytical tables was used for this enrichment.

---

# 🧪 Load Test Screenshot

![Historical Load Test](docs/screenshots/02_generator_running.png)

> **Screenshot placeholder:** Replace this with the appropriate load-test screenshot if a separate image was captured.

---

# 🔍 11. Validation and Evidence

The pipeline was validated at multiple stages.

## Event Generation

![Generator Running](docs/screenshots/02_generator_running.png)

> **Screenshot placeholder:** `docs/screenshots/02_generator_running.png`

---

## Event Hubs

![Event Hubs Overview](docs/screenshots/03_eventhub_overview.png)

> **Screenshot placeholder:** `docs/screenshots/03_eventhub_overview.png`

### Event ingestion metrics

![Event Hubs Metrics](docs/screenshots/04_eventhub_metrics.png)

> **Screenshot placeholder:** `docs/screenshots/04_eventhub_metrics.png`

---

## ADLS

### Containers

![ADLS Containers](docs/screenshots/05_adls_containers.png)

> **Screenshot placeholder:** `docs/screenshots/05_adls_containers.png`

### Bronze

![Bronze Data](docs/screenshots/06_bronze_data.png)

> **Screenshot placeholder:** `docs/screenshots/06_bronze_data.png`

### Silver and Quarantine

![Silver and Quarantine](docs/screenshots/07_silver_quarantine.png)

> **Screenshot placeholder:** `docs/screenshots/07_silver_quarantine.png`

### Gold

![Gold Outputs](docs/screenshots/08_gold_outputs.png)

> **Screenshot placeholder:** `docs/screenshots/08_gold_outputs.png`

---

## Spark

### Dockerized Spark Services

![Spark Containers](docs/screenshots/09_spark_containers.png)

> **Screenshot placeholder:** `docs/screenshots/09_spark_containers.png`

### Spark Processing

![Spark Processing](docs/screenshots/10_spark_processing.png)

> **Screenshot placeholder:** `docs/screenshots/10_spark_processing.png`

---

## Azure SQL

![Azure SQL](docs/screenshots/11_azure_sql.png)

> **Screenshot placeholder:** `docs/screenshots/11_azure_sql.png`

### SQL Results

![SQL Results](docs/screenshots/12_sql_results.png)

> **Screenshot placeholder:** `docs/screenshots/12_sql_results.png`

---

# 🐳 12. Containerization

The Spark processing layer is containerized using Docker.

The project separates processing responsibilities into multiple services:

```text
docker-compose.yml
        │
        ├── spark
        ├── silver
        └── gold
```

This provides:

* Reproducible Spark environments
* Isolated processing jobs
* Dependency management
* Easier local development
* Consistent runtime configuration

Important Spark dependencies are packaged into the Docker images rather than being downloaded dynamically at runtime.

---

# 📁 13. Project Structure

The project is organized approximately as follows:

```text
RealTime_Clickstream_Analytics/
│
├── producer/
│   ├── event_generator.py
│   ├── eventhub_producer.py
│   ├── session_generator.py
│   ├── user_simulator.py
│   └── ...
│
├── streaming/
│   ├── ...
│   ├── prepare_powerbi.py
│   └── load_sql.py
│
├── tests/
│   ├── ...
│   ├── load_test.py
│   └── historical_load_test.py
│
├── configs/
│   ├── config.py
│   └── config.yaml
│
├── docker/
│   └── ...
│
├── docker-compose.yml
├── requirements.txt
├── .env
├── .gitignore
└── README.md
```

> The exact repository structure may contain additional supporting modules and configuration files.

---

# ⚙️ 14. Configuration and Secrets

Azure connection details and credentials are not hardcoded into the application.

Environment-specific values are stored through environment variables / `.env`.

Typical configuration includes:

```text
Event Hubs connection information
Storage account information
Azure SQL connection information
Spark configuration
Simulation parameters
Data-quality probabilities
```

### Important

The `.env` file should **never be committed to GitHub**.

Use a template such as:

```text
.env.example
```

containing variable names without real credentials.

Example:

```env
EVENTHUB_CONNECTION_STRING=<your-eventhub-connection-string>
STORAGE_ACCOUNT_KEY=<your-storage-key>
SQL_SERVER=<your-sql-server>
SQL_DATABASE=<your-database>
SQL_USERNAME=<your-username>
SQL_PASSWORD=<your-password>
```

---

# 🛠️ 15. Major Issues Encountered and Solutions

One of the main goals of this project was to understand the problems that occur when building an actual Azure Data Engineering pipeline.

The following issues were encountered during development.

---

## Issue 1 — Azure CLI Subscription Error

### Problem

The initial Storage Account creation attempt returned a `SubscriptionNotFound` error.

### Resolution

The Azure CLI authentication and subscription context were checked and corrected before continuing with resource deployment.

### Lesson

Azure CLI commands depend on the currently selected subscription and tenant context. Always verify:

```bash
az account show
```

before creating resources.

---

# Issue 2 — Event Hubs Retention Configuration

### Problem

An attempted Event Hub creation command used an unsupported/incorrect retention parameter.

Commands involving:

```text
--message-retention
```

and:

```text
--retention-time
```

resulted in CLI errors.

### Resolution

The Event Hub was created successfully using the supported configuration rather than forcing an invalid CLI parameter.

### Lesson

Azure CLI syntax can vary between resource types and CLI versions. Checking the supported command parameters is important rather than assuming flags from another Azure service.

---

# Issue 3 — Generator Class/Configuration Bugs

Several implementation issues appeared while building the clickstream simulator.

Examples included:

```text
Persona.__init__(name)
Missing PERSONA_WEIGHTS
Function objects being printed instead of results
create_new NameError
current_product/current_products initialization issues
INITIAL_USER_POOL import issue
```

### Resolution

The generator was progressively refactored into separate responsibilities:

```text
User Simulator
      ↓
Session Generator
      ↓
Event Generator
      ↓
Event Hubs Producer
```

This made the simulation logic easier to test and debug.

### Lesson

Separating simulation responsibilities is significantly easier to maintain than putting all user/session/event logic inside a single class.

---

# Issue 4 — Power BI Direct ADLS Authentication

### Problem

Direct Power BI access to ADLS created authentication/access complications with the personal Microsoft account used for the project.

### Resolution

Azure SQL Database was introduced as a dedicated serving layer:

```text
ADLS Gold
    ↓
Azure SQL
    ↓
Power BI
```

### Lesson

A data lake and a BI-serving layer have different responsibilities.

Introducing a relational serving layer also provided a clean interface for Power BI and made the final architecture more representative of enterprise data platforms.

---

# Issue 5 — Docker Compose Service vs Container Names

### Problem

The Spark Gold container was named:

```text
clickstream-gold
```

but its Docker Compose service name was:

```text
gold
```

Therefore:

```bash
docker compose logs clickstream-gold
```

failed because Docker Compose expects the service name.

### Correct command

```bash
docker compose logs gold
```

### Lesson

Docker container names and Compose service names are not necessarily the same.

---

# Issue 6 — Spark MemoryManager Warnings

During heavier processing, Spark produced warnings such as:

```text
Total allocation exceeds 95.00% of heap memory
Scaling row group sizes...
```

### Resolution

The warnings were monitored and confirmed to be Spark Parquet writer memory-management warnings rather than processing failures.

The streaming jobs continued processing successfully.

### Lesson

Not every `WARN` entry represents a pipeline failure. Logs need to be interpreted based on whether the query is actually terminating or producing incorrect output.

---

# Issue 7 — Spark State Store Warnings

The Gold streaming job produced messages such as:

```text
HDFSBackedStateStoreProvider
The state for version ... doesn't exist in loadedMaps
```

These occurred while the state store was loading/recovering state.

### Resolution

The pipeline continued successfully and the messages were identified as state-store recovery/loading messages rather than fatal failures.

### Lesson

Stateful Spark streaming applications require understanding of checkpoints and state-store behaviour before treating state-related warnings as errors.

---

# Issue 8 — Historical Data and Streaming Watermarks

After the initial dashboard was created, the dataset contained only a small number of distinct dates.

Simply generating additional events would increase volume but would not necessarily create useful historical dates.

There was also an important streaming consideration:

> Events significantly older than the current event-time watermark can be considered late and may not participate in downstream aggregations.

### Resolution

A dedicated historical load script was created:

```text
tests/historical_load_test.py
```

It generated sessions for subsequent dates and shifted the generated `event_time` values before sending the events through Event Hubs.

The events still passed through the normal streaming architecture.

### Lesson

When working with event-time streaming, data volume and event-time distribution are separate concerns.

A dashboard needs both sufficient volume and a useful temporal distribution.

---

# Issue 9 — Sparse Dashboard Time Series

### Problem

The first dashboard contained only a few event dates.

This made the Revenue Over Time and Purchases Over Time visuals less informative.

### Resolution

Additional historical synthetic data was generated across multiple dates:

```text
Sep 04
Sep 05
Sep 06
Sep 07
```

This created a more meaningful time series while preserving the actual pipeline flow.

---

# 🧠 16. Engineering Decisions

Several architectural decisions were made deliberately.

### Why Event Hubs?

It provides a managed Azure ingestion layer between the simulated application and Spark.

### Why Spark Structured Streaming?

Spark provides distributed stream processing and supports:

* Schema handling
* Transformations
* Stateful processing
* Event-time operations
* Windowed aggregations
* Checkpointing

### Why ADLS Gen2?

It provides scalable cloud storage and supports the Bronze/Silver/Gold data architecture.

### Why Bronze/Silver/Gold?

The layers separate:

```text
Raw Data
   ↓
Validated/Clean Data
   ↓
Business Data
```

This improves maintainability and makes debugging easier.

### Why Azure SQL?

Azure SQL provides a relational serving layer optimized for the project's Power BI consumption pattern.

### Why Docker?

Docker provides reproducibility for Spark and its dependencies without requiring every component to be installed directly on the host machine.

---

# 📈 17. Scalability Considerations

Although this is a portfolio project, the architecture was designed around patterns used in larger data platforms.

Potential scaling dimensions include:

### Event Hubs

Increase:

* Partitions
* Throughput capacity

### Spark

Increase:

* Executors
* Memory
* CPU
* Parallelism

### ADLS

Storage can scale independently from processing.

### SQL

The serving tier can be scaled depending on BI workload.

### Power BI

The semantic model and serving layer can be optimized independently from the streaming pipeline.

---

# 💰 18. Cost Considerations

This project was intentionally designed with personal-cloud cost in mind.

The architecture uses managed Azure services but avoids unnecessary services such as:

* Azure Synapse
* Azure Data Explorer
* Microsoft Fabric

The project was built using a relatively small Azure footprint.

For development:

* Event Hubs capacity was kept small.
* Storage volumes were intentionally limited.
* Azure SQL used a low-cost development tier.
* Dockerized Spark was executed locally.
* Services can be stopped/deleted after project completion.

This makes the project suitable for experimentation without maintaining a permanently running production environment.

---

# 🔐 19. Security Considerations

The project follows basic secret-management practices.

### Secrets

Credentials should be stored in:

```text
.env
```

and excluded using:

```text
.gitignore
```

### Never commit

Do not commit:

```text
.env
*.key
*.pem
connection strings
storage account keys
passwords
```

### Production improvements

A production deployment should replace local `.env` secrets with:

* Azure Key Vault
* Managed Identity
* Azure RBAC
* Secret rotation
* Private endpoints
* Network restrictions

---

# 📋 20. Validation Checklist

The project was validated across the complete pipeline.

```text
☑ Python event generation
☑ User/persona simulation
☑ Session generation
☑ Data-quality injection
☑ Azure Event Hubs ingestion
☑ Spark streaming ingestion
☑ Bronze layer
☑ Silver layer
☑ Quarantine handling
☑ Gold aggregations
☑ Spark checkpointing
☑ Historical load testing
☑ Azure SQL serving layer
☑ Power BI connectivity
☑ Power BI dashboard
☑ Dashboard filtering
☑ End-to-end data refresh
```

---

# 📸 21. Project Screenshots

The following screenshots document the implementation.

## Architecture

![High-Level Architecture](docs/architecture/high_level_architecture.png)

![Low-Level Architecture](docs/architecture/low_level_architecture.png)

---

## Source Generator

![Generator Running](docs/screenshots/02_generator_running.png)

---

## Azure Event Hubs

![Event Hubs Overview](docs/screenshots/03_eventhub_overview.png)

![Event Hubs Metrics](docs/screenshots/04_eventhub_metrics.png)

---

## ADLS Gen2

![ADLS Containers](docs/screenshots/05_adls_containers.png)

![Bronze Data](docs/screenshots/06_bronze_data.png)

![Silver and Quarantine](docs/screenshots/07_silver_quarantine.png)

![Gold Outputs](docs/screenshots/08_gold_outputs.png)

---

## Spark

![Spark Containers](docs/screenshots/09_spark_containers.png)

![Spark Processing](docs/screenshots/10_spark_processing.png)

---

## Azure SQL

![Azure SQL](docs/screenshots/11_azure_sql.png)

![SQL Results](docs/screenshots/12_sql_results.png)

---

## Power BI

![Final Power BI Dashboard](docs/screenshots/13_powerbi_dashboard.png)

![Power BI Dashboard Filters](docs/screenshots/14_powerbi_filters.png)

---

# 🚀 22. How to Run

## Prerequisites

The following are required:

* Python
* Docker Desktop
* Docker Compose
* Azure CLI
* Azure subscription
* Power BI Desktop
* Git

---

## Step 1 — Clone the repository

```bash
git clone <repository-url>
cd RealTime_Clickstream_Analytics
```

---

## Step 2 — Create Python environment

```bash
python -m venv .venv
```

Activate on Windows:

```bash
.venv\Scripts\activate
```

---

## Step 3 — Install Python dependencies

```bash
pip install -r requirements.txt
```

---

## Step 4 — Configure environment variables

Create:

```text
.env
```

from:

```text
.env.example
```

Populate the required Azure/Event Hubs/SQL configuration.

---

## Step 5 — Start Spark

```bash
docker compose up -d spark silver gold
```

Verify:

```bash
docker compose ps
```

---

## Step 6 — Start the clickstream producer

```bash
python run_producer.py
```

The generator will continuously create clickstream sessions and publish them to Event Hubs.

---

## Step 7 — Monitor Spark

```bash
docker compose logs --tail 50 silver
```

and:

```bash
docker compose logs --tail 50 gold
```

---

## Step 8 — Run load testing

```bash
python tests\load_test.py
```

For historical dashboard enrichment:

```bash
python tests\historical_load_test.py
```

---

## Step 9 — Prepare Power BI data

```bash
python streaming\prepare_powerbi.py
```

---

## Step 10 — Load Azure SQL

```bash
python streaming\load_sql.py
```

---

## Step 11 — Refresh Power BI

Open the Power BI report and refresh the dataset.

---

# 🔮 23. Future Improvements

The current implementation is intentionally scoped as an intermediate/portfolio project.

Possible production-grade extensions include:

### Infrastructure as Code

Introduce:

```text
Terraform / Bicep
```

for repeatable Azure deployment.

### CI/CD

Add:

```text
GitHub Actions
```

for:

* Unit testing
* Docker image builds
* Static analysis
* Deployment automation

### Secret Management

Move secrets from `.env` to:

```text
Azure Key Vault
Managed Identity
```

### Observability

Add:

* Azure Monitor
* Log Analytics
* Application Insights
* Data-quality metrics
* Pipeline health alerts

### Schema Management

Introduce:

* Schema Registry
* Schema versioning
* Backward compatibility checks

### Streaming Scale

Increase:

* Event Hub partitions
* Spark parallelism
* Event volume
* Concurrent users

### Advanced Analytics

Add:

* Sessionization
* Funnel analysis
* Customer segmentation
* Recommendation features
* Anomaly detection
* Real-time conversion monitoring

---

# 🧠 24. Key Learning Outcomes

This project provided hands-on experience with:

### Streaming Data Engineering

* Event-driven architectures
* Event Hubs
* Spark Structured Streaming
* Event-time processing
* Stateful streaming
* Checkpointing

### Cloud Data Engineering

* Azure resource deployment
* ADLS Gen2
* Azure Event Hubs
* Azure SQL
* Cloud authentication
* Azure CLI

### Data Architecture

* Bronze/Silver/Gold
* Data quality
* Quarantine patterns
* Business serving layers

### Distributed Processing

* Spark
* Dockerized Spark
* Stateful operations
* Streaming aggregations

### Analytics

* DAX
* Power BI
* KPI design
* Interactive dashboards
* Business-oriented analytical datasets

### Engineering Practices

* Environment-based configuration
* Secret separation
* Debugging
* Load testing
* Failure investigation
* Documentation

---

# 🏁 25. Conclusion

The **Real-Time E-Commerce Clickstream Analytics Platform** demonstrates a complete cloud-based Data Engineering workflow:

```text
        APPLICATION
             │
             ▼
      EVENT INGESTION
             │
             ▼
      STREAM PROCESSING
             │
             ▼
       DATA LAKEHOUSE
             │
             ▼
     BUSINESS AGGREGATION
             │
             ▼
       SERVING LAYER
             │
             ▼
      BUSINESS INTELLIGENCE
```

The project goes beyond a simple streaming demonstration by incorporating:

* Realistic user personas
* Session-based behaviour
* Data-quality problems
* Duplicate events
* Invalid records
* Event-time processing
* Streaming checkpoints
* Bronze/Silver/Gold architecture
* Quarantine handling
* Load testing
* Azure SQL serving
* Interactive Power BI analytics

The result is an end-to-end Data Engineering platform that demonstrates how raw user interactions can be transformed into actionable business intelligence using modern cloud and distributed-processing technologies.

---

# 👤 Author

**Abhinav**

Data Engineering Portfolio Project

Focus areas:

```text
Python
SQL
Apache Spark
Azure
Data Engineering
Streaming Analytics
Data Lakehouse
Power BI
```

---

## ⭐ Project Highlights

```text
✔ Real-time event ingestion
✔ Azure Event Hubs
✔ Spark Structured Streaming
✔ Dockerized processing
✔ ADLS Gen2 Bronze/Silver/Gold
✔ Data-quality and quarantine handling
✔ Event-time processing
✔ Checkpointing
✔ Azure SQL serving layer
✔ Power BI dashboard
✔ Load testing
✔ Production-oriented architecture
```

---
