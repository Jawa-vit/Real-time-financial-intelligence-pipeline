# 🚀 Real-Time Financial Intelligence Data Pipeline

<p align="center">

![Python](https://img.shields.io/badge/Python-3.11-blue?style=for-the-badge\&logo=python)
![PySpark](https://img.shields.io/badge/PySpark-3.5-orange?style=for-the-badge\&logo=apachespark)
![Kafka](https://img.shields.io/badge/Kafka-Streaming-black?style=for-the-badge\&logo=apachekafka)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-Database-blue?style=for-the-badge\&logo=postgresql)
![PowerBI](https://img.shields.io/badge/PowerBI-Dashboard-yellow?style=for-the-badge\&logo=powerbi)
![Databricks](https://img.shields.io/badge/Databricks-Lakehouse-red?style=for-the-badge\&logo=databricks)
![Azure](https://img.shields.io/badge/Azure-Cloud-blue?style=for-the-badge\&logo=microsoftazure)

</p>

---

## 📌 Project Overview

An end-to-end data engineering pipeline that streams live stock quotes through Kafka, processes them with PySpark, lands them in a medallion-architecture lakehouse on Azure Data Lake Storage via Databricks, orchestrates the nightly batch with Azure Data Factory, serves the results from PostgreSQL, and visualizes everything in Power BI.

---

## 🏗️ High-Level Architecture

```mermaid
flowchart LR

A[📈 Finance APIs]
--> B[⚡ Kafka]

B --> C[🔥 PySpark Streaming]

C --> D[🥉 Bronze Layer]
D --> E[🥈 Silver Layer]
E --> F[🥇 Gold Layer]

subgraph Databricks Lakehouse
D
E
F
end

F --> G[🔄 Azure Data Factory]

G --> H[(🐘 PostgreSQL)]

H --> I[📊 Power BI Dashboard]
```

---

## 🔄 End-to-End Data Flow

```text
Finance APIs
      │
      ▼
   Kafka
      │
      ▼
PySpark Streaming
      │
      ▼
 Databricks Lakehouse
 ┌─────────────────┐
 │ Bronze Layer    │
 │ Silver Layer    │
 │ Gold Layer      │
 └─────────────────┘
      │
      ▼
Azure Data Factory
      │
      ▼
 PostgreSQL
      │
      ▼
   Power BI
```

---

## 🏆 Project Highlights

✅ Real-Time Streaming Architecture

✅ Medallion Lakehouse Architecture

✅ PySpark Structured Streaming

✅ Azure Data Factory Orchestration

✅ PostgreSQL Analytics Warehouse

✅ Power BI Executive Dashboards

✅ Dockerized Deployment

✅ CI/CD with GitHub Actions

---

## 🛠️ Technology Stack

| Layer         | Technologies            |
| ------------- | ----------------------- |
| Data Source   | Finance APIs            |
| Streaming     | Apache Kafka            |
| Processing    | PySpark                 |
| Lakehouse     | Databricks              |
| Storage       | Azure Data Lake Storage |
| Orchestration | Azure Data Factory      |
| Database      | PostgreSQL              |
| Visualization | Power BI                |
| DevOps        | Docker, GitHub Actions  |

---

## 📂 Repository Structure

```text
financial-intelligence-pipeline
│
├── common/
├── data_ingestion/
├── kafka/
├── spark_processing/
├── databricks/
├── azure/
├── postgres/
├── powerbi/
├── tests/
├── .github/workflows/
└── docker-compose.yml
```

---

## 🥉 Bronze Layer

Raw stock market events directly ingested from Kafka.

### Sample Fields

* symbol
* timestamp
* open
* high
* low
* close
* volume

---

## 🥈 Silver Layer

Validated and transformed market data.

### Operations

* Data Cleaning
* Type Casting
* Schema Validation
* Missing Value Handling

---

## 🥇 Gold Layer

Business-ready analytical datasets.

### Metrics Generated

* Average Price
* Total Volume
* Daily Return (% Change)
* Volatility
* RSI-14 Indicator

---

## 📊 Dashboard Preview

### Executive Summary Dashboard

```text
✔ Total Stocks Tracked
✔ Average Stock Price
✔ Total Trading Volume
✔ Daily Performance
```

### Stock Performance Dashboard

```text
✔ Price Trends
✔ Volume Trends
✔ Volatility Analysis
✔ Percentage Change Analysis
```

---

## 🚀 Local Deployment Flow

```mermaid
flowchart TD

A[docker compose up]
--> B[Kafka Running]

B --> C[Producer Streams Data]

C --> D[PySpark Processing]

D --> E[Gold Parquet]

E --> F[PostgreSQL]

F --> G[Power BI Dashboard]
```

---

## ☁️ Azure Deployment Flow

```mermaid
flowchart LR

A[Finance APIs]
--> B[Kafka/Event Hub]

B --> C[Databricks]

C --> D[Azure Data Lake]

D --> E[Azure Data Factory]

E --> F[Azure PostgreSQL]

F --> G[Power BI Service]
```

---

## 🧪 Testing

```bash
pytest tests/ -v
```

### Coverage

* SMA Calculation
* Volatility
* RSI-14
* Percentage Change
* Kafka Schema Validation

---

## 🎯 Resume-Worthy Skills Demonstrated

* Data Engineering
* Real-Time Streaming
* ETL/ELT Pipelines
* Lakehouse Architecture
* Cloud Data Platforms
* Data Warehousing
* Dashboard Development
* DevOps & CI/CD

---

## 🔮 Future Enhancements

* Historical Market Backfill
* Airflow Integration
* FastAPI Analytics Layer
* Real-Time Alerts
* Machine Learning Forecasting
* Event Hub Integration
* AKS Deployment

---

## 👨‍💻 Author

### Jawagar K R

B.Tech – Computer Science and Business Systems

VIT-AP University

📧 [jawagarkrsoft@gmail.com](mailto:jawagarkrsoft@gmail.com)

🔗 GitHub: https://github.com/Jawa-vit

⭐ If you found this project useful, consider giving it a star!
