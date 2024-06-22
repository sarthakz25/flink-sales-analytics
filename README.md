## Flink Sales Analytics

Real-time e-commerce sales analytics using Apache Flink, Kafka, PostgreSQL, and Elasticsearch.

### Overview

This project implements a real-time sales analytics system for e-commerce data. It processes sales transaction data from Kafka using Apache Flink, storing results in PostgreSQL and Elasticsearch for analysis and visualization.

### Architecture

1. Sales data streamed to Kafka
2. Apache Flink processes the data stream
3. Results stored in PostgreSQL and Elasticsearch
4. Kibana used for visualization

### Technologies

- Apache Flink
- Apache Kafka
- PostgreSQL
- Elasticsearch
- Kibana
- Docker

### Setup & Usage

1. Ensure Docker and Docker Compose are installed
2. Clone the repository
3. Set environment variables for database connection (JDBC_URL, DB_USERNAME, DB_PASSWORD)
4. Run `docker-compose up`
5. Execute the sales transaction generator
6. Run the Flink job

Note: To run Flink on Windows, use Cygwin.

### Data Processing

- Consumes transaction data from Kafka
- Performs transformations and aggregations
- Stores results in PostgreSQL and Elasticsearch