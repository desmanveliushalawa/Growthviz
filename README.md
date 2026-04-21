# Growthviz — End-to-End E-Commerce Data Platform

Pipeline data e-commerce otomatis dari event streaming hingga dashboard analitik.

## Stack

- **Kafka** — real-time event streaming
- **Airflow** — pipeline orchestration (jadwal harian)
- **Spark** — batch data processing
- **dbt** — data modeling & transformation
- **PostgreSQL** — data warehouse
- **Metabase** — business intelligence dashboard

## Output

Dashboard otomatis update setiap hari menampilkan:

- Total revenue harian
- Produk terlaris
- Status order (confirmed, shipped, pending)
