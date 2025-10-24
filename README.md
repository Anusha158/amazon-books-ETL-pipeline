# amazon-books-ETL-pipeline


## Overview
This project is an **ETL (Extract, Transform, Load) pipeline** that collects Amazon book data, transforms it into a structured format, and loads it into a PostgreSQL database for further analysis and visualization. The pipeline is orchestrated using **Apache Airflow**, with modular DAGs and reusable Python scripts.

---

## Features
- **Extract:** Scrapes book data from Amazon using APIs or web scraping techniques.
- **Transform:** Cleans and structures the data for analytics, handling missing values and formatting fields.
- **Load:** Stores the processed data in a PostgreSQL database.
- **Automation:** Scheduled and managed with Airflow DAGs.
- **Logging:** Maintains logs for each ETL run for easy monitoring and debugging.

---



# setup Airflow in Docker 
Follow steps in the link - https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html
