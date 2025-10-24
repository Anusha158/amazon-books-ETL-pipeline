# Use the base Airflow image version specified in your docker-compose.yaml (3.1.0 in your case)
FROM apache/airflow:3.1.0

# Install the Postgres provider package
RUN pip install apache-airflow-providers-postgres