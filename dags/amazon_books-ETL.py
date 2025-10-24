import logging
from regex import search
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import requests
import pandas as pd
import os
import json


# User-Agent to avoid request blocking
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/58.0.3029.110 Safari/537.3"
}

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}
def check_internet():
    try:
        res = requests.get("https://www.google.com", timeout=5)
        print("Status Code:", res.status_code)
        if res.status_code == 200:
            print("✅ Internet access is working inside Airflow")
        else:
            print("⚠️ Received non-200 response, may have limited access")
    except Exception as e:
        print("❌ No internet access:", e)
with DAG(
    dag_id="amazon_books_ETL_v1",
    description="ETL DAG for Amazon Books Data",
    schedule=timedelta(days=1),
    start_date=datetime(2025, 6, 10, 2),
    catchup=False,
    default_args=default_args
) as dag:
    
    # ---------------------- 1️⃣ EXTRACT ----------------------
    def get_amazon_data_books(num_books, ti):
        SERPAPI_KEY=os.getenv("serpapi_api_key")
        #SERPAPI_KEY = "f4b6cc426481606019eb6133e189a9a4a8b7ca058bb3d378b9a03980603ae8ee"
        print("Using SerpApi key:", SERPAPI_KEY)

        if not SERPAPI_KEY:
            raise ValueError("SerpApi key not found in environment variables")

        params = {
            "key": SERPAPI_KEY,
            "engine": "amazon",
            "k": "data engineering books",
            "maxResults": 40
        }

        books = []
        seen_titles = set()

        # Single API call (pagination requires another param)
        print("Requesting:", "https://serpapi.com/search", params)
        res = requests.get("https://serpapi.com/search", params=params)

        if res.status_code != 200:
            print("Failed to retrieve data:", res.status_code)
            return
        logging.info(f"Raw API Response: {res.json()}")
        data = res.json()
        print(data.keys())
        book_containers = data.get("organic_results", [])
        print(f"Found {len(book_containers)} book entries")
        for book in book_containers:
            title = book.get("title")
            author = book.get("author")
            price = book.get("price")
            rating = book.get("rating")

            # if title and author and price and rating:
            #     title_clean = title.strip()
            #     if title_clean not in seen_titles:
            #         seen_titles.add(title_clean)
            books.append({
                        "Title": title,
                        "Author": author,
                        "Price": price,
                        "Rating": str(rating),
                    })

            if len(books) >= num_books:
                break

        df = pd.DataFrame(books)
        df.drop_duplicates(subset="Title", inplace=True)

        print(f"Extracted {len(df)} books")
        print(df.head())

        safe_data = json.loads(df.to_json(orient='records'))
        ti.xcom_push(key='book_data', value=safe_data)
  # ---------------------- 2️⃣ CLEAN ----------------------
    def clean_data(ti):
        raw_data = ti.xcom_pull(task_ids="extract_data_task", key='book_data')
        cleaned_data = []

        for record in raw_data:
            price = record["Price"].replace("$", "").replace(",", "")
            try:
                price = float(price)
            except:
                price = None

            cleaned_data.append({
                "Title": record["Title"],
                "Author": record["Author"],
                "Price": price,
                "Rating": record["Rating"]
            })

        ti.xcom_push(key="cleaned_data", value=cleaned_data)

    # ---------------------- 3️⃣ LOAD ----------------------
    def load_data(ti):
        cleaned_data = ti.xcom_pull(task_ids="clean_data_task", key="cleaned_data")
        pg_hook = PostgresHook(postgres_conn_id="amazon_books_connection")

        insert_query = """
        INSERT INTO amazon_books (title, author, price, rating)
        VALUES (%s, %s, %s, %s);
        """

        for record in cleaned_data:
            pg_hook.run(insert_query, parameters=(
                record["Title"], record["Author"], record["Price"], record["Rating"]
            ))

    # ---------------------- 4️⃣ TASKS ----------------------
    check_internet_task = PythonOperator(
        task_id="check_internet_task",
        python_callable=check_internet
    )
    extract_task = PythonOperator(
        task_id="extract_data_task",
        python_callable=get_amazon_data_books,
        op_kwargs={"num_books": 40}
    )

    create_table_task = SQLExecuteQueryOperator(
        task_id="create_table_task",
        conn_id="amazon_books_connection",
        sql="""
        CREATE TABLE IF NOT EXISTS amazon_books (
            id SERIAL PRIMARY KEY,
            title VARCHAR(255),
            author VARCHAR(255),
            price NUMERIC,
            rating VARCHAR(50)
        );
        """
    )

    clean_task = PythonOperator(
        task_id="clean_data_task",
        python_callable=clean_data
    )

    load_task = PythonOperator(
        task_id="load_data_task",
        python_callable=load_data
    )

    # ---------------------- 5️⃣ DEPENDENCIES ----------------------
    check_internet_task >> extract_task >> create_table_task >> clean_task >> load_task
