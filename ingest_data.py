#!/usr/bin/env python
import os
import pandas as pd
import sqlalchemy 
import argparse
import requests
from sqlalchemy import create_engine

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    file_name = "data.parquet"

    print("📥 Downloading file...")
    response = requests.get(url)
    with open(file_name, "wb") as f:
        f.write(response.content)

    print("📄 Reading parquet file...")
    df = pd.read_parquet(file_name)

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    print("🧹 Converting datetime columns...")
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    print("🟢 Inserting into database...")
    df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)

    print("✅ Data upload completed!")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest Parquet data to Postgres')
    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host name for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='table name for postgres')
    parser.add_argument('--url', help='URL to parquet file')

    args = parser.parse_args()
    main(args)
