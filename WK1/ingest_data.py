import os
from time import time
import argparse
import pandas as pd
from sqlalchemy import create_engine

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    # Download the CSV file
    csv_name = "output.csv.gz"
    os.system(f"wget {url} -O {csv_name}")

    # Create a connection to the PostgreSQL database
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    # Read the CSV file in chunks
    df_iter = pd.read_csv(csv_name, compression='gzip', iterator=True, chunksize=100000)

    # Insert data till there's no data for next df_iter
    chunk_count = 0
    try:
        while True:
            t_start = time()

            df = next(df_iter)
            chunk_count += 1

            df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
            df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

            df.to_sql(name=table_name, con=engine, if_exists='append', index=False)  # <-- Fix here

            t_end = time()
            print(f'inserted chunk {chunk_count}, took %.3f seconds' % (t_end - t_start))
    except StopIteration:
        print(f"All chunks processed. Total chunks: {chunk_count}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to PostgreSQL.')
    parser.add_argument('--user', help='PostgreSQL username')
    parser.add_argument('--password', help='PostgreSQL password')
    parser.add_argument('--host', help='PostgreSQL host')
    parser.add_argument('--port', help='PostgreSQL port')
    parser.add_argument('--db', help='PostgreSQL database name')
    parser.add_argument('--table_name', help='PostgreSQL table name')
    parser.add_argument('--url', help='URL of the CSV file')

    args = parser.parse_args()
    main(args)





