import yfinance as yf
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import mysql.connector
from datetime import datetime, timedelta

def get_sp500_data():
    ticker = "^GSPC"
    start_date = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
    end_date = datetime.today().strftime('%Y-%m-%d')
    sp500_data = yf.download(ticker, start=start_date, end=end_date, interval="1h")
    if sp500_data.empty:
        return
    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="root",
        database="StockETL"
    )
    cursor = conn.cursor()
    sp500_df = pd.DataFrame(sp500_data)
    sp500_df.reset_index(inplace=True)
    for index, row in sp500_df.iterrows():
        cursor.execute("""
            INSERT INTO sp500 (Datetime, Close, High, Low, Open, Volume)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (row['Datetime'], row['Close'], row['High'], row['Low'], row['Open'], row['Volume']))
    conn.commit()
    cursor.close()
    conn.close()

dag = DAG(
    'historical_stock_data_collection',
    description='Fetch historical stock price data and save it to database',
    schedule='@daily',
    start_date=datetime(2025, 3, 22),
    catchup=False,
)

fetch_data_task = PythonOperator(
    task_id='fetch_daily_stock_data',
    python_callable=get_sp500_data,
    dag=dag
)
