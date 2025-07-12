# banks_project.py
# Code for ETL operations on Top 10 Largest Banks data

# ===== 1. Import Libraries =====
import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
import numpy as np
from datetime import datetime

# ===== 2. Logging Function =====
def log_progress(message):
    timestamp_format = '%Y-%m-%d %H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open("code_log.txt", "a") as f:
        f.write(f"{timestamp} : {message}\n")

# ===== 3. ETL Functions (to be completed) =====
def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''

    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")

    tables = pd.read_html(response.text)

    df_raw = tables[0]

    df = pd.DataFrame(columns=table_attribs)
    df["Name"] = df_raw["Bank name"]

    df["MC_USD_Billion"] = df_raw["Market cap (US$ billion)"].apply(
        lambda x: float(str(x).replace("\n", "").strip())
    )

    return df



def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''

    exchange_df = pd.read_csv(csv_path)

    exchange_rate = dict(zip(exchange_df['Currency'], exchange_df['Rate']))

    df['MC_GBP_Billion'] = [np.round(x * exchange_rate['GBP'], 2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x * exchange_rate['EUR'], 2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x * exchange_rate['INR'], 2) for x in df['MC_USD_Billion']]

    return df


def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing. '''
    df.to_csv(output_path, index=False)


def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing. '''
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)


def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    print(f"\nExecuting query: {query_statement}")
    cursor = sql_connection.cursor()
    cursor.execute(query_statement)
    results = cursor.fetchall()
    
    for row in results:
        print(row)



# Declare known variables
url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
exchange_rate_csv_path = "./exchange_rate.csv"
table_attribs = ["Name", "MC_USD_Billion"]
output_csv_path = "./Largest_banks_data.csv"
db_name = "Banks.db"
table_name = "Largest_banks"

log_progress("Preliminaries complete. Initiating ETL process")
df = extract(url, table_attribs)
log_progress("Data extraction complete. Initiating Transformation process")
df = transform(df, exchange_rate_csv_path)
log_progress("Data transformation complete. Initiating Loading process")
print(df.head(10))  
load_to_csv(df, output_csv_path)
log_progress("Data saved to CSV file")
conn = sqlite3.connect(db_name)
log_progress("SQL Connection initiated")
load_to_db(df, conn, table_name)
log_progress("Data loaded to Database as a table, Executing queries")

run_query("SELECT * FROM Largest_banks", conn)

run_query("SELECT AVG(MC_GBP_Billion) FROM Largest_banks", conn)

run_query("SELECT Name FROM Largest_banks LIMIT 5", conn)

log_progress("Process Complete")

conn.close()
log_progress("Server Connection closed")






