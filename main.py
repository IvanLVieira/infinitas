import requests
import pandas as pd
import json
import sqlite3
import argparse

conn= sqlite3.connect("infinitas.db")
#C:\Users\ivieira\PycharmProjects\infinitas

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

api_url = "https://randomuser.me/api/?results=10"
response = requests.get(api_url)
data = response.json()
df = pd.json_normalize(data['results'])

df.to_sql('customer2', conn, if_exists='replace')

print(df)
gender = 'female'
query_add = ' where gender='
db_df = pd.read_sql_query("SELECT * FROM customer2" + query_add + '\'' + gender + '\'', conn)
db_df.to_csv('database.csv', index=False)

print(db_df)

