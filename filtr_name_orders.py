import psycopg2
import pandas as pd
import re

# pip install psycopg2-binary
# poetry add psycopg2-binary

min_price = 100000
usr = 'vokulovskiy'
pwd = 'OsPl27FLJC7c8N6t'
host = '212.24.35.176'
port = 10573

try:
    # пытаемся подключиться к базе данных
    conn = psycopg2.connect(dbname='attractor-raw', user=usr, password=pwd, host=host, port=port)
    print('Connected!')
except:
    # в случае сбоя подключения будет выведено сообщение  в STDOUT
    print('Can`t establish connection to database')

conn.autocommit = True  # устанавливаем актокоммит

df_db = pd.read_sql("SELECT sk,title,price,fit FROM gz44_ord WHERE fit=0 LIMIT 10", conn)

with conn.cursor() as curs:
    for idx,row in df_db.iterrows():
        if row.price < min_price:
            row.fit = -1
            print(row.fit, row.price, row.title)
            sql = f"UPDATE gz44_ord SET fit = {row.fit} / 2 WHERE sk = {row.sk};"
            curs.execute(sql)
print('='*100)
df = pd.read_sql("SELECT sk,title,price,fit FROM gz44_ord WHERE fit!=0", conn)
print(df)
conn.close()