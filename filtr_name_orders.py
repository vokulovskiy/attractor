import psycopg2 as pg2
import pandas as pd
import re, sys
from keys.my_secret import my_secret as ms

# pip install psycopg2-binary
# pip install pandas

min_price = 50000

# Получение соединения с БД
def get_my_pg_connection():
    host = ms['host']
    port = ms['port']
    database = ms['database']
    user = ms['username']
    password = ms['password']
    return pg2.connect(host=host, port=port, database=database, user=user, password=password)

def search_keys(text, keys):
    '''
    Функция считает количество ключевых слов/фраз в тексте
    На входе - текст, список ключевых слов в виде списка фраз в формате regex
    На выходе - словарь с ключами и их количеством
    '''
    text =  text.lower().strip() # переводим в нижний регистр
    text = ' '.join(text.split()) # удаляем двойные пробелы
    cnt= {l[0]:len(l) for k in keys if len(l:=re.findall(k.lower(), text))>0}       
    return cnt

try:
    # пытаемся подключиться к базе данных
    conn = get_my_pg_connection()
    print('Connected!')
except:
    # в случае сбоя подключения будет выведено сообщение  в STDOUT
    print('Can`t establish connection to database')
    sys.exit()

conn.autocommit = True  # устанавливаем автокоммит

df_db = pd.read_sql("SELECT sk,title,price,fit FROM gz44_ord WHERE fit=0", conn)
df_keys = pd.read_csv('key_words.csv',delimiter=';')
# Negative keys
keys_n = df_keys[(df_keys.np==0)].key.to_list()
# Positive keys
keys_p = df_keys[(df_keys.np==1)].key.to_list()

with conn.cursor() as curs:
    for idx,row in df_db.iterrows():
        if row.price < min_price:
            row.fit = -1
        else:
            try:
                text =  row.title.lower().strip() # переводим в нижний регистр
                count_n = len(search_keys(text, keys_n)) 
                count_p = len(search_keys(text, keys_p)) 
                if count_p==0 and count_n > 0:
                    row.fit = -1
                else:
                    row.fit = 1
            except:
                row.fit = -1
        sql = f"UPDATE gz44_ord SET fit = {row.fit} WHERE sk = {row.sk};"
        curs.execute(sql)
conn.close()