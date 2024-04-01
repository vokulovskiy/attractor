import os, shutil, sys, re
import aspose.zip as az
import aspose.words as aw
import pdfplumber
import pandas as pd
from fake_useragent import UserAgent
from bs4 import BeautifulSoup as bs
import requests as req
import psycopg2 as pg2
from keys.my_secret import my_secret as ms

# pip install aspose-zip
# pip install aspose-words
# pip install pdfplumber 
# pip install openpyxl
# pip install xlrd
# pip install pandas
# pip install fake_useragent
# pip install requests
# pip install bs4
# pip install psycopg2-binary
# pip3 install wget

# Ошибка если схема поменялась
# Логирование
def log_error(msg): print(msg)

def schema_error_(what = '', where = 0):
    msg = 'Ошибка схемы страницы {} при парсинге URL {}'.format(where, what)
    log_error(msg)
    raise RuntimeError(msg)

# Ошибка соединения
def conn_error_(what = '', where = 0): 
    msg = 'Ошибка соединения {} при парсинге URL {}'.format(where, what)
    log_error(msg)
    raise RuntimeError(msg)

def get_doc(path_in_file, url):
    '''
    Скачивает документ по URL
    '''
    headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 YaBrowser/23.11.0.0 Safari/537.36'
        }
    res = req.get(url, headers = headers, timeout = 30)
    with open(path_in_file, 'wb') as f:
        f.write(res.content)
    
def get_ord_docs(url):

    """
    Получение ссылок на документы из закупки по ее URL
    
    Входные данные: URL закупки
    URL имеет вид:
    https://zakupki.gov.ru/epz/order/notice/ok20/view/common-info.html?regNumber=0156200009923001178
    
    Выходные данные: словарь 'имя документа':'url документа'

    """
    def schema_error(where=0): schema_error_(ord_url, 'get_ord_data ' + str(where))
    def conn_error(where=0): conn_error_(ord_url, 'get_ord_data ' + str(where))

    ord_data = {}
    # Получаем ссылку на страницу с документами
    ord_url = url.replace('common-info','documents')

    ua = UserAgent()
    headers = {
        'accept': 'text/html', 
        'user-agent': ua.firefox
    }
   
    try: response = req.get(ord_url, timeout=120, headers=headers)
    except: conn_error(10)
    if response.status_code != 200: conn_error(20)

    with open('temp.txt','w') as f:
        print(response.text,file=f)
        
    soup = bs(response.text, 'html.parser')

    ord_data = {doc.find('a')['title']:doc.find('a')['href'] for doc in soup.findAll('span', class_='section__value')}
    return ord_data

def doc2txt(path):
    doc = aw.Document(path).get_text()
    s = re.sub('[\x07\x0b\x13HYPERLINK]', ' ', doc).split('\r')
    return '\n'.join([i.strip() for i in s if len(i.strip())>0][1:-1]) 

def pdf2txt(fname):
    with pdfplumber.open(fname) as pdf:
        if len(pdf.pages):
            text_o = ' '.join([
                page.extract_text() or '' for page in pdf.pages if page
            ])
    return text_o

def xls2txt(fname):
    df = pd.read_excel(fname)
    df.fillna('', inplace=True)
    return df.to_string(index=False)

def zip2dir(fname):
    # Загрузить архив
    with az.Archive(fname) as archive:
	    # Извлечь файл 
        fn = os.path.basename(fname)
        archive.extract_to_directory(os.path.join(path_temp, fn))

def rar2dir(fname):
    # Загрузить архив
    with az.rar.RarArchive(fname) as archive:
        # Извлечь файл 
        fn = os.path.basename(fname)
        archive.extract_to_directory(os.path.join(path_temp, fn))

def to_txt(path):
    txt_from_files = {}
    for root, dirs, files in os.walk(path):
        #print(f'{root=}, {dirs=}, {files=}')
        for fn in files:
            ext = fn.split('.')[-1]
            ffn = os.path.join(root, fn)
            if ext in ['doc','docx']:
                txt_from_files[ffn] = doc2txt(ffn)
            elif ext=='txt':
                with open(ffn,'r') as f:
                    txt_from_files[ffn] = f.read()
            elif ext in ['xls', 'xlsx']:
                txt_from_files[ffn] = xls2txt(ffn)
            elif ext=='pdf':
                txt_from_files[ffn] = pdf2txt(ffn)
            elif ext=='rar':
                rar2dir(ffn)
            elif ext=='zip':
                zip2dir(ffn)
    return txt_from_files

def clear_temp(path_temp):# Очистка временной папки
    if os.path.exists(path_temp): 
        shutil.rmtree(path_temp)
    os.mkdir(path_temp)

def find_nearest_file(texts, keywords):
    '''
    На вход подается словарь вида имя файла: текст и список ключевых фраз в формате re
    Функция ищет по ключевым фразам в тексте наиболее близкий по содержанию файл
    На выходе имя найденного файла 
    '''
    closest_file = None
    max_count = 0
    for filename, text in texts.items():
        if len(text)>0: 
            if sum([len(re.findall (k, os.path.basename(filename).lower())) for k in keywords]): # Если найдены совпадения в названии файла
                return filename
            else:
                text = re.sub("[^а-я\n ]", "", text.lower()).strip() # оставляем только русские буквы и перевод строки
                cnt = sum([len(re.findall (k, text)) for k in keywords])
                distance = 0
                text = text.split('\n')
                if cnt: # если найдено хоть одно совпадение
                    for i,s in enumerate(text): # то ищем номера строк в которых эти ключи найдены
                        cnt = sum([len(re.findall (k, s)) for k in keywords])
                        if cnt:
                            distance += cnt/(i+1) # вычисляем дистанцию, чем дальше фраза от начала документа, тем меньше ее вес
                    if distance > max_count:
                        max_count = distance
                        closest_file = filename
                print(f'{distance=:0.3f}, {filename=}')
                    
    return closest_file

# Получение соединения с БД
def get_my_pg_connection():
    host = ms['host']
    port = ms['port']
    database = ms['database']
    user = ms['username']
    password = ms['password']
    return pg2.connect(host=host, port=port, database=database, user=user, password=password)

# ==================================================================
path_root = r'/mnt/c/temp/'

path_temp = "temp"
keywords = [r"\bтехническ.. задан*",  "описание объекта закупки"]

try:
    # пытаемся подключиться к базе данных
    conn = get_my_pg_connection()
    print('Connected!')
except:
    # в случае сбоя подключения будет выведено сообщение  в STDOUT
    print('Can`t establish connection to database')
    sys.exit()

conn.autocommit = True  # устанавливаем актокоммит

df_db = pd.read_sql("SELECT sk,url,fit FROM gz44_ord WHERE (fit>0 and fit<3)", conn)
df_keys = pd.read_csv('key_words.csv',delimiter=';')
# Negative keys
keys_n = df_keys[(df_keys.np==0)].key.to_list()
# Positive keys
keys_p = df_keys[(df_keys.np==1)].key.to_list()

with conn.cursor() as curs:
    for idx,row in df_db.iterrows():
        ord_docs = get_ord_docs(row.url)
        clear_temp(path_temp)
        txt = ''
        detected = False
        print(row.sk)
        # Проверяем ТЗ в названии файла
        for fname, url in ord_docs.items():
            if sum([len(re.findall (k, os.path.basename(fname).lower())) for k in keywords]) > 0:
                get_doc(f'{path_temp}/{fname}', url)
                txt = to_txt(f'{path_temp}')
                txt = txt[next(iter(txt))]
                detected = True
                print(f'Техническое задание найдено в названии файла')
                break
        # Ищем ТЗ внутри файлов
        clear_temp(path_temp)
        if not detected:
            # Скачиваем все документы из закупки
            for fname, url in ord_docs.items():
                path = f'{path_temp}/{fname}'
                get_doc(path, url)
            txt_from_files = to_txt(path_temp)
            #clear_temp(path_temp)
            file_tz = find_nearest_file(txt_from_files, keywords)
            if file_tz:
                print(f'Техническое задание из каталога {path} с наибольшей вероятностью содержится в файле: {file_tz}')
                print(txt_from_files[file_tz][:200])
                txt = txt_from_files[file_tz]
                detected = True
            else:
                print(f'Техническое задание из каталога {path} не найдено')
            print()
        if detected and len(txt.strip())>0:
            row.fit=3
            sql = f"INSERT INTO long_strings (gz44_ord_sk, tz) VALUES ({row.sk}, '{txt}');"
            curs.execute(sql)
        else:
            row.fit=-3
        sql = f"UPDATE gz44_ord SET fit = {row.fit} WHERE sk = {row.sk};"
        curs.execute(sql)
