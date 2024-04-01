from fake_useragent import UserAgent
from bs4 import BeautifulSoup as bs
import requests as req
import re

# pip install fake_useragent
# pip install requests
# pip install bs4

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

def get_ord_docs(url):

    """
    Получение данных о закупке по ее URL
    
    Входные данные: URL закупки
    URL имеет вид:
    https://zakupki.gov.ru/epz/order/notice/ok20/view/common-info.html?regNumber=0156200009923001178
    
    Выходные данные: словарь 'имя документа':'url'

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

sp_doc = get_ord_data('https://zakupki.gov.ru/epz/order/notice/ok20/view/common-info.html?regNumber=0156200009923001178')
print(sp_doc)