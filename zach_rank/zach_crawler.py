import csv
import datetime
import requests

from bs4 import BeautifulSoup
from crawler_templates.parallel_crawler import ParallelCrawler

field_names = ['ticker', 'company', 'marketCap', 'sectorName']
def get_stock_list():
    page_number = 1
    stock_list = []
    while page_number < 125:
        url_template = 'https://www.nasdaq.com/api/v1/screener?page={}&pageSize=50'
        stock_list += requests.get(url_template.format(page_number), timeout=None).json()['data']
        page_number += 1
        if stock_list == []:
            break
    return stock_list

stock_list = get_stock_list()
field_names += ['Rank', 'Style', 'Date']
now = str(datetime.datetime.now())

current = 0
total = len(stock_list)

writer = csv.DictWriter(open(f'data_{now}.csv', 'w'), fieldnames=field_names)
writer.writeheader()


def url_resolver(obj):
    symbol = obj.get('ticker')
    url = 'https://www.zacks.com/stock/quote/{}'.format(symbol)
    return url


def callback(row, source):
    global current
    current += 1
    print(f'Crawled {current}/{total} {row["ticker"]}')
    data = {}
    try:
        soup = BeautifulSoup(source, "html.parser")
        ranks = soup.findAll('span', class_='rank_chip')
        style = soup.findAll('p', class_='rank_view')
        data = {
            'Rank': ranks[-1].text.strip() if ranks else None,
            'Style': style[1].text.replace('\xa0', '').replace('\n', '').replace(',', '').strip() if style and len(
                style) >= 1 else None,
        }
    except Exception as e:
        pass

    row['Rank'] = data.get('Rank')
    row['Style'] = data.get('Style')
    row['Date'] = now
    output = {}
    for field in field_names:
        output[field] = row.get(field)
    writer.writerow(output)


parallel_crawler = ParallelCrawler()
parallel_crawler.crawl(stock_list, url_resolver, callback)
