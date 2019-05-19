from crawler_templates.parallel_crawler import ParallelCrawler
from bs4 import BeautifulSoup
import csv
import datetime
import requests

obj = ParallelCrawler()
now = str(datetime.datetime.now())

exchanges = ['NASDAQ', 'NYSE']
field_names = ['Symbol', 'Name', 'LastSale', 'MarketCap', 'ADR TSO', 'IPOyear', 'Sector', 'Industry', 'Summary Quote',  'Rank', 'Style', 'Date']
writer = csv.DictWriter(open(f'data_{now}.csv', 'w'), fieldnames=field_names)
writer.writeheader()

for each_exchange in exchanges:
    print(f'Fetching exchange: {each_exchange}')
    print('*' * 25)
    url_template = 'https://www.nasdaq.com/screening/companies-by-industry.aspx?exchange={}&render=download'
    csv_data = requests.get(url_template.format(each_exchange)).content.decode('utf8').split('\r\n')

    reader = csv.DictReader(csv_data)
    c = 0
    # Filtering list
    new_reader = []
    for each_reader in reader:
        symb = each_reader.get('Symbol')
        if '^' in symb or '.' in symb:
            continue
        new_reader.append(each_reader)

    current = 0
    total = len(new_reader)

    def url_resolver(obj):
        symbol = obj.get('Symbol')
        url = 'https://www.zacks.com/stock/quote/{}'.format(symbol)
        return url

    def callback(row, source):
        global current
        current += 1
        print(f'Crawled {current}/{total} {row["Symbol"]}')
        data = {}
        try:
            soup = BeautifulSoup(source, "html.parser")
            ranks = soup.findAll('span', class_='rank_chip')
            style = soup.findAll('p', class_='rank_view')
            data = {
                'Rank': ranks[-1].text.strip() if ranks else None,
                'Style': style[1].text.replace('\xa0', '').replace('\n', '').replace(',', '').strip() if style and len(style) >= 1 else None,
            }
        except Exception as e:
            pass

        row['Rank'] = data.get('Rank')
        row['Style'] = data.get('Style')
        row['Date'] = now
        if '' in row:
            del row['']

        writer.writerow(row)

    obj.crawl(new_reader, url_resolver, callback)
