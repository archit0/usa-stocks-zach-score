import csv
import requests
import datetime

from bs4 import BeautifulSoup
from multiprocessing import Pool, cpu_count
from concurrent.futures import ThreadPoolExecutor as PoolExecutor
import atexit

output = []
def exit_handler():
    print('*' * 100)
    print('Writing to file')
    writer.writerows(output)
    print('*' * 100)

atexit.register(exit_handler)

cpu = cpu_count()
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'
}
now = str(datetime.datetime.now())



def process(row):
    symbol = row.get('Symbol')

    def get_zachs_data(idx):
        try:
            url = 'https://www.zacks.com/stock/quote/{}'.format(idx)
            source = requests.get(url, headers=HEADERS, allow_redirects=False).content
            soup = BeautifulSoup(source, "html.parser")
            ranks = soup.findAll('span', class_='rank_chip')
            style = soup.findAll('p', class_='rank_view')
            return {
                'Rank': ranks[-1].text.strip() if ranks else None,
                'Style': style[1].text.replace('\xa0', '').replace('\n', '').replace(',', '').strip() if style and len(style) >= 1 else None,
            }
        except Exception as e:
            return {}

    data = get_zachs_data(symbol)
    row['Rank'] = data.get('Rank')
    row['Style'] = data.get('Style')
    row['Date'] = now
    if '' in row:
        del row['']
    output.append(row)

    return row




mapping = ['NASDAQ', 'NYSE']
field_names = ['Symbol', 'Name', 'LastSale', 'MarketCap', 'ADR TSO', 'IPOyear', 'Sector', 'Industry', 'Summary Quote',  'Rank', 'Style', 'Date']
writer = csv.DictWriter(open(f'data_{now}.csv', 'w'), fieldnames=field_names)
writer.writeheader()

for each_stock in mapping:
    print(f'Fetching exchange: {each_stock}')
    print('*' * 25)
    url_template = 'https://www.nasdaq.com/screening/companies-by-industry.aspx?exchange={}&render=download'
    csv_data = requests.get(url_template.format(each_stock)).content.decode('utf8').split('\r\n')

    reader = csv.DictReader(csv_data)
    c = 0
    # Filtering list
    new_reader = []
    for each_reader in reader:
        symb = each_reader.get('Symbol')
        if '^' in symb or '.' in symb:
            continue
        new_reader.append(each_reader)
    row = None
    try:
        total = 0
        readers = []


        with PoolExecutor(max_workers=cpu * 2) as executor:
            for ar in executor.map(process, [row for row in new_reader]):
                total += 1
                print(f'Total Processed: {total}/{len(new_reader)}- Symbol {ar.get("Symbol")}\t\tRank:{ar.get("Rank")}\t\tStyle:{ar.get("Style")}')

    except Exception as e:
        pass



