import warnings
import requests
from bs4 import BeautifulSoup

warnings.filterwarnings("ignore", category=UserWarning, module='bs4')
HEADERS = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}


url_template = 'https://www.advfn.com/nasdaq/nasdaq.asp?companies={}'
file = open('stocks.txt', 'w+')
for code in range(ord('A'), ord('Z') + 1):
    url = url_template.format(chr(code))
    print("CRAWLING: ", chr(code))
    source = requests.get(url, headers=HEADERS).content
    soup = BeautifulSoup(source)

    table = soup.find('table', class_='market')
    rows = table.findAll('tr')
    rows = rows[2:]

    for each_row in rows:
        cells = each_row.findAll('td')
        name = cells[0].text.replace(',', ' ').strip()
        idx = cells[1].text
        file.write('{},{}\n'.format(name, idx))
