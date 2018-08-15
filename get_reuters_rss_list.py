import requests as req
from bs4 import BeautifulSoup as bs

res = req.get('https://www.reuters.com/tools/rss')
soup = bs(res.content, 'lxml')

news = soup.findAll('table', {'class': 'dataTable'})[0]
rss_links = news.findAll('td', {'class': 'xmlLink'})

# TODO: see if any links have been changed periodically
