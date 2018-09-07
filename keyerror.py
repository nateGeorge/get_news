finished, waiting one minute...


biz
---------------------------------------------------------------------------
KeyError                                  Traceback (most recent call last)
~/github/get_news/scrape_reuters_rss.py in <module>
    417
    418 if __name__ == "__main__":
--> 419     continually_scrape_rss()
    420
    421 # to convert timezone

~/github/get_news/scrape_reuters_rss.py in continually_scrape_rss()
     77             print(l)
     78             feeds = feedparser.parse(f)
---> 79             if feeds['status'] != 200:
     80                 print('status is not good:', str(feeds['status']) + '; exiting')
     81                 break

/usr/lib/python3/dist-packages/feedparser.py in __getitem__(self, key)
    381             elif dict.__contains__(self, realkey):
    382                 return dict.__getitem__(self, realkey)
--> 383         return dict.__getitem__(self, key)
    384
    385     def __contains__(self, key):

KeyError: 'status'
