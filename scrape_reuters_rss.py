import os
import time

import feedparser
import pandas as pd
import numpy as np

# ignored feeds that seemed to have no companies/stocks in them
reuters_feed_list = {
                    'biz': 'http://feeds.reuters.com/reuters/businessNews',
                    'company': 'http://feeds.reuters.com/reuters/companyNews',
                    'health': 'http://feeds.reuters.com/reuters/healthNews',
                    'wealth': 'http://feeds.reuters.com/news/wealth',
                    'mostRead': 'http://feeds.reuters.com/reuters/MostRead',
                    'politics': 'http://feeds.reuters.com/Reuters/PoliticsNews',
                    'tech': 'http://feeds.reuters.com/reuters/technologyNews',
                    'top': 'http://feeds.reuters.com/reuters/topNews',
                    'US': 'http://feeds.reuters.com/Reuters/domesticNews',
                    'world': 'http://feeds.reuters.com/Reuters/worldNews'
                    }


def continually_scrape_rss():
    while True:
        all_feeds = []
        for l, f in reuters_feed_list.items():
            print(l)
            feeds = feedparser.parse(f)
            if feeds['status'] != 200:
                print('status is not good:', str(feeds['status']) + '; exiting')
                break

            for feed in feeds['entries']:
                feed['category'] = l
                # at least for reuters, it seems like 'tags' is just the same as category
                del feed['tags']
                # links seems to be the same as feedburner_origlink, at least for reuters
                del feed['links']

            all_feeds = all_feeds + [f for f in feeds['entries']]

        feeds_df = pd.io.json.json_normalize(all_feeds).dropna(axis=1)
        # no longer using time as index, now it's hash of the link
        # convert timestamp to time since epoch so hdf can use it as index
        # feeds_df['published_parsed'] = feeds_df['published_parsed'].apply(lambda x: int(time.mktime(x)))
        # feeds_df.set_index('published_parsed', inplace=True)
        # feeds_df.index = feeds_df['link'].apply(lambda x: int(hashlib.md5(x.encode('utf-8')).hexdigest(), 16))
        # feeds_df.reset_index(inplace=True)

        filename = 'reuters_raw_rss.h5'
        if os.path.exists(filename):
            # seems that only 20 stories can be returned from any feed max, so 10x20 = 200
            # but use feeds_df shape for more robust performance
            current_df = pd.read_hdf(filename, start=-feeds_df.shape[0])
            current_df.reset_index(inplace=True, drop=True)
            mode = 'r+'
            # check for new stuff
            df_all = feeds_df.merge(current_df, how='left', indicator=True)
            new_entries_idx = df_all['_merge'] == 'left_only'
            new_entries = new_entries_idx.sum()
            print('\n')
            if new_entries != 0:
                new_df = df_all.loc[new_entries_idx]
                new_df.drop('_merge', axis=1, inplace=True)
                new_df.to_hdf(filename, 'raw_rss', mode='r+')
                print(str(new_entries), 'updates')
            else:
                print('no updates')

            print('\n')
        else:
            feeds_df.to_hdf(filename, 'raw_rss', mode='w')

        print('finished, waiting one minute...\n\n')
        time.sleep(60)


if __name__ == "__main__":
    continually_scrape_rss()

# to convert timezone
"""
from time import mktime
from datetime import datetime

dt = datetime.fromtimestamp(mktime(feeds['entries'][0]['published_parsed']))
import pytz    # $ pip install pytz
import tzlocal # $ pip install tzlocal

local_timezone = tzlocal.get_localzone() # get pytz tzinfo
local_time = dt.replace(tzinfo=pytz.utc).astimezone(local_timezone)
"""


# TODO: scrape hot stocks, archive, maybe Rates archive
# https://www.reuters.com/news/archive/hotStocksNews
# https://www.reuters.com/news/archive
# https://www.reuters.com/news/archive/rates-rss?view=page&page=6&pageSize=10
