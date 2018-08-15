"""
postgresql:

sudo -u postgres psql
list dbs: \l
use a db: \c dbname
list tables: \d
quit: \q

"""

import os
import time
from datetime import datetime

import requests as req
from bs4 import BeautifulSoup as bs
import feedparser
import pandas as pd
import numpy as np
from sqlalchemy import create_engine

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
    # tried with sqlite first, but having trouble
    # 'sqlite://'
    # db_filename = '/home/nate/rss_feeds.db'
    # db = 'sqlite://' + db_filename

    # tricks for postgres:
    # find databases:
    # https://stackoverflow.com/a/8237512/4549682
    # run psql: https://www.digitalocean.com/community/tutorials/how-to-install-and-use-postgresql-on-ubuntu-16-04
    # create tables, users, etc: https://medium.com/coding-blocks/creating-user-database-and-adding-access-on-postgresql-8bfcd2f4a91e
    postgres_uname = os.environ.get('postgres_uname')
    postgres_pass = os.environ.get('postgres_pass')
    db = 'postgresql://{}:{}@localhost:5432/rss_feeds'.format(postgres_uname, postgres_pass)
    engine = create_engine(db, echo=False)
    # create test table for sanity check
    # engine.execute("CREATE TABLE IF NOT EXISTS test();")
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
        # convert time to seconds since epoch
        feeds_df['published_parsed'] = feeds_df['published_parsed'].apply(lambda x: datetime.fromtimestamp((time.mktime(x))))
        feeds_df['time_added'] = datetime.utcnow()

        tablename = 'reuters_raw_rss'
        # check if table exists
        # sqlite way
        # stmt = "select count(*) from sqlite_master where type='table' and name='{}'".format(tablename)
        # stmt = 'select exists(select * from information_schema.tables where table_name={})'.format(tablename)
        stmt = "select exists(select relname from pg_class where relname='" + tablename + "')"
        res = engine.execute(stmt)
        table_exists = res.fetchone()[0]
        if table_exists:
            # need to grab quite a few stories just to make sure we aren't getting any dupes, since some categories may update more frequently than others
            # ideally would grab the 20 most recent for each category
            cur_df = pd.read_sql('SELECT * FROM {} ORDER BY time_added DESC LIMIT '.format(tablename) + '3000', con=engine)
            # check for new stuff
            cols = feeds_df.columns
            no_time_added_cols = [c for c in cols if c != 'time_added']
            # ignore time added column
            feeds_df_no_time_added = feeds_df[no_time_added_cols]
            cur_df_no_time_added = cur_df[no_time_added_cols]
            df_all = feeds_df_no_time_added.merge(cur_df_no_time_added.drop_duplicates(), on=list(['feedburner_origlink', 'id']), how='left', indicator=True)
            new_entries_idx = df_all['_merge'] == 'left_only'
            new_entries = new_entries_idx.sum()
            print('\n')
            if new_entries != 0:
                df_all_new = df_all.loc[new_entries_idx]
                new_links = df_all_new['feedburner_origlink']
                new_stories = feeds_df[feeds_df['feedburner_origlink'].isin(new_links)]
                new_stories.to_sql(tablename, con=engine, if_exists='append', index=False)
                print(str(new_entries), 'updates')
            else:
                print('no updates')

            print('\n')
        else:
            print('writing new table')
            # to delete table:
            # engine.execute('DROP TABLE reuters_raw_rss')
            feeds_df.to_sql(tablename, con=engine, index=False)

        print('finished, waiting one minute...\n\n')
        time.sleep(60)


def load_rss():
    """
    loads full sql database full of feedparser-parsed rss feeds
    """
    postgres_uname = os.environ.get('postgres_uname')
    postgres_pass = os.environ.get('postgres_pass')
    db = 'postgresql://{}:{}@localhost:5432/rss_feeds'.format(postgres_uname, postgres_pass)
    engine = create_engine(db, echo=False)
    tablename = 'reuters_raw_rss'
    df = pd.read_sql(tablename, con=engine)
    print(df.shape)
    df.drop_duplicates(subset=['feedburner_origlink', 'id'], inplace=True)
    print(df.shape)
    # set timezone and convert to Mountain time
    # published_parsed is wrong
    df['time_added'] = df['time_added'].dt.tz_localize('UTC')
    df['published_parsed'] = pd.to_datetime(df['published']).dt.tz_localize('UTC')
    for c in ['time_added', 'published_parsed']:
        df[c] = df[c].dt.tz_convert('America/Denver')



# scraping stories
link = df[df['category'] == 'tech'].iloc[0]['feedburner_origlink']
res = req.get(link)
soup = bs(res.content, 'lxml')
header = soup.find('div', {'class': 'ArticleHeader_content-container'})
datetime_str = soup.find('div', {'class': 'ArticleHeader_date'}).text.split('/')
# with base requests, this should be in UTC/GMT
article_datetime = pd.to_datetime(datetime[0].strip() + ' ' + datetime[1].strip()).tz_localize('UTC')
body = soup.find('div', {'class': 'StandardArticleBody_body'}).text
# clean up location and reporting agency
loc_reporting_idx = body.find(' - ') + 3
body = body[loc_reporting_idx:]
# clean up boilerplate at the end
if 'Additional reporting by' in body:
    ar_idx = body.find('Additional reporting')
    body = body[:ar_idx].strip()
elif 'Writing by ' in body:
    wb_idx = body.find('Additional reporting')
    body = body[:wb_idx].strip()
elif 'Editing by ' in body:
    eb_idx = body.find('Additional reporting')
    body = body[:eb_idx].strip()
elif 'Our Standards: ' in body:
    os_idx = body.find('Additional reporting')
    body = body[:os_idx].strip()




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
