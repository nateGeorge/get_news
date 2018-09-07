"""
postgresql:

sudo -u postgres psql
list dbs: \l
use a db: \c dbname
list tables: \d
quit: \q



"""

# TODO: deal with companies without tickers in the story, e.g.
# https://www.reuters.com/article/us-sprint-m-a-t-mobile/trump-advisor-touts-sprint-t-mobile-deal-while-denying-lobbying-idUSKBN1L01R4
# Trump advisor touts Sprint, T-Mobile deal while denying lobbying

import re
import os
import time
from datetime import datetime

import spacy
from fuzzywuzzy import fuzz
import requests as req
from bs4 import BeautifulSoup as bs
import feedparser
import pandas as pd
import numpy as np
from sqlalchemy import create_engine as ce
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# nlp = spacy.load('en')
nlp = spacy.load('en_core_web_lg')

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


def create_engine():
    # create connection
    postgres_uname = os.environ.get('postgres_uname')
    postgres_pass = os.environ.get('postgres_pass')
    db = 'postgresql://{}:{}@localhost:5432/rss_feeds'.format(postgres_uname, postgres_pass)
    engine = ce(db, echo=False)
    return engine


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
    engine = create_engine()
    # create test table for sanity check
    # engine.execute("CREATE TABLE IF NOT EXISTS test();")
    while True:
        all_feeds = []
        for l, f in reuters_feed_list.items():
            print(l)
            complete = False
            while not complete:
                try:
                    feeds = feedparser.parse(f)
                    if feeds['status'] != 200:
                        print('status is not good:', str(feeds['status']))
                    else:
                        complete = True
                except KeyError:
                    print('keyerror, probably a bad html request or something')
                    time.sleep(3)

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
        stmt = "select exists(select relname from pg_class where relname='" + tablename + "');"
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
            # engine.execute('DROP TABLE reuters_raw_rss;')
            feeds_df.to_sql(tablename, con=engine, index=False)

        print('finished, waiting one minute...\n\n')
        time.sleep(60)


def load_rss():
    """
    loads full sql database full of feedparser-parsed rss feeds
    """
    engine = create_engine()
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

    return df


def get_sentiments_vader(x, analyzer):
    vs = analyzer.polarity_scores(x)
    return pd.DataFrame([vs['compound'], vs['pos'], vs['neg'], vs['neu']], index=['compound', 'pos', 'neg', 'neu']).T


def scrape_story(story_df):
    """
    pass in one slice of the raw rss dataframe

    this grabs the stocks in the story, the overall sentiment, and cleans the body
    then stores it in a sql database
    """
    link = story_df['feedburner_origlink']

    # first check if story and sentiments in db, if so, skip that one
    tablename = 'reuters_story_bodies'
    # check if already in DBs
    tableexists = engine.execute("SELECT to_regclass('" + tablename + "');")
    body_table_exists = tableexists.fetchone()[0]
    in_body_db = None
    # if table exists, check if entry is in DB
    if body_table_exists is not None:
        indb = engine.execute('select 1 from ' + tablename + ' where feedburner_origlink = \'' + link + '\';')
        in_body_db = indb.fetchone()
        if in_body_db is not None:
            in_body_db = in_body_db[0]

    # DB for sentence-level sentiment if stock in title
    tablename = 'reuters_story_sentiments'
    # check if already in DB
    tableexists = engine.execute("SELECT to_regclass('" + tablename + "');")
    sent_table_exists = tableexists.fetchone()[0]
    in_sent_db = None
    # if table exists, check if entry is in DB
    if sent_table_exists is not None:
        indb = engine.execute('select body from ' + tablename + ' where feedburner_origlink = \'' + link + '\';')
        in_sent_db = indb.fetchone()
        if in_sent_db is not None:
            in_sent_db = int_sent_db[0]


    if in_body_db is not None and in_sent_db is not None:
        print('already in both DBs')
        return

    # scrape story details
    tablename = 'reuters_story_bodies'
    if in_body_db is None:
        res = req.get(link)
        if res.status_code == 500:
            print('status code 500; page unavailable')
            return
        soup = bs(res.content, 'lxml')
        # get published time, which seems to be more accurate than the rss feed's time
        header = soup.find('div', {'class': 'ArticleHeader_content-container'})
        datetime_str = soup.find('div', {'class': 'ArticleHeader_date'}).text.split('/')
        # with base requests, this should be in UTC/GMT
        article_datetime = pd.to_datetime(datetime_str[0].strip() + ' ' + datetime_str[1].strip()).tz_localize('UTC')

        # get story body
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
    else:
        body_query = engine.execute('select body from ' + tablename + ' where feedburner_origlink = \'' + link + '\';')
        body = body_query.fetchone()[0]



    # search for tickers in story
    # TODO: find CEOs, other important entities in story and get sentiment towards them too

    stocks_in_story = []
    stocks = re.findall('\([A-Z]+\.[A-Z]+\)', body)
    if stocks is not None and len(stocks) > 0:
        print('found stocks:')
        for s in stocks:
            print(s)
            # remove RIC and paranthesis
            per_idx = s.find('.')
            stocks_in_story.append(s[1:per_idx])

    # find all mentions of stocks in story and get full stock name


    # get list of entity names (stocks_ents) used for stocks in story, so can get sentences with stocks mentioned in them
    # also get full stock names in full_stock_names

    proc_doc = nlp(body)
    # get all mentions of stocks
    stocks_to_match = set(stocks_in_story)
    stocks_ents = {}
    full_stock_names = {}
    for ent in proc_doc.ents:
        if len(stocks_to_match) != 0:
            for s in stocks_to_match:
                if s in ent.text:
                    # sometimes the entity also has ticker, e.g.  Xcel Energy Inc (XEL.O
                    # clean stock from entity
                    cleaned_stock_name = re.sub('\([A-Z]+\.[A-Z]+\)*', '', ent.text).strip()
                    full_stock_names[s] = cleaned_stock_name

            for r in ent.rights:
                remove = None
                for s in stocks_to_match:
                    if s in r.text:
                        full_stock_names[s] = ent.text
                        remove = s
                        break
                if remove is not None:
                    stocks_to_match.remove(remove)

        for s, f in full_stock_names.items():
            if f == ent.text or ent.text in f:
                stocks_ents.setdefault(s, []).append(ent)
            else:
                ratio = fuzz.ratio(f, ent.text)
                if ratio > 50:
                    stocks_ents.setdefault(s, []).append(ent)


    # TODO: search for company names without ticker in story


    # TODO: get sentiment towards entities (stocks)
    # possible tools for it:
    # https://github.com/D2KLab/sentinel
    # https://github.com/charlesashby/entity-sentiment-analysis


    # get overall document sentiment -- doesn't work super well with vader for whole document
    analyzer = SentimentIntensityAnalyzer()

    sentiments = get_sentiments_vader(body, analyzer)

    stocks_ents_sentiments = {}
    avg_stocks_ents_sents  = {}
    for s in stocks_ents.keys():
        sentiments_list = []
        for ent in stocks_ents[s]:
            temp_sentiments = get_sentiments_vader(ent.sent.text, analyzer)
            sentiments_list.append(temp_sentiments)

        stocks_ents_sentiments[s] = pd.concat(sentiments_list).reset_index().drop('index', axis=1)
        avg_stocks_ents_sents[s] = stocks_ents_sentiments[s].mean()


    # TODO:
    # flog keywords: SEC, subpoena, sue, etc for negative
    # look for stock entity in title to find focus of story
    if in_body_db is None:
        story_df_to_save = pd.Series({'feedburner_origlink': link,
                                'datetime': article_datetime,
                                'body': body,
                                'stocks_in_story': ', '.join(stocks_in_story),
                                # 'stocks_ents': stocks_ents,  # can't put dict in sql, and not sure want to save this anyway
                                'overall_vader_compound': sentiments['compound'][0],
                                'overall_vader_pos': sentiments['pos'][0],
                                'overall_vader_neg': sentiments['neg'][0],
                                'overall_vader_neu': sentiments['neu'][0]}).to_frame().T

        # save to sql database
        # save overall story details
        story_df_to_save.to_sql(tablename, con=engine, if_exists='append', index=False)


    # TODO: email summary/save avg_stocks_ents_sents to db
    # TODO: text/email alert if sentiment is highly positive or negative (> 0.5 or < -0.5)
    # or if keyword in story like sue, subpoena, etc


    # find any stocks in title; set these as focus of the story
    stocks_in_title = 0
    for s in stocks_in_story:
        # sometimes entity detection doesn't detect the stock
        if s not in stocks_ents.keys():
            continue

        for ent in stocks_ents[s]:
            if ent.text in story_df['title']:
                stocks_in_title += 1
                main_stock = s
                break  # only match once per stock

    if stocks_in_title == 1 and sent_table_exists is not None:
        tablename = 'reuters_story_sentiments'
        if in_sent_db is None:
            # save overall story sentiment if not already in db
            sent_df = pd.Series({'feedburner_origlink': link,
                                'ticker': main_stock,
                                'overall_vader_compound': sentiments['compound'][0],
                                'overall_vader_pos': sentiments['pos'][0],
                                'overall_vader_neg': sentiments['neg'][0],
                                'overall_vader_neu': sentiments['neu'][0],
                                'sentence_vader_compound': avg_stocks_ents_sents[s]['compound'],
                                'sentence_vader_pos': avg_stocks_ents_sents[s]['pos'],
                                'sentence_vader_neg': avg_stocks_ents_sents[s]['neg'],
                                'sentence_vader_neu': avg_stocks_ents_sents[s]['neu']}).to_frame().T
            sent_df.to_sql(tablename, con=engine, if_exists='append', index=False)


# # scraping stories
# df = load_rss()
# #
# # # 246 temporarily unavailable
# # # TABLE-U.S. fund investors move cash to foreign stocks, most since May -ICI
# # # https://www.reuters.com/article/usa-mutualfunds-ici/table-u-s-fund-investors-move-cash-to-foreign-stocks-most-since-may-ici-idUSL1N1V60YX?feedType=RSS&feedName=companyNews
engine = create_engine()
# for i, r in df.iterrows():
#     if i == 246:
#         continue
#     print(i)
#     print(r['title'])
#     scrape_story(r, engine)
#
# story_df = df[df['category'] == 'tech'].iloc[0]




def load_story_df(remove_dupes=False):
    engine = create_engine()
    tablename = 'reuters_story_bodies'
    story_df = pd.read_sql(tablename, con=engine)
    if remove_dupes:
        story_df.drop_duplicates(inplace=True)
        engine.execute('DROP TABLE IF EXISTS ' + tablename + ';')
        story_df.to_sql(tablename, con=engine, index=False)

    return story_df


def load_sent_df(remove_dupes=False):
    engine = create_engine()
    tablename = 'reuters_story_sentiments'
    sent_df = pd.read_sql(tablename, con=engine)
    if remove_dupes:
        sent_df.drop_duplicates(inplace=True)
        engine.execute('DROP TABLE IF EXISTS ' + tablename + ';')
        sent_df.to_sql(tablename, con=engine, index=False)

    return story_df



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
