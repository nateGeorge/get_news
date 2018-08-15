import string
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import pandas as pd
import requests as req
from bs4 import BeautifulSoup as bs
from tqdm import tqdm

BASE_LINK = 'https://www.nasdaq.com/screening/companies-by-name.aspx?{}&pagesize=200&page={}'

# can get all stocks at once setting pagesize equal to a huge number
full_link = 'https://www.nasdaq.com/screening/companies-by-name.aspx?pagesize=20000'


def add_stocks(letter, page, get_last_page=False):
    """
    goes through each row in table and adds to df if it is a stock

    returns the appended df
    """
    df = pd.DataFrame()
    res = req.get(BASE_LINK.format(letter, page))
    soup = bs(res.content, 'lxml')
    table = soup.find('table', {'id': 'CompanylistResults'})
    stks = table.findAll('tr')
    stocks_on_page = (len(stks) - 1) / 2

    for stk in stks[1:]:
        deets = stk.findAll('td')
        if len(deets) != 7:
            continue
        company_name = deets[0].text.strip()
        ticker = deets[1].text.strip()
        market_cap = deets[2].text.strip()
        # 4th entry is blank
        country = deets[4].text.strip()
        ipo_year = deets[5].text.strip()
        subsector = deets[6].text.strip()
        df = df.append(pd.Series({'company_name': company_name,
                                'market_cap': market_cap,
                                'country': country,
                                'ipo_year': ipo_year,
                                'subsector': subsector},
                                name=ticker))

    if get_last_page:
        # get number of pages
        lastpage_link = soup.find('a', {'id': 'two_column_main_content_lb_LastPage'})
        last_page_num = int(lastpage_link['href'].split('=')[-1])
        return df, total_num_stocks, last_page_num

    return df, stocks_on_page


def get_all_stocks():
    """
    goes through each row in table and adds to df if it is a stock

    returns the appended df
    """
    df = pd.DataFrame()
    res = req.get(full_link)
    soup = bs(res.content, 'lxml')
    table = soup.find('table', {'id': 'CompanylistResults'})
    stks = table.findAll('tr')

    for stk in stks[1:]:
        deets = stk.findAll('td')
        if len(deets) != 7:
            continue
        company_name = deets[0].text.strip()
        ticker = deets[1].text.strip()
        market_cap = deets[2].text.strip()
        # 4th entry is blank
        country = deets[4].text.strip()
        ipo_year = deets[5].text.strip()
        subsector = deets[6].text.strip()
        df = df.append(pd.Series({'company_name': company_name,
                                'market_cap': market_cap,
                                'country': country,
                                'ipo_year': ipo_year,
                                'subsector': subsector},
                                name=ticker))

    return df


def clean_abbreviations(x):
    """
    replaces K with 000, M with 000000, B with 000000000
    """
    # a few entries in Revenue were nan
    if pd.isnull(x) or x == 'n/a':
        return np.nan
    elif 'K' in x:
        return float(x[:-1]) * 1e3
    elif 'M' in x:
        return float(x[:-1]) * 1e6
    elif 'B' in x:
        return float(x[:-1]) * 1e9
    else:
        return float(x)



def get_all_stocks():
    # can just do it all at once
    full_df = get_all_stocks()

    # clean and save
    full_df_na_sub = full_df.copy()
    # remove $ sign
    full_df_na_sub['market_cap'] = full_df_na_sub['market_cap'].apply(lambda x: x.replace('$', ''))
    full_df_na_sub['market_cap'] = full_df_na_sub['market_cap'].apply(clean_abbreviations)
    full_df_na_sub = full_df_na_sub.replace('n/a', np.nan)
    full_df_na_sub['market_cap'] = full_df_na_sub['market_cap'].astype('float')
    full_df_na_sub['ipo_year'] = full_df_na_sub['ipo_year'].astype('float')
    full_df_na_sub.index.name = 'ticker'
    todays_date = datetime.today().strftime('%Y-%m-%d')
    filename = 'nasdaq_stock_listing_' + todays_date + '.csv'
    full_df_na_sub.to_csv(filename)


    # test loading
    test_df = pd.read_csv(filename, index_col='ticker')
    # small issue with float precision upon saving and loading
    cols = ['company_name', 'country', 'subsector']
    print(test_df[cols].equals(full_df_na_sub[cols]))
    float_cols = ['ipo_year', 'market_cap']
    # should be 1
    print(np.mean(np.isclose(test_df[float_cols], full_df_na_sub[float_cols], equal_nan=True)))


def load_nasdaq_stocklist():
    # TODO: save in folder, get latest file
    date = '2018-08-15'
    filename = 'nasdaq_stock_listing_' + date + '.csv'
    df = pd.read_csv(filename, index_col='ticker')
    return df

# old try; no longer needed
"""
total_num_stocks = 0  # sanity check to make sure everything was grabbed
# full_df = pd.DataFrame()  # initialize df for storing all stock data



# for letter in tqdm(string.ascii_uppercase):
#     print(letter)

letter = 'A'
page = ''
# 1st row is labels
full_df1, stocks_on_page, last_page_num = add_stocks(letter, page, get_last_page=True)
total_num_stocks += stocks_on_page

jobs = []
with ThreadPoolExecutor() as executor:
    for page in range(2, last_page_num + 1):
        print(page)
        r = executor.submit(add_stocks, letter, str(page))
        jobs.append((page, r))
        # df, stocks_on_page = add_stocks(letter, str(page), df, total_num_stocks)

dfs = []
for page, r in jobs:
    cur_df, stocks_on_page = r.result()
    dfs.append(cur_df)
    total_num_stocks += stocks_on_page

full_df = pd.concat([full_df] + dfs)
"""
