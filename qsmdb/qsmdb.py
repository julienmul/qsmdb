from .config import cfg, available_fundamentals
from .db_io import execute_query, pull_daily_prices, pull_fundamentals
from .queries import get_asset_type, check_asset_query

import datetime as dt
import pandas as pd
import time

database = cfg['postgres']['secmaster_db']
user = cfg['postgres']['secmaster_user']
password = cfg['postgres']['secmaster_password']
host = cfg['postgres']['secmaster_host']
port = cfg['postgres']['secmaster_port']
excl_sec_type = cfg['non_ticker_type']


def get_available_fundamentals(sec_class='equity', sec_type='stock'):
    return [cat for cat in available_fundamentals[sec_class][sec_type]]


# TODO: implement sector search
def get_sector_list():
    pass


# TODO: implement industry search
def get_industry_list():
    pass


def get_security_prices(tsid_list, beg_date='1990-01-01',
                        end_date=dt.datetime.today(), frequency='daily',
                        data_vendor_id=20, adjust=True, verbose=False):
    """ Wrapper for function pull_daily_prices. Gets input tsid symbols and checks config for input.

    :param tsid_list: List or String of security symbols
    :param beg_date: String of the ISO date to start with; default '1990-01-01'
    :param end_date: String of the ISO date to end with; default 'today'
    :param frequency: String of the predefined periodicity of data; default 'daily'
    :param data_vendor_id: Integer of the data vendor id; default 20
    :param verbose: Boolean of whether print debug info or not; default false
    :return: Status message and DataFrame of the appended tsid prices
    """
    if type(tsid_list) is not list:
        tsid_list = [tsid_list]
    start_time = time.time()
    prices_df = pd.DataFrame()

    for ticker in tsid_list:
        # 1. check if ticker exists
        check = check_asset_query(ticker, data_vendor_id)
        ticker_type = execute_query(check, user, password, host, port, database)
        if ticker.split('.')[1] in excl_sec_type:
            ticker_type['exists'][0] = True  # --> TODO: Refactor Backend!!!
        if ticker_type['exists'][0]:
            # 2. check if frequency is implemented
            if frequency == 'daily':
                # 3. run price query for each ticker
                ticker_df = pull_daily_prices(ticker, data_vendor_id, beg_date, end_date, adjust,
                                              database, user, password, host, port,
                                              excl_sec_type)
            else:
                raise NotImplementedError('Frequency %s is not implemented within '
                                          'qsmdb.py' % frequency)

            prices_df = pd.concat([prices_df, ticker_df], ignore_index=False, sort=True)
        else:
            print("Skipping: ", ticker, " -> ticker does not exist for vendor id or in excluded 'sec_type' list!")
            continue

    if verbose:
        print('Query took %0.2f seconds' % (time.time() - start_time))
        unique_codes = pd.unique((prices_df['tsid']).values)
        print('There are %i unique tsid codes' % (len(unique_codes)))
        print('There are %s rows' % ('{:,}'.format(len(prices_df.index))))
        print('The earliest date in query is {}\nThe latest date is {}'.format(
            min(prices_df.index), max(prices_df.index)))
        print('Today is the {}'.format(dt.datetime.today().strftime('%Y-%m-%d')))

    return prices_df


def get_security_fundamentals(tsid_list, cat_list, beg_date='1990-01-01',
                              end_date=dt.datetime.today(),
                              data_vendor_id=20, verbose=False):
    """ Wrapper for function pull_daily_prices. Gets input tsid symbols and checks config for input.

    :param tsid_list: List or String of security symbols
    :param cat_list: List or String of query categories
    :param beg_date: String of the ISO date to start with; default '1990-01-01'
    :param end_date: String of the ISO date to end with; default 'today'
    :param data_vendor_id: Integer of the data vendor id; default 20
    :param verbose: Boolean of whether print debug info or not; default false
    :return: Status message and DataFrame of the appended tsid prices
    """
    if type(tsid_list) is not list:
        tsid_list = [tsid_list]
    if type(cat_list) is not list:
        cat_list = [cat_list]
    start_time = time.time()
    output_dict = dict()

    for tsid in tsid_list:
        ticker_dict = dict()
        # 1. check if ticker exists
        check = check_asset_query(tsid, data_vendor_id)
        ticker_type = execute_query(check, user, password, host, port, database)
        if ticker_type['exists'][0] and tsid.split('.')[-1] not in excl_sec_type:
            # 2. if exists, check ticker class/type
            asset_query = get_asset_type(tsid, data_vendor_id)
            asset_type = execute_query(asset_query, user, password, host, port, database)
            # 3. run fundamentals query for each category
            if asset_type['type'][0] in ['Common Stock']:
                for category in cat_list:
                    if category in available_fundamentals['equity']['stock']:
                        cat = available_fundamentals['equity']['stock'][category]
                        if category == 'earnings_trend':
                            special_cat = 'earnings_trend'
                        else:
                            special_cat = None
                        ticker_dict[category] = pull_fundamentals(cat, data_vendor_id, beg_date, end_date,
                                                                  database, user, password, host, port, verbose,
                                                                  tsid, special_cat)
                    else:
                        print("Skipping: ", category, " -> category does not exist or not implemented!")
                        continue

            elif asset_type['type'][0] in ['ETF', 'ETP']:
                for category in cat_list:
                    if category in available_fundamentals['equity']['etp']:
                        cat = available_fundamentals['equity']['etp'][category]
                        ticker_dict[category] = pull_fundamentals(cat, data_vendor_id, beg_date, end_date,
                                                                  database, user, password, host, port, verbose,
                                                                  tsid)
                    else:
                        print("Skipping: ", category, " -> category does not exist or not implemented!")
                        continue

            else:
                print(NotImplemented)
                continue
            output_dict[tsid] = ticker_dict
        else:
            print("Skipping: ", tsid, " -> ticker does not exist for vendor id or in excluded 'sec_type' list!")
            continue

        # if verbose:
        #     unique_codes = pd.unique((fundamentals_data['tsid']).values)
        #     print('"%s" has # %i unique tsid codes' % (cat, len(unique_codes)))
    print('Query took %0.2f seconds' % (time.time() - start_time))
    return output_dict
