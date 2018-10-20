from .config import daily_equity, daily_non_equity, cfg, available_fundamentals
from .mods import calculate_adjusted_prices

import datetime as dt
from sqlalchemy import create_engine
import pandas as pd
import time


def get_available_fundamentals(sec_type='equity'):
    return [cat for cat in available_fundamentals[sec_type]]


def pull_daily_prices(database, user, password, host, port, query_type,
                      data_vendor_id, beg_date, end_date, adjust=True,
                      *args):
    """ Query the daily prices from the database for the tsid provided between
    the start and end dates. Return a DataFrame with the prices.

    :param database: String of the database name
    :param user: String of the username used to login to the database
    :param password: String of the password used to login to the database
    :param host: String of the database address (localhost, url, ip, etc.)
    :param port: Integer of the database port number (5432)
    :param query_type: List of security properties to distinct equities
    :param data_vendor_id: Integer of which data vendor id to return prices for
    :param beg_date: String of the ISO date to start with
    :param end_date: String of the ISO date to end with
    :param adjust: Boolean of whether to adjust the values or not; default True
    :return: DataFrame of the returned prices
    """

    try:
        engine = create_engine('postgresql://' + user + ':' + password + '@' + host + ':' + port + '/' + database)
        tsid, = args
        print('Extracting the daily prices for %s' % (tsid,))
        if tsid.split('.')[-1] not in query_type:
            df = pd.read_sql(sql=daily_equity(tsid,
                                              data_vendor_id,
                                              beg_date,
                                              end_date),
                             con=engine)
        else:
            df = pd.read_sql(sql=daily_non_equity(tsid,
                                                  data_vendor_id,
                                                  beg_date,
                                                  end_date),
                             con=engine)
        if len(df) == 0:
            raise SystemExit('No data returned from table query. Try adjusting the criteria for the query.')

        df['date'] = pd.to_datetime(df['date'], utc=True)
        df['date'] = df['date'].apply(lambda x: x.date())
        # The next two lines change the index of the df to be the date.
        df.set_index(['date'], inplace=True)
        df.index.name = 'date'

        df.sort_values(by='date', inplace=True)

        if adjust and tsid.split('.')[-1] not in query_type:
            # Calculate the adjusted prices for the close column
            df = calculate_adjusted_prices(df=df)
        return df

    except Exception as e:
        print(e)
        raise SystemError('Error: Unknown issue occurred in pull_daily_prices')


def query_security_prices(tsid_list, beg_date='1990-01-01',
                          end_date=dt.datetime.today(), frequency='daily',
                          data_vendor_id=20, verbose=False):
    """ Wrapper for function pull_daily_prices. Gets input tsid symbols and checks config for input.

    :param tsid_list: List or String of security symbols
    :param beg_date: String of the ISO date to start with; default '1990-01-01'
    :param end_date: String of the ISO date to end with; default 'today'
    :param frequency: String of the predefined periodicity of data; default 'daily'
    :param data_vendor_id: Integer of the data vendor id; default 20
    :param verbose: Boolean of whether print debug info or not; default false
    :return: Status message and DataFrame of the appended tsid prices
    """
    database = cfg['postgres']['secmaster_db']
    user = cfg['postgres']['secmaster_user']
    password = cfg['postgres']['secmaster_password']
    host = cfg['postgres']['secmaster_host']
    port = cfg['postgres']['secmaster_port']
    query_type = cfg['non_ticker_type']

    if type(tsid_list) is not list:
        tsid_list = [tsid_list]
    start_time = time.time()
    prices_df = pd.DataFrame()

    for ticker in tsid_list:
        if frequency == 'daily':
            ticker_df = pull_daily_prices(database,
                                          user,
                                          password,
                                          host,
                                          port,
                                          query_type,
                                          data_vendor_id,
                                          beg_date,
                                          end_date,
                                          'tsid', ticker)

        else:
            raise NotImplementedError('Frequency %s is not implemented within '
                                      'qsmdb.py' % frequency)

        prices_df = pd.concat([prices_df, ticker_df], ignore_index=False, sort=True)
    if verbose:
        print('Query took %0.2f seconds' % (time.time() - start_time))
        unique_codes = pd.unique((prices_df['tsid']).values)
        print('There are %i unique tsid codes' % (len(unique_codes)))
        print('There are %s rows' % ('{:,}'.format(len(prices_df.index))))
        print('The earliest date in query is {}\nThe latest date is {}'.format(
            min(prices_df.index), max(prices_df.index)))
        print('Today is the {}'.format(dt.datetime.today().strftime('%Y-%m-%d')))

    return prices_df


def pull_fundamentals(database, user, password, host, port, query_type,
                      data_vendor_id, beg_date, end_date, cat, verbose,
                      *args):
    """ Query the daily prices from the database for the tsid provided between
    the start and end dates. Return a DataFrame with the prices.

    :param database: String of the database name
    :param user: String of the username used to login to the database
    :param password: String of the password used to login to the database
    :param host: String of the database address (localhost, url, ip, etc.)
    :param port: Integer of the database port number (5432)
    :param query_type: List of security properties to distinct equities
    :param data_vendor_id: Integer of which data vendor id to return prices for
    :param beg_date: String of the ISO date to start with
    :param end_date: String of the ISO date to end with
    :return: DataFrame of the returned prices
    """

    try:
        engine = create_engine('postgresql://' + user + ':' + password + '@' + host + ':' + port + '/' + database)
        tsid, = args
        if verbose:
            print('Extracting the "%s" for "%s"' % (cat, tsid))
        if tsid.split('.')[-1] not in query_type:
            df = pd.read_sql(sql=available_fundamentals['equity'][cat](tsid, beg_date, end_date, data_vendor_id),
                             con=engine)
        else:
            return pd.DataFrame()
        if len(df) == 0:
            print('!!! --> No data for "%s" available, or try adjusting the criteria for the query.' % tsid)
        if cat == 'earnings_trend':
            df['extraction_date'] = pd.to_datetime(df['extraction_date'], utc=True)
            df['extraction_date'] = df['extraction_date'].apply(lambda x: x.date())
            # The next two lines change the index of the df to be the date.
            df.set_index(['extraction_date'], inplace=True)
            df.index.name = 'extraction_date'

            df.sort_values(by='extraction_date', inplace=True)
            return df
        else:
            df['date'] = pd.to_datetime(df['date'], utc=True)
            df['date'] = df['date'].apply(lambda x: x.date())
            # The next two lines change the index of the df to be the date.
            df.set_index(['date'], inplace=True)
            df.index.name = 'date'

            df.sort_values(by='date', inplace=True)
            return df

    except Exception as e:
        print(e)
        raise SystemError('Error: Unknown issue occurred in pull_fundamentals')


def query_security_fundamentals(tsid_list, cat_list, beg_date='1990-01-01',
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
    database = cfg['postgres']['secmaster_db']
    user = cfg['postgres']['secmaster_user']
    password = cfg['postgres']['secmaster_password']
    host = cfg['postgres']['secmaster_host']
    port = cfg['postgres']['secmaster_port']
    query_type = cfg['non_ticker_type']

    if type(tsid_list) is not list:
        tsid_list = [tsid_list]
    if type(cat_list) is not list:
        cat_list = [cat_list]
    start_time = time.time()
    funda_dict = dict()

    for cat in cat_list:
        if cat in available_fundamentals['equity']:
            fundamentals_data = pd.DataFrame()
            for ticker in tsid_list:
                new_df = pull_fundamentals(database, user, password, host, port,
                                           query_type, data_vendor_id, beg_date,
                                           end_date, cat, verbose, ticker)

                fundamentals_data = pd.concat([fundamentals_data, new_df], ignore_index=False, sort=True)
            funda_dict[cat] = fundamentals_data
        else:
            print("Skipping: ", cat, " -> category does not exist or not implemented!") % cat
            continue
        if verbose:
            unique_codes = pd.unique((fundamentals_data['tsid']).values)
            print('"%s" has # %i unique tsid codes' % (cat, len(unique_codes)))
    print('Query took %0.2f seconds' % (time.time() - start_time))
    return funda_dict
