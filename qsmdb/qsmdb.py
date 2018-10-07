from .config import daily_equity, daily_non_equity, cfg
from .mods import calculate_adjusted_prices

import datetime as dt
from sqlalchemy import create_engine
import pandas as pd
import time


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


def query_secmasterdb(tsid_list, beg_date='1960-01-01',
                      end_date=dt.datetime.today(), frequency='daily',
                      data_vendor_id=20):
    """ Wrapper for function pull_daily_prices. Gets input tsid symbols and checks config for input.

    :param tsid_list:
    :param beg_date: String of the ISO date to start with
    :param end_date: String of the ISO date to end with
    :param frequency: String of the predefined periodicity of data
    :param data_vendor_id: Integer of the data vendor id
    :return: Status message and DataFrame of the appended tsid prices
    """
    database = cfg['postgres']['secmaster_db']
    user = cfg['postgres']['secmaster_user']
    password = cfg['postgres']['secmaster_password']
    host = cfg['postgres']['secmaster_host']
    port = cfg['postgres']['secmaster_port']
    query_type = cfg['non_ticker_type']

    if type(tsid_list) is not list:
        tsid_list = list(tsid_list)
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

    print('Query took %0.2f seconds' % (time.time() - start_time))
    unique_codes = pd.unique((prices_df['tsid']).values)
    print('There are %i unique tsid codes' % (len(unique_codes)))
    print('There are %s rows' % ('{:,}'.format(len(prices_df.index))))
    print('The earliest date in query is {}\nThe latest date is {}'.format(
        min(prices_df.index), max(prices_df.index)))
    print('Today is the {}'.format(dt.datetime.today().strftime('%Y-%m-%d')))

    return prices_df
