def execute_query(sql_query, user, password, host, port, database):
    from sqlalchemy import create_engine
    from pandas import read_sql

    engine = create_engine('postgresql://' + user + ':' + password + '@' + host + ':' + port + '/' + database)
    data = read_sql(sql=sql_query, con=engine)
    engine.dispose()
    return data


def pull_daily_prices(tsid, data_vendor_id, beg_date, end_date, adjust,
                      database, user, password, host, port,
                      excl_sec_type):
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
    from pandas import to_datetime
    from numpy import nan
    from .queries import daily_equity, daily_non_equity
    from .mods import calculate_adjusted_prices
    try:
        print('Extracting the daily prices for %s' % (tsid,))

        if tsid.split('.')[-1] not in excl_sec_type:
            price_query = daily_equity(tsid, data_vendor_id, beg_date, end_date)
        else:
            price_query = daily_non_equity(tsid, data_vendor_id, beg_date, end_date)

        df = execute_query(price_query, user, password, host, port, database)
        df.fillna(value=nan, inplace=True)
        if len(df) == 0:
            raise SystemExit('No data returned from table query. Try adjusting the criteria for the query.')

        df['date'] = to_datetime(df['date'], utc=True)
        df['date'] = df['date'].apply(lambda x: x.date())
        # The next two lines change the index of the df to be the date.
        df.set_index(['date'], inplace=True)
        df.index.name = 'date'

        df.sort_values(by='date', inplace=True)

        if adjust and tsid.split('.')[-1] not in excl_sec_type:
            # Calculate the adjusted prices for the close column
            df = calculate_adjusted_prices(df=df)
        return df

    except Exception as e:
        print(e)
        raise SystemError('Error: Unknown issue occurred in pull_daily_prices')


def pull_fundamentals(cat, data_vendor_id, beg_date, end_date, database, user, password, host, port, verbose,
                      tsid, special_cat=None):
    """ Query the daily prices from the database for the tsid provided between
    the start and end dates. Return a DataFrame with the prices.

    :param database: String of the database name
    :param user: String of the username used to login to the database
    :param password: String of the password used to login to the database
    :param host: String of the database address (localhost, url, ip, etc.)
    :param port: Integer of the database port number (5432)
    :param data_vendor_id: Integer of which data vendor id to return prices for
    :param beg_date: String of the ISO date to start with
    :param end_date: String of the ISO date to end with
    :return: DataFrame of the returned prices
    """
    from numpy import nan
    from pandas import to_datetime
    try:

        if verbose:
            print('Extracting the "%s" for "%s"' % (cat, tsid))

        cat_query = cat(tsid, beg_date, end_date, data_vendor_id)
        df = execute_query(cat_query, user, password, host, port, database)
        df.fillna(value=nan, inplace=True)

        if len(df) == 0:
            print('!!! --> No data for "%s" available, or try adjusting the criteria for the query.' % tsid)
            return df
        if special_cat == 'earnings_trend':
            df['extraction_date'] = to_datetime(df['extraction_date'], utc=True)
            df['extraction_date'] = df['extraction_date'].apply(lambda x: x.date())
            # The next two lines change the index of the df to be the date.
            df.set_index(['extraction_date'], inplace=True)
            df.index.name = 'extraction_date'

            df.sort_values(by='extraction_date', inplace=True)
            return df
        else:
            df['date'] = to_datetime(df['date'], utc=True)
            df['date'] = df['date'].apply(lambda x: x.date())
            # The next two lines change the index of the df to be the date.
            df.set_index(['date'], inplace=True)
            df.index.name = 'date'

            df.sort_values(by='date', inplace=True)
            return df

    except Exception as e:
        print(e)
        raise SystemError('Error: Unknown issue occurred in pull_fundamentals')
