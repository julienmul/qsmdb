from qsmdb.config import cfg

host = cfg['postgres']['secmaster_host']
port = cfg['postgres']['secmaster_port']
user = cfg['postgres']['secmaster_user']
password = cfg['postgres']['secmaster_password']
database = cfg['postgres']['secmaster_db']

beg_date = '2017-06-01'
end_date = '2018-09-04'
data_vendor_id = 20
query_type = cfg['non_ticker_type']
tsid = 'DBK.DE'
tsid_list = ['CBK.DE', 'DBK.DE', 'GDAXI.INDEX', 'AAPL.Q']
test_category = 'key_metrics_technicals'
verbose = True


def test_pull_prices_part1():
    from qsmdb.db_io import pull_daily_prices
    df = pull_daily_prices(database, user, password, host, port, query_type,
                           data_vendor_id, beg_date, end_date, True,
                           tsid)
    print(df.head())
    assert len(df) > 0


def test_pull_fundamentals_part1():
    from qsmdb.db_io import execute_query
    from qsmdb.config import available_fundamentals
    from numpy import nan
    cat = available_fundamentals['equity']['stock'][test_category]
    cat_query = cat(tsid, beg_date, end_date, data_vendor_id)
    df = execute_query(cat_query, user, password, host, port, database)
    df.fillna(value=nan, inplace=True)
    assert len(df) > 0


def test_pull_fundamentals():
    from qsmdb.db_io import pull_fundamentals
    from qsmdb.config import available_fundamentals
    cat = available_fundamentals['equity']['stock'][test_category]
    df = pull_fundamentals(cat, data_vendor_id, beg_date, end_date,
                           database, user, password, host, port, verbose,
                           tsid)
    print(df.head())
    assert len(df) > 0
