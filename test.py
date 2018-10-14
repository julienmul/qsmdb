from qsmdb.qsmdb import pull_daily_prices, cfg, query_security_prices, query_security_fundamentals
from qsmdb.config import available_fundamentals
import datetime as dt
from qsmdb.qsmdb import pull_fundamentals

host = cfg['postgres']['secmaster_host']
port = cfg['postgres']['secmaster_port']
user = cfg['postgres']['secmaster_user']
password = cfg['postgres']['secmaster_password']
database = cfg['postgres']['secmaster_db']
beg_date = '2017-06-01'
end_date = '2018-09-04'
data_vendor_id = 20
query_type = cfg['non_ticker_type']
ticker = 'DBK.DE'
tsid_list = ['CBK.DE', 'DBK.DE', 'GDAXI.INDEX', 'AAPL.Q']


def test_dbconnecction():
    test = pull_daily_prices(database,
                             user,
                             password,
                             host,
                             port,
                             query_type,
                             data_vendor_id,
                             beg_date,
                             end_date,
                             'tsid', ticker)
    assert len(test) > 0
    if ticker.split('.')[-1] not in query_type:
        assert len(test['adj_close']) > 0
    print(test.head())


def test_full():
    test = query_security_prices(tsid_list,
                                 frequency='daily',
                                 data_vendor_id=20)

    assert len(test) > 0
    if ticker.split('.')[-1] not in query_type:
        assert len(test['adj_close']) > 0
    print(test.head())


def test_get_available_fundamentals(sec_type='equity'):
    return [category for category in available_fundamentals[sec_type]]


category_list = test_get_available_fundamentals()

if __name__ == "__main__":
    # test_dbconnecction()
    # test_full()
    # print(test_get_available_fundamentals())
    # cat = test_get_available_fundamentals()[1]
    # test_pull = pull_fundamentals(database, user, password, host, port, query_type,
    #                   data_vendor_id, beg_date, end_date, cat,
    #                   ticker)
    print(category_list)
    test_pull = query_security_fundamentals(tsid_list, cat_list=category_list, beg_date='1990-01-01',
                                            end_date=dt.datetime.today(),
                                            data_vendor_id=20)

    print(test_pull)
