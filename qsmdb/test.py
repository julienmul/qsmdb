from qsmdb.master import *
import json

with open('utils/config.json') as data_file:
    cfg = json.load(data_file)
host = cfg['postgres']['secmaster_host']
port = cfg['postgres']['secmaster_port']
user = cfg['postgres']['secmaster_user']
password = cfg['postgres']['secmaster_password']
database = cfg['postgres']['secmaster_db']
beg_date = '2018-06-01'
end_date = '2018-09-04'
data_vendor_id = 20
query_type = cfg['non_ticker_type']
ticker = 'GDAXI.INDEX'
tsid_list = ['GDAXI.INDEX', 'DBK.DE']


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
    test = query_secmasterdb(tsid_list,
                             frequency='daily',
                             data_vendor_id=20)

    assert len(test) > 0
    if ticker.split('.')[-1] not in query_type:
        assert len(test['adj_close']) > 0
    print(test.head())


if __name__ == "__main__":
    # test_dbconnecction()
    test_full()
