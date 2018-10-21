from qsmdb.config import cfg
host = cfg['postgres']['secmaster_host']
port = cfg['postgres']['secmaster_port']
user = cfg['postgres']['secmaster_user']
password = cfg['postgres']['secmaster_password']
database = cfg['postgres']['secmaster_db']


def test_check_asset_in_db():
    from qsmdb.queries import check_asset_query
    from qsmdb.db_io import execute_query
    test_query = check_asset_query('DBK.DE', data_vendor_id=20)
    output = execute_query(test_query, user, password, host, port, database)
    print(type(output['exists'][0]))
    print(output['exists'][0])
    assert output['exists'][0] == True


def test_check_asset_type():
    from qsmdb.queries import get_asset_type
    from qsmdb.db_io import execute_query
    test_query = get_asset_type('DBK.DE', 20)
    output = execute_query(test_query, user, password, host, port, database)
    print(type(output['type'][0]))
    print(output['type'][0])
    # assert len(output['type']) > 0
    assert isinstance(output['type'][0], str)


def test_check_asset_sector():
    from qsmdb.queries import get_asset_sector
    from qsmdb.db_io import execute_query
    test_query = get_asset_sector('DBK.DE', 20)
    output = execute_query(test_query, user, password, host, port, database)
    print(type(output['sector'][0]))
    print(output['sector'][0])
    # assert len(output['type']) > 0
    assert isinstance(output['sector'][0], str)


def test_check_asset_industry():
    from qsmdb.queries import get_asset_industry
    from qsmdb.db_io import execute_query
    test_query = get_asset_industry('DBK.DE', 20)
    output = execute_query(test_query, user, password, host, port, database)
    print(type(output['industry'][0]))
    print(output['industry'][0])
    # assert len(output['type']) > 0
    assert isinstance(output['industry'][0], str)
