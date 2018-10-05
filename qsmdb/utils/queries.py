def daily_non_equity(tsid, data_vendor_id, beg_date, end_date):
    return """SELECT dp.tsid,
        CAST(dp.date AS DATE),
        CAST(dp.ohlc ->> 'open' AS float) AS open,
        CAST(dp.ohlc ->> 'high' AS float) AS high,
        CAST(dp.ohlc ->> 'low' AS float) AS low,
        CAST(dp.ohlc ->> 'close' AS float) AS close,
        (dp.volume ->> 'volume') AS volume
        FROM daily_prices dp
        WHERE dp.tsid = '%s'
        AND dp.vendor_id = '%s'
        AND dp.date>='%s'::date AND dp.date<='%s'::date;""" % (tsid, data_vendor_id, beg_date, end_date)


def daily_equity(tsid, data_vendor_id, beg_date, end_date):
    return """SELECT dp.tsid,
        CAST(dp.date AS DATE),
        CAST(dp.ohlc ->> 'open' AS float) AS open,
        CAST(dp.ohlc ->> 'high' AS float) AS high,
        CAST(dp.ohlc ->> 'low' AS float) AS low,
        CAST(dp.ohlc ->> 'close' AS float) AS close,
        (dp.volume ->> 'volume') AS volume,
        COALESCE(CAST(sp.splits ->> 'splits' AS float), 1.0) AS split,
        COALESCE(CAST(div.dividends ->> 'dividends' AS float), 0.0) AS dividend
        FROM daily_prices dp
        LEFT JOIN equity_splits sp ON sp.vendor_id = dp.vendor_id
        AND sp.tsid = dp.tsid
        AND sp.date = dp.date
        LEFT JOIN equity_dividends div ON div.vendor_id = dp.vendor_id
        AND div.tsid = dp.tsid
        AND div.date = dp.date
        WHERE dp.tsid = '%s'
        AND dp.vendor_id = '%s'
        AND dp.date>='%s'::date AND dp.date<='%s'::date;""" % (tsid, data_vendor_id, beg_date, end_date)