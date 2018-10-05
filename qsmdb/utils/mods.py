import numpy as np


def calculate_adjusted_prices(df):
    """ Vectorized approach for calculating the adjusted prices for the
    specified column in the provided DataFrame. This creates a new column
    called 'adj_<column name>' with the adjusted prices. This function requires
    that the DataFrame have columns with dividend and split values.

    NOTE: This assumes the input split values direct. E.g. 7-for-1 split = 7

    :param df: DataFrame with raw prices along with dividend and split_ratio
        values
    :return: DataFrame with the addition of the adjusted price column
    """

    # Reverse the DataFrame order, sorting by date in descending order
    df.sort_values(by='date', ascending=False, inplace=True)

    split_col = df['split']
    dividend_col = df['dividend']

    for column in ['open', 'high', 'low', 'close']:
        # price_col = list(map(lambda x: float(x), df[column].values))
        df[column].replace(0.0, np.nan, inplace=True)
        df[column].fillna(method='backfill', inplace=True)
        price_col = df[column].values
        adj_price_col = np.zeros(len(df.index))
        adj_price_col[0] = price_col[0]

        adj_column = 'adj_' + column

        for i in range(1, len(price_col)):
            adj_price_col[i] = \
                round((adj_price_col[i - 1] + adj_price_col[i - 1] *
                       (((price_col[i] * (1 / split_col[i - 1])) -
                         price_col[i - 1] -
                         dividend_col[i - 1]) / price_col[i - 1])), 4)
        df[adj_column] = adj_price_col

    # Change the DataFrame order back to dates ascending
    df.sort_values(by='date', ascending=True, inplace=True)

    # drop redundant columns
    df.drop(['open', 'high', 'low', 'close'], axis=1, inplace=True)
    df.rename(columns={'adj_open': 'open', 'adj_high': 'high', 'adj_low': 'low', 'adj_close': 'close'}, inplace=True)
    cols = ['tsid', 'open', 'high', 'low', 'close', 'volume', 'split', 'dividend']
    df = df[cols]
    return df
