from django.conf import settings
import pandas as pd
import os


def get_last_n_days_transaction(last_n_days):
    df = pd.DataFrame()
    # looping over transaction files inside the INBOUND_TRANSACTIONS_DIR and checking if transaction is available or not
    for filename in os.listdir(settings.INBOUND_TRANSACTIONS_DIR):
        f = os.path.join(settings.INBOUND_TRANSACTIONS_DIR, filename)
        # checking if it is a file and filename startswith 'Transaction'
        if os.path.isfile(f) and filename[:11] == 'Transaction':
           df = df.append(pd.read_csv(f))
    df['transactionDatetime'] =  pd.to_datetime(df['transactionDatetime'], format='%d/%m/%Y %H:%M')

    #set index from column Date
    df = df.set_index('transactionDatetime')
    #if datetimeindex isn't order, order it
    df= df.sort_index()
    #last n days of date lastday
    lastdayfrom = pd.to_datetime(df.index[-1])
    transactions_df = df.loc[lastdayfrom - pd.Timedelta(days=last_n_days):lastdayfrom].reset_index()

    products_df = pd.read_csv(settings.PRODUCT_REFERENCE_PATH)

    return transactions_df, products_df