from rest_framework import status
from rest_framework.decorators import api_view
from rest_framework.response import Response
from django.conf import settings
from transactions.utils import get_last_n_days_transaction
import pandas as pd
import os

@api_view(['GET'])
def getTransactionByID(request, transaction_id):
    try:
        # looping over transaction files inside the INBOUND_TRANSACTIONS_DIR and checking if transaction is available or not
        for filename in os.listdir(settings.INBOUND_TRANSACTIONS_DIR):
            f = os.path.join(settings.INBOUND_TRANSACTIONS_DIR, filename)
            # checking if it is a file and filename startswith 'Transaction'
            if os.path.isfile(f) and filename[:11] == 'Transaction':
                df = pd.read_csv(f)
                transaction_info = df.loc[df['transactionId'] == transaction_id]

                if not transaction_info.empty:
                    break
        
        # returning the response based on transaction availability
        if transaction_info.empty:
            return Response({"message": 'No transactions found with the transaction_id specified'}, status=status.HTTP_400_BAD_REQUEST)   

        producctId = transaction_info['productId'].values[0]
        product_info = pd.read_csv(settings.PRODUCT_REFERENCE_PATH).loc[df['productId'] == producctId]

        body = { 
                "transactionId": transaction_id, 
                "productName": product_info['productName'].values[0], 
                "transactionAmount": transaction_info['transactionAmount'].values[0], 
                "transactionDatetime": transaction_info['transactionDatetime'].values[0]
            }
        statuscode=status.HTTP_200_OK
        
    except Exception as e:
        body = { 'message': 'Something went wrong, Plesae try again!' }
        statuscode = status.HTTP_500_INTERNAL_SERVER_ERROR

    return Response(body, status=statuscode)


@api_view(['GET'])
def transactionSummaryByProducts(request, last_n_days):
    try:
        # getting list of transactions for last n days and list products for reference
        transactions_df, products_df = get_last_n_days_transaction(last_n_days)

        # aggregating the list of trasactions by dropping the duplicates by productsId and getting the transactionAmount summed
        transactions_df = transactions_df.drop_duplicates().groupby('productId',sort=False,as_index=False).agg(totalAmount=('transactionAmount', 'sum'))
        
        # merging the transactions df and products df based on productId and dropping the unnecessary columns
        mergedDF = pd.merge(transactions_df, products_df, left_on='productId', right_on='productId', how='left').drop(['productId', 'productManufacturingCity'], axis=1)

        # converting the dataframe to dictionry based key value pairs
        body = { "summary": mergedDF.to_dict(orient='records') }
        statuscode=status.HTTP_200_OK
    except Exception as e:
        body = { 'message': 'Something went wrong, Plesae try again!' }
        statuscode = status.HTTP_500_INTERNAL_SERVER_ERROR

    return Response(body, status=statuscode)

@api_view(['GET'])
def transactionSummaryByManufacturingCity(request, last_n_days):
    try:
        # getting list of transactions for last n days and list products for reference
        transactions_df, products_df = get_last_n_days_transaction(last_n_days)

        # merging the transactions df and products df based on productId
        mergedDF = pd.merge(transactions_df, products_df, left_on='productId', right_on='productId', how='left')

        # aggregating the merged datafame by dropping duplicates by productManufacturingCity and getting the transactionsAmount summed for each productManufacturingCity
        mergedDF = mergedDF.drop_duplicates().groupby('productManufacturingCity',sort=False,as_index=False).agg(totalAmount=('transactionAmount', 'sum'))
        
        # converting the dataframe to dictionry based key value pairs
        body = { "summary": mergedDF.to_dict(orient='records')}
        statuscode=status.HTTP_200_OK
    except Exception as e:
        body = { 'message': 'Something went wrong, Plesae try again!' }
        statuscode = status.HTTP_500_INTERNAL_SERVER_ERROR

    return Response(body, status=statuscode)










