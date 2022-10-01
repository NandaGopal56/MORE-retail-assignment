# Dynamic data handling using Python-Flask or Djnago application
1. This application populates and provides retrieval features for transactions of a company.
2. Transaction information is coming as files (let’s say every 5 minutes) in a folder.
3. Another folder contains a file, which contains a reference data for products, against which the
transaction are happening.
4. This application is an in-memory application so no persistent storage is required. i.e. You can
reload the already available data in the transaction folder upon start-up of the application.

5. A transaction record contains following attributes in a comma separated format
a. transactionId
b. productId
c. transactionAmount
d. transactionDatetime

6. The product reference data have following attributes in a CSV.
a. productId
b. productName
c. productManufacturingCity

7. Reference data is static and transaction data is keep coming in real-time in their respective
folders.


# Application functionalities:
1. Processes the data in streaming fashion, meaning provides up to date view for below mentioned
REST calls.

2. Following REST APIs should be implemented.
3. 
a. GET request http://localhost:8080/assignment/transaction/{transaction_id}
i. Type: GET
ii. Output data JSON: { “transactionId”: 1, “productName”: “P1”,
“transactionAmount”: 1000.0, “transactionDatetime”: “2018-01-01 10:10:10”}

b. GET request http://localhost:8080/assignment/transactionSummaryByProducts/{last_n_days}
i. Type: GET
ii. Output data JSON: { “summary”: [ {“productName”: “P1”, {“totalAmount”:
3000.0}]}

c. GET request http://localhost:8080/assignment/transactionSummaryByManufacturingCity/{last_n_days}
i. Type: GET
ii. Output data JSON: { “summary”: [ {“cityName”: “C1”, {“totalAmount”: 3000.0}]}
