# Transactions

read_transaction_data.py and calculate_cashback.py use **pandas**.

read_transaction_data.py does the following:

* reads and transforms raw dataset containing transaction info, then saves it to *data-selected.csv*
* reads and transforms dataset containing all mcc codes and mcc groups, then saves it to *mcc-data-transformed.csv*
* generates or reads client status info
* generates a pair of current cashback category + discount for each client and saves it to *full-client-info.csv*
* generates a random mcc code for each transaction
* adds a real mcc group corresponding to each transaction's mcc code
* adds client's cashback category and discount columns to transacton dataset and saves it to *data-transformed.csv*

calculate_cashback.py does the following:

* gives user a list of clients to choose from
* calculates client's cashback for each purchase based on its mcc group and client's current cashback category + discount
* adds and fills 'cashback' column in given dataframe

spark_transform_data.py uses **spark** and does the following:

* reads preprocessed dataset containing transactions
* gives user a list of clients to choose from
* filters dataset by client id

mllib_classifier.py has two ml algorithms based on **spark mllib**, but is not currently working.

## Next steps:

* find out a way to transform .csv dataset to libsvm to be able to use those ml algorithms
* train and test models that those algorithms give
