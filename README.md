# Transactions

read_transaction_data.py and calculate_cashback.py use **pandas**.

read_transaction_data.py does the following:

* reads and transforms raw dataset containing transaction info
* reads and transforms dataset containing all mcc codes and mcc groups
* generates or reads client status info (it is now there)
* generates dataset containing cashback categories and discounts that each client has at the moment
* generates a random mcc code for each transaction
* adds a real mcc group corresponding to each transaction's mcc code

calculate_cashback.py does the following:

* gives user a list of clients to choose from
* calculates client's cashback for each purchase based on its mcc group and client's current cashback categories
* adds and fills 'cashback' column in given dataframe

spark_test.py uses **spark** and does the following:

* reads preprocessed dataset containing transactions
* gives user a list of clients to choose from
* filters dataset by client id

## Next steps:

* calculate client's cashback for each purchase based on its mcc group and client's current cashback categories on spark
* add and fill 'cashback' column in given dataframe on spark
* find out suitable ml algorithm that will suggest a future category to each client based on received cashback
