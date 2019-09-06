# transactions

There's just one .py file that transforms initial dataset fount on the web.
read_transaction_data.py does the following:

* read and transform raw dataset containing transaction info
* read and transform dataset containing all mcc codes and mcc groups
* generate or read client status info (it is now there)
* generate dataset containing cashback categories and discounts that each client has at the moment
* generate a random mcc code for each transaction
* add a real mcc group corresponding to each transaction's mcc code

##Next steps:

* calculate chashback sum based on each transaction's mcc group and client's current cashback categories
* find out suitable ml algorithm that will give a future category to each client based on received cashback
