from helpers.mongodb import get_mongo_client


def converter_of_prices_to_next_from_current(username, password, clustername, clusterlink, clientname,
                                             collectionname, column1, column2, symbol_of_current_currency,
                                             symbol_of_new_currency, exchange_rate):
    # Accessing mongodb database
    database = get_mongo_client(username, password, clustername, clusterlink, clientname)

    # Deciding collection
    collection = database[collectionname]

    # Converting currency columns
    collection.update_many({f"{column1}": {"$ne": ""}}, {"$mul": {f"{column1}": exchange_rate}})
    collection.update_many({f"{column2}": {"$ne": ""}}, {"$mul": {f"{column2}": exchange_rate}})

    return print(
        f"{column1} and {column2} are converted to {symbol_of_new_currency} from {symbol_of_current_currency}")

