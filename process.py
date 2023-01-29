import pandas as pd

from helpers.mongodb import get_mongo_client


def builder_database(username, password, clustername, clusterlink, clientname, mongo_collectionname, data_path):
    # Accessing database
    database = get_mongo_client(username, password, clustername, clusterlink, clientname)
    # Deciding collection
    collection = database[mongo_collectionname]

    # Defining id for data
    csv_filename = data_path
    df = pd.read_csv(csv_filename)

    # Converting data to dictionary from csv
    data = df.to_dict('records')

    # Creating documents for mongodb
    collection.insert_many(data)

    return print(f"{mongo_collectionname} is created")


def converter_of_discounted_price_to_tl_from_rupi(username, password, clustername, clusterlink, clientname,
                                                  mongo_collectionname):
    # Accessing database
    database = get_mongo_client(username, password, clustername, clusterlink, clientname)
    # Deciding collection
    collection = database[mongo_collectionname]

    # Converting discounted prices to tl from rupi

    # Extract-Transform data
    old_discounted_price_values = collection.aggregate(
        [{"$project": {"_id": 0, "old_discounted_price": {"$split": ["$discounted_price", "₹"]}}},
         {"$unwind": "$old_discounted_price"},
         {"$match": {"old_discounted_price": {"$ne": ""}}},
         {"$project": {"values": {"$convert": {"input": {
             "$reduce": {"input": {"$split": ['$old_discounted_price', ',']}, "initialValue": '',
                         "in": {"$concat": ['$$value', '$$this']}}}, "to": "double", "onError": 0}}}},
         {"$addFields": {"exchange_rate_of_rupi_to_tl": 4.33}},
         {"$project": {"_id": 0,
                       "new_discounted_price": {"$divide": ["$values", "$exchange_rate_of_rupi_to_tl"]}}},
         {"$merge": {"into": f"{mongo_collectionname}", "whenMatched": "replace"}}
         ])

    return print("Discounted price is converted to tl from rupi")


def converter_of_actual_price_to_tl_from_rupi(username, password, clustername, clusterlink, clientname,
                                              mongo_collectionname):
    # Accessing database
    database = get_mongo_client(username, password, clustername, clusterlink, clientname)
    # Deciding collection
    collection = database[mongo_collectionname]

    # Getting actual prices from mongodb
    actual_price_col = collection.find({}, {"_id": 0, "actual_price": 1})

    # Converting discounted prices to tl from rupi

    # Extract-Transform data
    old_actual_price_values = collection.aggregate(
        [{"$project": {"_id": 0, "old_actual_price": {"$split": ["$actual_price", "₹"]}}},
         {"$unwind": "$old_actual_price"},
         {"$match": {"old_actual_price": {"$ne": ""}}},
         {"$project": {"values": {"$convert": {"input": {
             "$reduce": {"input": {"$split": ['$old_actual_price', ',']}, "initialValue": '',
                         "in": {"$concat": ['$$value', '$$this']}}}, "to": "double", "onError": 0}}}},
         {"$addFields": {"exchange_rate_of_rupi_to_tl": 4.33}},
         {"$project": {"_id": 0,
                       "new_actual_price": {"$divide": ["$values", "$exchange_rate_of_rupi_to_tl"]}}},
         {"$merge": {"into": f"{mongo_collectionname}", "whenMatched": "replace"}}
         ])

    return print("Actual price is converted to tl from rupi")
