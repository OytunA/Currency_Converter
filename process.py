import csv
import pandas as pd
from helpers.mongodb import get_mongo_client


def builder_database(username, password, clustername, clusterlink, clientname, mongo_collectionname, data_path):
    # Accessing database
    database = get_mongo_client(username, password, clustername, clusterlink, clientname)
    # Deciding collection
    collection = database[mongo_collectionname]

    # Defining id's for data
    count = 1
    csv_filename = data_path
    df = pd.read_csv(csv_filename)
    df["_id"] = ""
    for x in range(len(df.axes[0]) - 1):
        df.at[count - 1, "_id"] = count
        count = count + 1
    df.to_csv(csv_filename, index=False)

    # Creating documents for mongodb
    with open(csv_filename, encoding="utf8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            collection.insert_one(row)

    return print(f"{mongo_collectionname} is created")


def converter_of_discounted_price_to_tl_from_rupi(username, password, clustername, clusterlink, clientname,
                                                  mongo_collectionname):
    # Accessing database
    database = get_mongo_client(username, password, clustername, clusterlink, clientname)
    # Deciding collection
    collection = database[mongo_collectionname]

    # Getting discounted prices from mongodb
    discounted_price_col = collection.find({}, {"_id": 0, "discounted_price": 1})

    # Converting discounted prices to tl from rupi
    count = 1
    for x in discounted_price_col:
        discounted_price_column = {"_id": f"{count}"}
        my_converted_values = float(x.get("discounted_price").replace("₹", "").replace(",", "")) * 4.33
        converted_currency_to_tl_for_discounted_price = {"$set": {"discounted_price": f"₺{my_converted_values}"}}
        collection.update_one(discounted_price_column, converted_currency_to_tl_for_discounted_price)
        count = count + 1

    return print("Discounted price is converted to tl from rupi")


def converter_of_actual_price_to_tl_from_rupi(username, password, clustername, clusterlink, clientname,
                                              mongo_collectionname):
    # Accessing database
    database = get_mongo_client(username, password, clustername, clusterlink, clientname)
    # Deciding collection
    collection = database[mongo_collectionname]

    # Getting actual prices from mongodb
    actual_price_col = collection.find({}, {"_id": 0, "actual_price": 1})

    # Converting actual prices to tl from rupi
    count = 1
    for x in actual_price_col:
        actual_price_column = {"_id": f"{count}"}
        my_converted_values = float(x.get("actual_price").replace("₹", "").replace(",", "")) * 4.33
        converted_currency_to_tl_for_actual_price = {"$set": {"actual_price": f"₺{my_converted_values}"}}
        collection.update_one(actual_price_column, converted_currency_to_tl_for_actual_price)
        count = count + 1

    return print("Actual price is converted to tl from rupi")
