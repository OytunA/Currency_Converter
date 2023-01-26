import config
from helpers.mongodb import get_mongo_client

database = get_mongo_client(config.mongo_username, config.mongo_password, config.mongo_clustername,
                            config.mongo_clusterlink, config.mongo_clientname)

collection = database[config.mongo_collectionname]

discounted_price_col = collection.find({}, {"_id": 0, "discounted_price": 1})
actual_price_col = collection.find({}, {"_id": 0, "actual_price": 1})

count = 1
for x in discounted_price_col:
    discounted_price_column = {"_id": f"{count}"}
    my_converted_values = float(x.get("discounted_price").replace("₹", "").replace(",", "")) * 4.33
    converted_currency_to_tl_for_discounted_price = {"$set": {"discounted_price": f"₺{my_converted_values}"}}
    collection.update_one(discounted_price_column, converted_currency_to_tl_for_discounted_price)
    count = count + 1

count = 1
for x in actual_price_col:
    actual_price_column = {"_id": f"{count}"}
    my_converted_values = float(x.get("actual_price").replace("₹", "").replace(",", "")) * 4.33
    converted_currency_to_tl_for_actual_price = {"$set": {"actual_price": f"₺{my_converted_values}"}}
    collection.update_one(actual_price_column, converted_currency_to_tl_for_actual_price)
    count = count + 1
