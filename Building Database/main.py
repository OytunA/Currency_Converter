import csv
import config
from helpers.mongodb import get_mongo_client

db = get_mongo_client(config.mongo_username, config.mongo_password, config.mongo_clustername, config.mongo_clusterlink,
                      config.mongo_clientname)

collection = db[config.mongo_collectionname]

csv_filename = config.data_path
with open(csv_filename, encoding="utf8") as f:
    reader = csv.DictReader(f)
    for row in reader:
        collection.insert_one(row)
