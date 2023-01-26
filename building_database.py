import csv
import config
import pandas as pd
from helpers.mongodb import get_mongo_client

database = get_mongo_client(config.mongo_username, config.mongo_password, config.mongo_clustername,
                            config.mongo_clusterlink, config.mongo_clientname)

collection = database[config.mongo_collectionname]

count = 1
csv_filename = config.data_path
df = pd.read_csv(csv_filename)
df["_id"] = ""
for x in range(len(df.axes[0])-1):
    df.at[count-1, "_id"] = count
    count = count + 1

df.to_csv(csv_filename, index=False)

with open(csv_filename, encoding="utf8") as f:
    reader = csv.DictReader(f)
    for row in reader:
        collection.insert_one(row)


