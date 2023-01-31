from pymongo import MongoClient
from bson.json_util import dumps


def get_mongo_client(username, password, clustername, clusterlink, clientname):
    # Initializing connection for mongodb
    connection_string = f"mongodb+srv://{username}:{password}@{clustername}.{clusterlink}.mongodb.net/?retryWrites=true&w=majority"
    client = MongoClient(connection_string)
    return client[clientname]


def get_data_from_mongodb(username, password, clustername, clusterlink, clientname, collectionname):
    # Accessing mongodb database
    database = get_mongo_client(username, password, clustername, clusterlink, clientname)

    # Deciding collection
    default_collection = database[collectionname]

    # Transforming data to JSON file
    data = default_collection.find()
    mongodb_docs = list(data)
    json_data = dumps(mongodb_docs)

    return json_data
