from pymongo import MongoClient


def get_mongo_client(username, password, clustername, clusterlink, clientname):
    connection_string = f"mongodb+srv://{username}:{password}@{clustername}.{clusterlink}.mongodb.net/?retryWrites=true&w=majority"
    client = MongoClient(connection_string)
    return client[clientname]

