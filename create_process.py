import pandas as pd
from helpers.googlecloud import get_google_cloud_environment
from helpers.mongodb import get_mongo_client
from google.cloud import storage


def create_a_database_in_mongodb(username, password, clustername, clusterlink, clientname, collectionname, data_path,
                                 column1, column2, symbol_of_current_currency):
    # Accessing mongodb database
    database = get_mongo_client(username, password, clustername, clusterlink, clientname)
    # Deciding collection
    default_collection = database[collectionname]

    # Using pandas to transform data for converting process
    csv_filename = data_path
    df = pd.read_csv(csv_filename)
    cols_to_check = [f"{column1}", f"{column2}"]
    df[cols_to_check] = df[cols_to_check].replace({f'{symbol_of_current_currency}': ''},
                                                  regex=True).replace({',': ''}, regex=True).astype(float)

    # Converting data to dictionary from csv
    data = df.to_dict('records')

    # Creating documents for mongodb
    default_collection.insert_many(data)

    return print(f"Collection named {collectionname} has been created in mongodb")


def create_a_bucket_in_google_cloud(google_cloud_service_key, bucket_name):
    # Accessing google cloud database
    get_google_cloud_environment(google_cloud_service_key)

    # Accessing storage
    storage_client = storage.Client()

    # Creating bucket
    new_bucket_name = bucket_name
    storage_client.create_bucket(new_bucket_name)

    return print(f"Bucket named {bucket_name} has been created in google cloud storage")
