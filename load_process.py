from google.cloud import storage
from helpers.googlecloud import get_google_cloud_environment
from helpers.mongodb import get_data_from_mongodb


def loading_data_to_google_cloud_from_mongodb(username, password, clustername, clusterlink, clientname,
                                              collectionname, google_cloud_service_key, bucket_name, blob_name):
    # Accessing google cloud database
    get_google_cloud_environment(google_cloud_service_key)

    # Accessing storage
    storage_client = storage.Client()

    # Deciding bucket
    bucket = storage_client.get_bucket(bucket_name)

    # Creating blob
    blob = bucket.blob(blob_name)
    blob.upload_from_string(get_data_from_mongodb(username, password, clustername, clusterlink, clientname,
                                                  collectionname))

    return print(f"Blob named {blob_name} has been created in bucket named {bucket_name}")
