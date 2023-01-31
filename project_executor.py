import config
import create_process
import extract_and_transform_process
import load_process


def create_database_in_mongodb():
    try:
        our_database = create_process.create_a_database_in_mongodb(config.mongo_username,
                                                                   config.mongo_password,
                                                                   config.mongo_clustername,
                                                                   config.mongo_clusterlink,
                                                                   config.mongo_clientname,
                                                                   config.mongo_collectionname,
                                                                   config.data_path, config.column_to_convert_1,
                                                                   config.column_to_convert_2,
                                                                   config.symbol_of_current_currency)
        return our_database
    except Exception as ex:
        return print(str(ex))


def converting_currencies():
    try:
        converted_discounted_price = extract_and_transform_process. \
            converter_of_prices_to_next_from_current(config.mongo_username, config.mongo_password,
                                                     config.mongo_clustername,
                                                     config.mongo_clusterlink,
                                                     config.mongo_clientname,
                                                     config.mongo_collectionname, config.column_to_convert_1,
                                                     config.column_to_convert_2, config.symbol_of_current_currency,
                                                     config.symbol_of_new_currency,
                                                     config.exchange_rate_current_to_new_currency)
        return converted_discounted_price
    except Exception as ex:
        return print(str(ex))


def creating_database_in_google_cloud():
    try:
        our_database = create_process.create_a_bucket_in_google_cloud(config.google_cloud_service_key,
                                                                      config.bucket_name)
        return our_database
    except Exception as ex:
        return print(str(ex))


def loading_data_to_google_cloud_from_mongodb():
    try:
        loading_process = load_process.loading_data_to_google_cloud_from_mongodb(config.mongo_username,
                                                                                 config.mongo_password,
                                                                                 config.mongo_clustername,
                                                                                 config.mongo_clusterlink,
                                                                                 config.mongo_clientname,
                                                                                 config.mongo_collectionname,
                                                                                 config.google_cloud_service_key,
                                                                                 config.bucket_name, config.blob_name)
        return loading_process
    except Exception as ex:
        return print(str(ex))


create_database_in_mongodb()
converting_currencies()
creating_database_in_google_cloud()
loading_data_to_google_cloud_from_mongodb()

