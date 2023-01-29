import process
import config


def create_database():
    try:
        our_database = process.builder_database(config.mongo_username,
                                                config.mongo_password,
                                                config.mongo_clustername,
                                                config.mongo_clusterlink,
                                                config.mongo_clientname,
                                                config.mongo_collectionname,
                                                config.data_path)
        return our_database
    except Exception as ex:
        return print(str(ex))


def converting_discounted_price_to_tl_from_rupi():
    try:
        converted_discounted_price = process.converter_of_discounted_price_to_tl_from_rupi(config.mongo_username,
                                                                                           config.mongo_password,
                                                                                           config.mongo_clustername,
                                                                                           config.mongo_clusterlink,
                                                                                           config.mongo_clientname,
                                                                                           config.mongo_collectionname)
        return converted_discounted_price
    except Exception as ex:
        return print(str(ex))


def converting_actual_price_to_tl_from_rupi():
    try:
        converted_actual_price = process.converter_of_actual_price_to_tl_from_rupi(config.mongo_username,
                                                                                   config.mongo_password,
                                                                                   config.mongo_clustername,
                                                                                   config.mongo_clusterlink,
                                                                                   config.mongo_clientname,
                                                                                   config.mongo_collectionname)
        return converted_actual_price
    except Exception as ex:
        return print(str(ex))


converting_discounted_price_to_tl_from_rupi()
