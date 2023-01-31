import os


def get_google_cloud_environment(google_cloud_service_key):
    # Initializing connection for mongodb
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = google_cloud_service_key


