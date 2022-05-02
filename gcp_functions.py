from socket import timeout
from google.cloud import storage

class GCP_Functions:
    def client_var(self, gcp_credentials):
        client = storage.Client.from_service_account_json('{}'.format(gcp_credentials))

        return client

    def upload_to_gcp_bucket(self, client, bucket_name, blob_name, path_to_file):
        bucket = client.get_bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.upload_from_filename(path_to_file, 
                                  timeout = 1200)

        return print('{} has been uplaoded succesfully at {}'.format(blob_name, blob.public_url))