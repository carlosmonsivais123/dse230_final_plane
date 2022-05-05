# Libararies used for GCP_File_Upload.py
from socket import timeout
from google.cloud import storage

class GCP_Functions:
    '''
    Class --> GCP_Functions:
                    This class is in charge of creating the client type for your GCP account. Afterwards, using that client value
                    to upload files up to a specified GCP bucket.

                    Input Variables:
                        None
    '''

    def client_var(self, gcp_credentials):
        '''
        Function --> client_vars:          
                        This function will create the client driver necessary to access the GCP account API.

                        Input Variables
                                1. gcp_credentials: The json key produced in GCP giving you access to the API.

                        Output Variables
                                1. client: The client interface variable to acces the API's in GCP.
        '''
        # Creating the client variable that will be used as a driver to use the GCP API.
        client = storage.Client.from_service_account_json('{}'.format(gcp_credentials))

        # Returns the client driver.
        return client


    def upload_to_gcp_bucket(self, client, bucket_name, blob_name, path_to_file):
        '''
        Function --> upload_to_gcp_bucket:          
                        This function sends the specified file into the specified GCP bucket where it will be stored.

                        Input Variables
                                1. client: The client driver created in the function above used to access the GCP API.
                                2. bucket_name: The GCP bucket name where we will place all teh files.
                                3. blob_name: The filename of the file that is being uploaded.
                                4. path_to_file: Local path to the file that is being uploaded.

                        Output Variables
                                1. file_upload_message: Prints out the file has been succesfully uploaded at the specified location in the GCP bucket.
        '''
        bucket = client.get_bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.upload_from_filename(path_to_file, 
                                  timeout = 1200)

        file_upload_message = '{} has been uplaoded succesfully at {}'.format(blob_name, blob.public_url)

        return print(file_upload_message)