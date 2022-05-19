import os

# GCP_Functions class from the gcp_functions.py file
from GCP_Functions.GCP_File_Upload import GCP_Functions

# Variables from the read_vars.py file.
from Input_Variables.read_vars import gcp_bucket_name, gcp_credentials

#################################################### Send Files to GCP Bucket ####################################################
# Initiating the GCP_Functions() class from the gcp_functions.py file.
gcp_functions = GCP_Functions()
client = gcp_functions.client_var(gcp_credentials = gcp_credentials)

# List of Files we will use in the PySpark DataProc clusters.
dataproc_file_list = ['eda_dataproc_main.py', 'eda_dataproc_spark_commands.py', 'dataproc_spark_session.py']

# Other files we want to upload such as the data in the flights.csv file and the requirements.txt file.
other_file_list = ['requirements.txt', 'flight-delays/airlines.csv', 'flight-delays/airports.csv', 'flight-delays/flights.csv']

# Uploading DataProc Files
for file in dataproc_file_list:
    gcp_functions.upload_to_gcp_bucket(client = client, 
                                       bucket_name = '{}'.format(gcp_bucket_name), 
                                       blob_name = 'DataProc_Files/{}'.format(file), 
                                       path_to_file = '{}/DataProc_Files/{}'.format(os.getcwd(), file))

# Uploading Other Files
for file in other_file_list:
    gcp_functions.upload_to_gcp_bucket(client = client, 
                                       bucket_name = '{}'.format(gcp_bucket_name), 
                                       blob_name = '{}'.format(file), 
                                       path_to_file = '{}/{}'.format(os.path.normpath(os.getcwd() + os.sep + os.pardir), file))

if __name__ == '__main__':
    print("Sending Files to GCP")