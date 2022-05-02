from create_spark_cluster import Dataproc_Spark
from read_vars import project_id, region, cluster_name, gcp_credentials, gcp_bucket_name
from gcp_functions import GCP_Functions

# # Send Files to GCP Bucket
# print("Sending Files to GCP")
# gcp_functions = GCP_Functions()
# client = gcp_functions.client_var(gcp_credentials = gcp_credentials)

# file_list = ['install.sh', 
#              'create_plots.py', 
#              'spark_session.py', 
#              'flight-delays/flights.csv', 
#              'requirements.txt']
# for file in file_list:
#     gcp_functions.upload_to_gcp_bucket(client = client, 
#                                        bucket_name = gcp_bucket_name, 
#                                        blob_name = '{}'.format(file), 
#                                        path_to_file = '/Users/CarlosMonsivais/Desktop/dse230_plane/{}'.format(file))

# # Create Spark Cluster
# print('\nCreating Pyspark Cluster in Dataproc')
# with open('requirements.txt') as f:
#     lines = f.readlines()
# stripped_line = [s.rstrip() for s in lines]
# joined_string = " ".join(stripped_line)

# dataproc_spark = Dataproc_Spark()
# dataproc_spark.create_spark_cluster(project_id = project_id, 
#                                     region = region, 
#                                     cluster_name = cluster_name,
#                                     gcp_credentials = gcp_credentials,
#                                     requirements_file = joined_string)


# Send Files to GCP Bucket
print("Sending Files to GCP")
gcp_functions = GCP_Functions()
client = gcp_functions.client_var(gcp_credentials = gcp_credentials)

file_list = ['create_plots.py', 
             'spark_session.py']
for file in file_list:
    gcp_functions.upload_to_gcp_bucket(client = client, 
                                       bucket_name = gcp_bucket_name, 
                                       blob_name = '{}'.format(file), 
                                       path_to_file = '/Users/CarlosMonsivais/Desktop/dse230_plane/{}'.format(file))