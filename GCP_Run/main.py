# GCP_Functions class from the gcp_functions.py file
from GCP_Functions.GCP_File_Upload import GCP_Functions

# Dataproc_Spark class from the create_spark_cluster.py file.
from PySpark_Files.create_run_delete_spark_cluster import Dataproc_Spark

# Variables from the read_vars.py file.
from Input_Variables.read_vars import project_id, region, cluster_name, gcp_bucket_name, gcp_credentials

# Plotting class to create plots based on PySpark output.
from EDA_Plots.eda_plots import EDA_Plots


#################################################### Send Files to GCP Bucket ####################################################
print("Sending Files to GCP")

# Initiating the GCP_Functions() class from the gcp_functions.py file.
gcp_functions = GCP_Functions()
client = gcp_functions.client_var(gcp_credentials = gcp_credentials)

# List of Files we will use in the PySpark DataProc clusters.
dataproc_file_list = ['dataproc_main.py', 'dataproc_spark_commands.py', 'dataproc_spark_session.py']

# Other files we want to upload such as the data in the flights.csv file and the requirements.txt file.
other_file_list = ['requirements.txt', 'flight-delays/flights.csv']

# Uploading DataProc Files
for file in dataproc_file_list:
    gcp_functions.upload_to_gcp_bucket(client = client, 
                                       bucket_name = '{}'.format(gcp_bucket_name), 
                                       blob_name = 'DataProc_Files/{}'.format(file), 
                                       path_to_file = '/Users/CarlosMonsivais/Desktop/dse230_plane/GCP_Run/DataProc_Files/{}'.format(file))

# Uploading Other Files
for file in other_file_list:
    gcp_functions.upload_to_gcp_bucket(client = client, 
                                       bucket_name = '{}'.format(gcp_bucket_name), 
                                       blob_name = '{}'.format(file), 
                                       path_to_file = '/Users/CarlosMonsivais/Desktop/dse230_plane/{}'.format(file))


#################################################### Create Spark Cluster ####################################################
print('\nCreating Pyspark Cluster in Dataproc')

# Initiating the Dataproc_Spark() class from the create_spar_cluster.py file.
dataproc_spark = Dataproc_Spark()

# Creating the PySpark cluster on DataProc.
dataproc_spark.create_spark_cluster(project_id = project_id, 
                                    region = region, 
                                    cluster_name = cluster_name,
                                    gcp_credentials = gcp_credentials)
print('\nCluster has been created')


#################################################### Run Spark Job ####################################################
print('\nRunning PySpark Code.')

# Running the PySpark code on the cluster we created above.
dataproc_spark.run_spark(project_id = project_id, 
                         region = region, 
                         cluster_name = cluster_name,
                         gcp_credentials = gcp_credentials)


#################################################### Delete Spark Cluster ####################################################
print('\nDeleting PySpark Cluster.')

# Deleting the PySpark cluster we created above.
dataproc_spark.delete_cluster(project_id = project_id, 
                              region = region, 
                              cluster_name = cluster_name,
                              gcp_credentials = gcp_credentials)


#################################################### EDA Plots ####################################################
print('\nCreating EDA Plots and sending them to GCP')

# Initiating the GCP_Functions() class from the gcp_functions.py file.
eda_plots = EDA_Plots(bucket_name = gcp_bucket_name,
                      gcp_credentials = gcp_credentials,
                      client = client)

eda_plots.origin_destination_airport_counts_plotly()
eda_plots.corr_matrix_plotly()
eda_plots.summary_table_plotly()
eda_plots.null_values_counts_plotly()
eda_plots.pairplot_plotly()