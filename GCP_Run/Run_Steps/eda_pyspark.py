# Dataproc_Spark class from the create_spark_cluster.py file.
from PySpark_Files.create_run_delete_spark_cluster import Dataproc_Spark

# Variables from the read_vars.py file.
from Input_Variables.read_vars import project_id, region, cluster_name, gcp_bucket_name, gcp_credentials

# Plotting class to create plots based on PySpark output.
from EDA_Plots.eda_plots import EDA_Plots

# GCP_Functions class from the gcp_functions.py file
from GCP_Functions.GCP_File_Upload import GCP_Functions

#################################################### Initialize GCP Client ####################################################
# Initiating the GCP_Functions() class from the gcp_functions.py file.
gcp_functions = GCP_Functions()
client = gcp_functions.client_var(gcp_credentials = gcp_credentials)


#################################################### Create Spark Cluster ####################################################
print('\nEDA: Creating Pyspark Cluster in Dataproc')

# Initiating the Dataproc_Spark() class from the create_spar_cluster.py file.
dataproc_spark = Dataproc_Spark()

# Creating the PySpark cluster on DataProc.
dataproc_spark.create_spark_cluster(project_id = project_id, 
                                    region = region, 
                                    cluster_name = cluster_name,
                                    gcp_credentials = gcp_credentials)
print('\nCluster has been created')


#################################################### Run Spark Job ####################################################
print('\nEDA: Running PySpark Code.')

# Running the PySpark code on the cluster we created above.
dataproc_spark.run_spark(project_id = project_id, 
                         region = region, 
                         cluster_name = cluster_name,
                         gcp_credentials = gcp_credentials,
                         main_pyspark_file = 'gs://plane-pyspark-run/DataProc_Files/eda_dataproc_main.py',
                         other_pyspark_files = ["gs://plane-pyspark-run/DataProc_Files/dataproc_spark_session.py", 
                                                "gs://plane-pyspark-run/DataProc_Files/eda_dataproc_spark_commands.py"])


#################################################### Delete Spark Cluster ####################################################
print('\nEDA: Deleting PySpark Cluster.')

# Deleting the PySpark cluster we created above.
dataproc_spark.delete_cluster(project_id = project_id, 
                              region = region, 
                              cluster_name = cluster_name,
                              gcp_credentials = gcp_credentials)


#################################################### EDA Plots ####################################################
print('\nEDA: Creating EDA Plots and sending them to GCP')

# Initiating the GCP_Functions() class from the gcp_functions.py file.
eda_plots = EDA_Plots(bucket_name = gcp_bucket_name,
                      gcp_credentials = gcp_credentials,
                      client = client)

eda_plots.origin_destination_airport_counts_plotly()
eda_plots.corr_matrix_plotly()
eda_plots.summary_table_plotly()
eda_plots.null_values_counts_plotly()
eda_plots.pairplot_plotly()

if __name__ == '__main__':
    print('EDA PySpark Cluster')