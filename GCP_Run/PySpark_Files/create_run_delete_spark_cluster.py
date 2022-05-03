from google.cloud import dataproc_v1
from google.cloud import storage
from google.oauth2 import service_account
import re

class Dataproc_Spark:
    '''
    Class --> Dataproc_Spark:
                    This class is in charge of creating a PySpark Cluster, running a PySpark job, and then deleting the PySpark cluster that was
                    was just created.

                    Input Variables:
                        None
    '''

    def create_spark_cluster(self, project_id, region, cluster_name, gcp_credentials):
        '''
        Function --> create_spark_cluster:          
                        This function creates a spark cluster using the specified machine instances we are setting up.

                        Input Variables
                                1. project_id: The specified project ID from the GCP account.
                                2. region: Specified region where cluster will run, for example 'us-central1'
                                3. cluster_name: The specified name we will asssign to this cluster.
                                4. gcp_credentials: The json key produced in GCP giving you access to the API.

                        Output Variables
                                None
        '''
        # Creates credentials using the gcp_credential input variables.
        credentials = service_account.Credentials.from_service_account_file('{}'.format(gcp_credentials))

        # Create the cluster client.
        cluster_client = dataproc_v1.ClusterControllerClient(credentials = credentials,
                                                             client_options={"api_endpoint": "{}-dataproc.googleapis.com:443".format(region)})

        # Create the cluster config.
        cluster = {"project_id": project_id,
                   "cluster_name": cluster_name,
                   "config": {"master_config": {"num_instances": 1, "machine_type_uri": "n1-standard-4"},
                              "worker_config": {"num_instances": 2, "machine_type_uri": "n1-standard-4"},
                              "endpoint_config": {"enable_http_port_access": True}, 
                              "software_config": {"image_version": "2.0"}}}

        # Create the cluster.
        operation = cluster_client.create_cluster(request={"project_id": project_id, 
                                                           "region": region, 
                                                           "cluster": cluster})
        result = operation.result()
        print("Cluster created successfully: {}".format(result.cluster_name))


    def run_spark(self, project_id, region, cluster_name, gcp_credentials):
        '''
        Function --> run_spark:          
                        This function runs the PySpark code we wrote and stored in the GCS bucket at the specifed locations below.

                        Input Variables
                                1. project_id: The specified project ID from the GCP account.
                                2. region: Specified region where cluster will run, for example 'us-central1'
                                3. cluster_name: The specified name we will asssign to this cluster.
                                4. gcp_credentials: The json key produced in GCP giving you access to the API.

                        Output Variables
                                None
        '''
        # Creates credentials using the gcp_credential input variables.
        credentials = service_account.Credentials.from_service_account_file('{}'.format(gcp_credentials))

        # Create the job client.
        job_client = dataproc_v1.JobControllerClient(credentials = credentials, 
                                                     client_options={"api_endpoint": "{}-dataproc.googleapis.com:443".format(region)})

        # Create the job config.
        job = {"placement": {"cluster_name": cluster_name},
              "pyspark_job": {"main_python_file_uri": "gs://plane-pyspark-run/DataProc_Files/dataproc_main.py",
                              "python_file_uris": ["gs://plane-pyspark-run/DataProc_Files/dataproc_spark_session.py", 
                                                   "gs://plane-pyspark-run/DataProc_Files/dataproc_spark_commands.py"]}}

        operation = job_client.submit_job_as_operation(request={"project_id": project_id, 
                                                                "region": region, 
                                                                "job": job})
        response = operation.result()

        # Dataproc job output is saved to the Cloud Storage bucket
        # allocated to the job. Use regex to obtain the bucket and blob info.
        matches = re.match("gs://(.*?)/(.*)", response.driver_output_resource_uri)

        client = storage.Client.from_service_account_json('{}'.format(gcp_credentials))
        output = (client.get_bucket(matches.group(1)).blob(f"{matches.group(2)}.000000000").download_as_string())

        print(f"Job finished successfully: {output}\r\n")


    def delete_cluster(self, project_id, region, cluster_name, gcp_credentials):
        '''
        Function --> delete_cluster:          
                        This function deletes the cluster that was created above since we are done with it after the PySpark calcualtions 
                        have been run in order to not keep getting charged.

                        Input Variables
                                1. project_id: The specified project ID from the GCP account.
                                2. region: Specified region where cluster will run, for example 'us-central1'
                                3. cluster_name: The specified name we will asssign to this cluster.
                                4. gcp_credentials: The json key produced in GCP giving you access to the API.

                        Output Variables
                                None
        '''
        # Creates credentials using the gcp_credential input variables.
        credentials = service_account.Credentials.from_service_account_file('{}'.format(gcp_credentials))

        # Create the cluster client.
        cluster_client = dataproc_v1.ClusterControllerClient(credentials = credentials,
                                                             client_options={"api_endpoint": "{}-dataproc.googleapis.com:443".format(region)})

        # Delete the clusted client that is specified.
        operation = cluster_client.delete_cluster(request = {"project_id": project_id,
                                                             "region": region,
                                                             "cluster_name": cluster_name})
        operation.result()
        print("Cluster {} successfully deleted.".format(cluster_name))