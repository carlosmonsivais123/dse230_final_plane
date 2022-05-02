from google.cloud import dataproc_v1
from google.cloud import storage
from google.oauth2 import service_account
import re



class Dataproc_Spark:
    def create_spark_cluster(self, project_id, region, cluster_name, gcp_credentials):
        credentials = service_account.Credentials.from_service_account_file('{}'.format(gcp_credentials))

        # Create the cluster client.
        cluster_client = dataproc_v1.ClusterControllerClient(credentials = credentials,
                                                             client_options={"api_endpoint": "{}-dataproc.googleapis.com:443".format(region)})

        # Create the cluster config.
        cluster = {"project_id": project_id,
                   "cluster_name": cluster_name,
                   "config": {"master_config": {"num_instances": 1, "machine_type_uri": "n1-standard-4"},
                              "worker_config": {"num_instances": 2, "machine_type_uri": "n1-standard-4"}, 
                              "software_config": {"image_version": "2.0"}}}

        # Create the cluster.
        operation = cluster_client.create_cluster(request={"project_id": project_id, 
                                                          "region": region, 
                                                          "cluster": cluster})
        result = operation.result()
        print("Cluster created successfully: {}".format(result.cluster_name))


    def run_spark(self, region, cluster_name, project_id, gcp_credentials):
        credentials = service_account.Credentials.from_service_account_file('{}'.format(gcp_credentials))

        # Create the job client.
        job_client = dataproc_v1.JobControllerClient(credentials = credentials, 
                                                    client_options={"api_endpoint": "{}-dataproc.googleapis.com:443".format(region)})

        # Create the job config.
        job = {"placement": {"cluster_name": cluster_name},
              "pyspark_job": {"main_python_file_uri": "gs://plane-pyspark-run/create_plots.py",
                              "python_file_uris": ["gs://plane-pyspark-run/spark_session.py"]}}

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

    def delete_cluster(self, gcp_credentials, project_id, region, cluster_name):
        credentials = service_account.Credentials.from_service_account_file('{}'.format(gcp_credentials))

        # Create the cluster client.
        cluster_client = dataproc_v1.ClusterControllerClient(credentials = credentials,
                                                             client_options={"api_endpoint": "{}-dataproc.googleapis.com:443".format(region)})


        operation = cluster_client.delete_cluster(request = {"project_id": project_id,
                                                             "region": region,
                                                             "cluster_name": cluster_name})
        operation.result()
        print("Cluster {} successfully deleted.".format(cluster_name))