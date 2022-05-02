from google.cloud import dataproc_v1
from google.cloud import storage
from google.oauth2 import service_account


class Dataproc_Spark:
    def create_spark_cluster(self, project_id, region, cluster_name, gcp_credentials, requirements_file):
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