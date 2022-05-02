from google.cloud import dataproc_v1
from google.cloud import storage
from google.oauth2 import service_account
import re

from read_vars import project_id, region, cluster_name, gcp_bucket_name, gcp_credentials

def run_spark(region, cluster_name, gcs_bucket, spark_filename, project_id, gcp_credentials):
    credentials = service_account.Credentials.from_service_account_file('{}'.format(gcp_credentials))

    # Create the job client.
    job_client = dataproc_v1.JobControllerClient(credentials = credentials, 
                                                 client_options={"api_endpoint": "{}-dataproc.googleapis.com:443".format(region)})

    # Create the job config.
    job = {
        "placement": {"cluster_name": cluster_name},
        "pyspark_job": {"main_python_file_uri": "gs://{}/{}".format(gcs_bucket, spark_filename),
                        "python_file_uris": ["gs://dse_230_plane/spark_session.py"]},
    }

    operation = job_client.submit_job_as_operation(
        request={"project_id": project_id, "region": region, "job": job}
    )
    response = operation.result()

    # Dataproc job output is saved to the Cloud Storage bucket
    # allocated to the job. Use regex to obtain the bucket and blob info.
    matches = re.match("gs://(.*?)/(.*)", response.driver_output_resource_uri)

    client = storage.Client.from_service_account_json('{}'.format(gcp_credentials))
    output = (client.get_bucket(matches.group(1)).blob(f"{matches.group(2)}.000000000").download_as_string())

    print(f"Job finished successfully: {output}\r\n")

run_spark(region = region, 
          cluster_name = cluster_name, 
          gcs_bucket = gcp_bucket_name, 
          spark_filename = 'create_plots.py', 
          project_id = project_id,
          gcp_credentials = gcp_credentials)