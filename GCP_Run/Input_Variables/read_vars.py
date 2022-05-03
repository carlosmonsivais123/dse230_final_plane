# Reading in the variables we defined in the input_vars.yaml file to convert these variables into a Python variable we can use throughout the rest
# of the files in the GCP_Run project folder.

import yaml

with open('Input_Variables/input_vars.yaml') as info:
    info_dict = yaml.safe_load(info)

project_id = info_dict['gcp_project_id']
region = info_dict['gcp_region']
cluster_name = info_dict['gcp_cluster_name']
gcp_bucket_name = info_dict['gcp_bucket_name']
gcp_credentials = info_dict['gcp_credentials']