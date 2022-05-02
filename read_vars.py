import yaml

with open('input_vars.yaml') as info:
    info_dict = yaml.safe_load(info)

project_id = info_dict['gcp_project_id']
region = info_dict['gcp_region']
cluster_name = info_dict['gcp_cluster_name']
gcp_bucket_name = info_dict['gcp_bucket_name']
gcp_credentials = info_dict['gcp_credentials']