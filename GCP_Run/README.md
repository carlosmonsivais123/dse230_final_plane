# Running Project in GCP
* main.py
    * This file will initiate the execution of the whole program including:
        - Sending the required files into the speciifed GCP Buckets
        - Create the PySpark cluster
        - Run the Pyspark code stored in the GCP bucket
        - Delete the PySpark cluster
        - Plot and analyze the metadata --> (In progress, not done yet)

# Folder and File Structure
## Folder: Input_Variables
* These files take in the user specified variables needed for the project such as:
    * gcp_project_id: 'dse230' --> No change
    * gcp_region: 'us-central1' --> No change
    * gcp_cluster_name: 'dse-230-pyspark' --> No change
    * gcp_bucket_name: 'plane-pyspark-run' --> No change 

    ### Files: Input_Variables  
    * gcp_credentials: '/Users/CarlosMonsivais/Desktop/dse230_plane/dse230-0c3411f763a5.json' --> Change to wherever your JSON key is located.
        * input_vars.yaml: This is the YAML file where you deifne the varibles above, only need to chnage the location of the JSON key.
        * read_vars.py: Reads in the variable from the YAML file above so we can use those variables throughout the project.

## Folder: GCP_Functions
* These files will create a Client variable to access the GCP bucket API and will also upload local files up to a specified GCP bucket.

    ### Files: Input_Variables
    * GCP_File_Upload.py: This file has two functions within the same class, one to create a Client variables type and another to send the speciifed files into
    a GCP bucket.


## DataProc_Files
* These are the files that will be run in DataProc in GCP 
    * main.py: Runs the dataproc_spark_session.py and the dataproc_spark_commands.py Python files.
    * dataproc_spark_session.py: Creates a spark session and reads in teh data from a GCS bucket.
    * dataproc_spark_commands.py: These are the PySpark commands that we will use to make calculations on our dataframe.


