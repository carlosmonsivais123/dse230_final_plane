# DSE 230 Final Project: Delay or Hooray
## By: Jessica Allen and Carlos Monsivais

### Project Description
* Goal: Want to predict whether an airplane flight will be arriving in one of the following categories.
    * Super Early: <= -15 mins
    * Slightly Early: -5 mins
    * On Time: -5 to 5 mins
    * Slightly Delayed: 5 to 15 mins
    * Super Delayed: >= 15 mins

* Data: We will use flight data from the United States from the year 2015 from the following source:
    * https://www.kaggle.com/datasets/usdot/flight-delays?select=flights.csv

* Project Thoughts: Iterate this process to model by aiport, and compare individual models by airtport to a full model that models all airports?

### To Do List
| Item                             | Description                                                                                                                  | Completed   |
| :---:                            |    :----:                                                                                                                    |    :---:    |
| Get Data                         | We were able to get the plane data from https://www.kaggle.com/datasets/usdot/flight-delays?select=flights.csv               | Done        |
| Store Data in GCP                | Stored data in BQ and in a GCP bucket                                                                                        | Done        |
| Change Data Scehma               | Change the schema of the data                                                                                                | In Progress |
| Visualizations                   | Create EDA visualizations for plane data --> Run these through PySpark                                                       | Done        |
| Google Slides Presentation 1     | Complete Presenation 1                                                                                                       | Done        |
| Create prediction Category	   | Create super early, slightly early, on time, slightly delayed, and super delayed category in the data	                      | In Progress |
| Feature Engineering        	   | Create more features in data set that may be helpful for model                                                               | In Progress |
| Stratified Train Test Split Data | Train Test Split proportionally in PySpark.                                                                                  | In Progress |
| Model Data                       | Create Logisitic Regression Model                                                                                            | In Progress |
| Conclusion                       | Summarize Project and Models                                                                                                 | In Progress |


### Project Structure
#### Run on GCP PySpark Cluster
* The files to run on GCP are located in this repository in the GCP_Run folder.
* We will be able to run the project using our PySpark clusters we will spin up on GCP where we will run it end to end from the data cleaning, to the modeling and then to analyzing the otuput results and conclusions.

# Running Project in GCP
* To run this project make sure to go into the Input_Variables folder and change the YAML files gcp_cedentials variable to match the location of where your key is located.
* Afterwards, run the main.ipynb file in the directory, you will get messages as each step is completed.
* main.py
    * This file will initiate the execution of the whole program including:
        - Sending the required files into the speciifed GCP Buckets
        - Create the PySpark cluster
        - Run the Pyspark code stored in the GCP bucket
        - Delete the PySpark cluster

<br>

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

<br>

## Folder: GCP_Functions
* These files will create a Client variable to access the GCP bucket API and will also upload local files up to a specified GCP bucket.

    ### Files: GCP_Functions
    * GCP_File_Upload.py: This file has two functions within the same class, one to create a Client variables type and another to send the speciifed files into
    a GCP bucket.

<br>

## Folder: PySpark_Files
* These files will create a spark cluster, run a PySpark job using the python files we uploaded to GCP initially and then delete the cluster after the job is complete.

    ### Files: PySpark_Files
    * create_run_delete_spark_cluseter: This file has three functions within the same class, one to create a PySpark cluster, one to run the these files stored in the GCP bucket (dataproc_main.py, dataproc_spark_session.py, dataproc_spark_commands.py).

<br>

## Folder: DataProc_Files
* These are the files that will be run in DataProc in GCP.

    ### Files: DataProc_Files
    * dataproc_main.py: Runs the dataproc_spark_session.py and the dataproc_spark_commands.py Python files.
    * dataproc_spark_session.py: Creates a spark session and reads in teh data from a GCS bucket.
    * dataproc_spark_commands.py: These are the PySpark commands that we will use to make calculations on our dataframe.

<br>

## Folder: EDA_Plots
* This file will plot the calculations made in PySpark using plotly.

    ### Files: EDA_Plots
    * eda_plots.py: Reads in the csv files generated by the PySpark calculations, creates plots using Plotly and then saves them as PNG files in a GCS bucket.