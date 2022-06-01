# DSE 230 Final Project: Delay or Hooray
## By: Jessica Allen and Carlos Monsivais

### Project Description
* **Project Goal**: Can we predict when a flight will be delayed based on the origin airport, destination airport, time of day, airline, distance of flight, and date?

* **Data**: We will use flight data from the United States from the year 2015 from the following source:
    * https://www.kaggle.com/datasets/usdot/flight-delays?select=flights.csv

### Included in Project
- Kaggle Key.
- GCP Key.
- Code for EDA, Feature Engineering and ML Model .
- Code to run the above files on a cluster in GCP.

### To Run the Project in GCP
* Open the jupyter notebook named run_it.ipynb and follow the detailed instructions. This notebook will have the user set up a virtual enviroment and input the pwd for the GCP key.  It will also download the datasets from kaggle in the necessary location on the user’s local machine. Lastly, it will run the main.py file which will run all of the necessary code.


### Overview of Project
* Ultimately, the notebook is going to run the main.py file located in GCP_Run directory. This main file will run through four main steps. 
    1. **Upoad Files**
        - This will upload the neccesary python files with the pyspark commands to dataproc located here: gs://plane-pyspark-run/DataProc_Files/
        - This will also upload the CSV’s of the raw data that were downloaded in the jupyter notebook mentioned above to a GCP bucket located here: gs://   plane-pyspark-run/flight-delays/

    2. **EDA**
        - This will create a pySpark cluster,  run the EDA dataproc code that calculates a summary table for each column and creates multiple graphs used for initial exploratory analysis.
        - These outputs are saved to a GCP bucket located here: gs://plane-pyspark-run/EDA_Static_Images/
        -  Cluster is deleted.
    
    3. **Feature Engineering**
        - Creates a new cluster.
        - This step cleans the data by eliminating unnecessary columns, dropping nulls where needed, and changing column values from codes to something more readable.
        - The categories for the labels were  created.
        - The counts for departing/landing flights during the same hour of take off/depature were computed.
        - Final dataset ultimately includes: 
            - Day of week
            - Month
            - Airline
            - Origin Airport
            - Destination Airport
            - Hour of departure
            - Hour of arrival
            - Distance of flight
            - Number of flights leaving/arriving origin airport
            - Number of flights leaving/arriving destination airport
        - Uploads final dataset to a GCP bucket located here: gs://plane-pyspark-run/Spark_Data_Output/
        - The cluster is deleted

    4. **Logistic Regression**
        - Creates a new cluster.
        - One-Hot encodes labels and categorical features.
        - Creates a vector of the desired features.
        - Standarizes the features.
        - Stratifies the sample to a test and train dataset.
        - Runs the train data through a logistic regression model.
        - Optional: test hyper parameters but this is commented out to save on compute time.
        - Calculates accuracy using the test set and prints results in the log file.
        - Uploads prediction and model to GCP bucket located here: s://plane-pyspark-run/Spark_Models/lr_model_all
        - Cluster is deleted.

### Code Organization
- **GCP_Functions**: Functions that creates a GCP client and upload files to GCP buckets used throughout the project.
- **PySpark_Files**: Creates pyspark cluster, runs the pySpark files, and delete the cluster for every step in run_steps.
- **Run_Steps**: Contains all steps to be ran in the main.py file, these steps are what is described above in the overview of the project.
- **DataProc_Files**: Contains all python code with pySpark commands to be ran on clusters.
- **Input_Variables**: Contains all key locations and configs needed.
- **Confusion_Matrixes**: Contains output for accuracy used in the final presentation.
- **EDA_Plots**: Contains code to create graphs used in initial presentation and push graphs to bucket mentioned above.
