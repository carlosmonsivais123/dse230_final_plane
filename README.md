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

#### Run PySpark Locally
* The files to run locally are located in this repository in the Local_Run folder.
* We will be able to run the project using our local computers resources where we will run it end to end from the data cleaning, to the modeling and then to analyzing the otuput results and conclusions.