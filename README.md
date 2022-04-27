# dse230-final-project: On Time Flight or Wait Till Night, Delay or Hooray
## By: Jessica Allen and Carlos Monsivais

### Project Description
* Goal: Want to predict whether a airplane will be in one of the following categories.
    * Super Early: <= -15 mins
    * Slightly Early: -5 mins
    * On Time: -5 to 5 mins
    * Slightly Delayed: 5 to 15 mins
    * Super Delayed: >= 15 mins

* Project Thoughts: Iterate this process to model by aiport, and compare individual models by airtport to a full model that models all airports?


### To Do List
| Item                          | Description                                                                                                                  | Completed   |
| :---                          |    :----:                                                                                                                    |     ---:    |
| Get Data                      | We were able to get the plane data from https://www.kaggle.com/datasets/usdot/flight-delays?select=flights.csv               | Done        |
| Store Data in GCP             | Stored data in BQ and in a GCP bucket                                                                                        | Done        |
| Look into weather data        | Look into weather data by airport and see if it's possible to merge with current data set.                                   | In Progress |
| Change Data Scehma            | Change the schema of the data                                                                                                1| In Progress |
| Visualizations                | Create EDA visualizations for plane data: Either in Plotly or Matplotlib --> Run these through PySpark                       | In Progress |
| Google Slides Presentation 1  | Complete Presenation 1                                                                                                       | In Progress |
|Create prediction Cateogry	    | Create super early, slightly early, on time, slightly delayed, and super delayed category.	                               | In Progress |

### Project Structure
1. download_data.py --> Downloads data from Kaggle.
2. send_bq_gcp.py --> Will send data to BigQuery and to GCP.
3. data_clean.py --> Will clean data, put everything in the right format, take care of missing values. File should return a dataframe.
4. feature_create.py --> Will create more features in our data set that may be useful, should return new features we cna merge with our data.
5. create_plots.py --> Create plots in either Plotly or Matplotlib, maybe even a dashboard for the Visual EDA. Should return plots.
6. summary_statistics.py --> Create Summary statistic EDA. Should return summary statistics.
7. model.py --> Creates models Logisitic Regression, Time Series, etc.
8. run_all --> Will run all the Python files with one script.
9. present_plane.ipynb --> Jupyter Notebook that will take all the information from the Python files and display it here.

### Create Table Structures
* Below is the table structures for the data set. WIll complete once we look at the data a bit more.