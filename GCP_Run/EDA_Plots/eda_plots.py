from turtle import clear
import pandas as pd
import numpy as np
import re
import plotly.graph_objs as go
from plotly.subplots import make_subplots
import matplotlib.pyplot as plt

from GCP_Functions.GCP_File_Upload import GCP_Functions
from Input_Variables.read_vars import gcp_bucket_name, gcp_credentials

class EDA_Plots:
    def __init__(self, client, bucket_name, gcp_credentials):
        self.gcp_credentials = gcp_credentials
        bucket = client.get_bucket(bucket_name)
        blobs = bucket.list_blobs()

        gcs_regex_location_matches = ['Spark_Data_Output/origin_airport_count.csv/part', 
                                      'Spark_Data_Output/destination_airport_count.csv/part',
                                      'Spark_Data_Output/corr_matrix.csv/part', 
                                      'Spark_Data_Output/pairplot.csv/part',
                                      'Spark_Data_Output/summary_table.csv/part',
                                      'Spark_Data_Output/null_value_counts.csv/part']

        for blob in blobs:
            if re.match(r'\.*{}*'.format(gcs_regex_location_matches[0]),blob.name):
                self.origin_airport_count = blob.name

            if re.match(r'\.*{}*'.format(gcs_regex_location_matches[1]),blob.name):
                self.destination_airport_count = blob.name

            elif re.match(r'\.*{}*'.format(gcs_regex_location_matches[2]),blob.name):
                self.corr_matrix = blob.name

            elif re.match(r'\.*{}*'.format(gcs_regex_location_matches[3]),blob.name):
                self.pairplot = blob.name            
                
            elif re.match(r'\.*{}*'.format(gcs_regex_location_matches[4]),blob.name):
                self.summary_table = blob.name    

            elif re.match(r'\.*{}*'.format(gcs_regex_location_matches[5]),blob.name):
                self.null_value_counts = blob.name
            
            else:
                None


    def origin_destination_airport_counts_plotly(self):
        plot_origin_airports = pd.read_csv('gs://plane-pyspark-run/{}'.format(self.origin_airport_count),
                                           storage_options={"token": "{}".format(self.gcp_credentials)})

        plot_destination_airports = pd.read_csv('gs://plane-pyspark-run/{}'.format(self.destination_airport_count),
                                           storage_options={"token": "{}".format(self.gcp_credentials)})

        fig = make_subplots(rows=2, 
                    cols=1,
                    subplot_titles=("Origin Airport Counts", "Destination Airport Counts"))

        fig.add_trace(go.Bar(x = plot_origin_airports['ORIGIN_AIRPORT'].astype(str), 
                                y = plot_origin_airports['count']),
                        row=1, col=1)

        fig.add_trace(go.Bar(x = plot_destination_airports['DESTINATION_AIRPORT'].astype(str), 
                                y = plot_destination_airports['count']),
                        row=2, col=1)

        fig.update_xaxes(title_text = "Origin Aiport", row=1, col=1)
        fig.update_xaxes(title_text = "Destination Aiport", row=2, col=1)

        fig.update_yaxes(title_text = "Count", row=1, col=1)
        fig.update_yaxes(title_text = "Count", row=2, col=1)

        fig.update_layout(height=900, width = 1400, showlegend=False)
        fig.write_image("/Users/CarlosMonsivais/Desktop/dse230_plane/GCP_Run/EDA_Plots/EDA_Static_Images/Origin_Destination_Airport_Count.png")



    def corr_matrix_plotly(self):
        plot_corr_matrix = pd.read_csv('gs://plane-pyspark-run/{}'.format(self.corr_matrix),
                                        storage_options={"token": "{}".format(self.gcp_credentials)})


        fig = go.Figure(data=go.Heatmap(z = plot_corr_matrix.values,
                                        x = plot_corr_matrix.columns,
                                        y = plot_corr_matrix.columns,
                                        text = np.round(plot_corr_matrix.values, 2),
                                        texttemplate="%{text}",
                                        hoverongaps = False))
        fig.update_layout(title={'text': "Correlation Plot",
                                    'x':0.5,
                                    'xanchor': 'center',
                                    'yanchor': 'top'},
                            yaxis_nticks=len(plot_corr_matrix.columns),
                            xaxis_nticks=len(plot_corr_matrix.columns))
        fig.update_layout(height=900, width = 1400, showlegend=False)
        fig.write_image("/Users/CarlosMonsivais/Desktop/dse230_plane/GCP_Run/EDA_Plots/EDA_Static_Images/Correlation_Plot.png")


    def pairplot_plotly(self):
        plot_pairplot = pd.read_csv('gs://plane-pyspark-run/{}'.format(self.pairplot),
                                    storage_options={"token": "{}".format(self.gcp_credentials)})


        fig = sns.PairGrid(plot_pairplot)
        fig.map_diag(sns.histplot)
        fig.map_offdiag(sns.scatterplot)
        plt.show(block=False)
        plt.savefig('/Users/CarlosMonsivais/Desktop/dse230_plane/GCP_Run/EDA_Plots/EDA_Static_Images/Pairplot.png')

