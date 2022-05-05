import tempfile
from turtle import clear
import pandas as pd
import numpy as np
import re
import plotly.graph_objs as go
from plotly.subplots import make_subplots
import matplotlib.pyplot as plt

import plotly.figure_factory as ff
import plotly.express as px
import seaborn as sns


import io

from GCP_Functions.GCP_File_Upload import GCP_Functions

class EDA_Plots:
    def __init__(self, bucket_name, gcp_credentials, client):
        self.gcp_functions = GCP_Functions()
        self.client = client

        self.temp_dir = tempfile.TemporaryDirectory()

        self.bucket_name = bucket_name
        self.gcp_credentials = gcp_credentials

        gcs_regex_location_matches = ['Spark_Data_Output/origin_airport_count.csv/part', 
                                      'Spark_Data_Output/destination_airport_count.csv/part',
                                      'Spark_Data_Output/corr_matrix.csv/part', 
                                      'Spark_Data_Output/pairplot.csv/part',
                                      'Spark_Data_Output/summary_table.csv/part',
                                      'Spark_Data_Output/null_value_counts.csv/part']

        bucket = self.client.get_bucket(self.bucket_name)
        blobs = bucket.list_blobs()

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

        fig.write_image("{}/Origin_Destination_Airport_Count.png".format(self.temp_dir.name))

        # Push up to GCS Bucket
        self.gcp_functions.upload_to_gcp_bucket(client = self.client, 
                                                bucket_name = self.bucket_name, 
                                                blob_name = 'EDA_Static_Images/Origin_Destination_Airport_Count.png', 
                                                path_to_file = "{}/Origin_Destination_Airport_Count.png".format(self.temp_dir.name))



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

        fig.write_image("{}/Correlation_Plot.png".format(self.temp_dir.name))

        # Push up to GCS Bucket
        self.gcp_functions.upload_to_gcp_bucket(client = self.client, 
                                                bucket_name = self.bucket_name, 
                                                blob_name = 'EDA_Static_Images/Correlation_Plot.png', 
                                                path_to_file = "{}/Correlation_Plot.png".format(self.temp_dir.name))



    def pairplot_plotly(self):
        plot_pairplot = pd.read_csv('gs://plane-pyspark-run/{}'.format(self.pairplot),
                                    storage_options={"token": "{}".format(self.gcp_credentials)})

        fig = sns.PairGrid(plot_pairplot, corner=True)
        fig.map_diag(sns.histplot)
        fig.map_offdiag(sns.scatterplot)
        plt.savefig('{}/Pairplot.png'.format(self.temp_dir.name))

        # Push up to GCS Bucket
        self.gcp_functions.upload_to_gcp_bucket(client = self.client, 
                                                bucket_name = self.bucket_name, 
                                                blob_name = 'EDA_Static_Images/Pairplot.png', 
                                                path_to_file = "{}/Pairplot.png".format(self.temp_dir.name))
        plt.close()


    def summary_table_plotly(self):
        plot_summary_table = pd.read_csv('gs://plane-pyspark-run/{}'.format(self.summary_table),
                                        storage_options={"token": "{}".format(self.gcp_credentials)})

        summary_pandas_df = plot_summary_table.T
        summary_pandas_df.rename(columns = summary_pandas_df.iloc[0], inplace = True)
        summary_pandas_df_table = summary_pandas_df.iloc[1:]
        summary_pandas_df_table.reset_index(inplace = True, drop = False)
        summary_pandas_df_table.rename(columns={summary_pandas_df_table.columns[0]: "Columns" }, inplace = True)
        summary_pandas_df_table['mean'] = summary_pandas_df_table['mean'].astype(float)
        summary_pandas_df_table['mean'] = summary_pandas_df_table['mean'].round(2)
        summary_pandas_df_table['stddev'] = summary_pandas_df_table['stddev'].astype(float)
        summary_pandas_df_table['stddev'] = summary_pandas_df_table['stddev'].round(2)

        fig = go.Figure(data=[go.Table(header=dict(values=list(summary_pandas_df_table.columns)),
                cells=dict(values=[summary_pandas_df_table['Columns'],
                                   summary_pandas_df_table['count'],
                                   summary_pandas_df_table['mean'],
                                   summary_pandas_df_table['stddev'],
                                   summary_pandas_df_table['min'],
                                   summary_pandas_df_table['25%'],
                                   summary_pandas_df_table['50%'],
                                   summary_pandas_df_table['75%'],
                                   summary_pandas_df_table['max']],
                            font_size=9))])
        fig.update_layout(width=1400,
                        height=850)
        fig.write_image("{}/Summary_Table.png".format(self.temp_dir.name))

        # Push up to GCS Bucket
        self.gcp_functions.upload_to_gcp_bucket(client = self.client, 
                                                bucket_name = self.bucket_name, 
                                                blob_name = 'EDA_Static_Images/Summary_Table.png', 
                                                path_to_file = "{}/Summary_Table.png".format(self.temp_dir.name))



    def null_values_counts_plotly(self):
        plot_null_values = pd.read_csv('gs://plane-pyspark-run/{}'.format(self.summary_table),
                                        storage_options={"token": "{}".format(self.gcp_credentials)})

        fig = go.Figure(data=[go.Bar(x = list(plot_null_values.columns), 
                                     y = plot_null_values.values.tolist()[0],
                                     text = plot_null_values.values.tolist()[0],
                                     textposition = 'auto')])
        fig.update_layout(title={'text': "Null Value Count",
                                'x':0.5,
                                'xanchor': 'center',
                                'yanchor': 'top'},
                        xaxis_title = "Column Names",
                        yaxis_title = "Null Counts")                            
        fig.update_layout(height=900, width = 1400, showlegend=False)

        fig.write_image("{}/Null_Values.png".format(self.temp_dir.name))

        # Push up to GCS Bucket
        self.gcp_functions.upload_to_gcp_bucket(client = self.client, 
                                                bucket_name = self.bucket_name, 
                                                blob_name = 'EDA_Static_Images/Null_Values.png', 
                                                path_to_file = "{}/Null_Values.png".format(self.temp_dir.name))