import plotly.graph_objs as go

from spark_session import Create_Spark_Session


create_spark_session = Create_Spark_Session()
df = create_spark_session.create_spark_df()

class Create_EDA_Plots:
        def origin_airport_counts(self, df):
                origin_airport_count = df.groupBy('ORIGIN_AIRPORT').count().orderBy('count', ascending=False)

                fig = go.Figure([go.Bar(x = origin_airport_count.toPandas()['ORIGIN_AIRPORT'], 
                                        y = origin_airport_count.toPandas()['count'])])
                fig.update_layout(title={'text': "Origin Airport Counts",
                                         'x':0.5,
                                         'xanchor': 'center',
                                         'yanchor': 'top'},
                                  xaxis_title = "Origin Aiport",
                                  yaxis_title = "Count")
                fig.write_image("EDA_Images/Origin_Airport_Count.png")

        def destination_airport_counts(self, df):
                destination_airport_count = df.groupBy('DESTINATION_AIRPORT').count().orderBy('count', ascending=False)

                fig = go.Figure([go.Bar(x = destination_airport_count.toPandas()['DESTINATION_AIRPORT'], 
                                        y=destination_airport_count.toPandas()['count'])])
                fig.update_layout(title={'text': "Destination Airport Counts",
                                         'x':0.5,
                                         'xanchor': 'center',
                                         'yanchor': 'top'},
                                  xaxis_title = "Destination Aiport",
                                  yaxis_title = "Count")
                fig.write_image("EDA_Images/Destination_Airport_Count.png")

create_eda_plots = Create_EDA_Plots()
create_eda_plots.origin_airport_counts(df = df)
create_eda_plots.destination_airport_counts(df = df)