import plotly.graph_objs as go
from plotly.subplots import make_subplots

from spark_session import Create_Spark_Session
from pyspark.ml.stat import Correlation
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.types import IntegerType

create_spark_session = Create_Spark_Session()
df = create_spark_session.create_spark_df()

class Create_EDA_Plots:
        def origin_destination_counts(self, df):
                origin_airport_count = df.groupBy('ORIGIN_AIRPORT').count().orderBy('count', ascending=False)
                destination_airport_count = df.groupBy('DESTINATION_AIRPORT').count().orderBy('count', ascending=False)

                fig = make_subplots(rows=2, 
                                    cols=1,
                                    specs=[[{"type": "bar"}],
                                          [{"type": "bar"}]],
                                    subplot_titles=("Origin Airport Counts", "Destination Airport Counts"))

                fig.add_trace(go.Bar(x = origin_airport_count.toPandas()['ORIGIN_AIRPORT'], 
                                     y = origin_airport_count.toPandas()['count']),
                              row=1, col=1)

                fig.add_trace(go.Bar(x = destination_airport_count.toPandas()['DESTINATION_AIRPORT'], 
                                     y=destination_airport_count.toPandas()['count']),
                              row=2, col=1)

                fig.update_xaxes(title_text = "Origin Aiport", row=1, col=1)
                fig.update_xaxes(title_text = "Destination Aiport", row=2, col=1)

                fig.update_yaxes(title_text = "Count", row=1, col=1)
                fig.update_yaxes(title_text = "Count", row=2, col=1)

                fig.update_layout(height=700, showlegend=False)

                fig.write_image("EDA_Static_Images/Origin_Destination_Airport_Count.png")
                fig.write_html("EDA_HTML_Images/Origin_Destination_Airport_Count.html")

        def correlation_matrix(self, df):
                integer_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, IntegerType)]
                integer_df = df[[integer_cols]]
                integer_df = integer_df.na.drop("any")

                vector_col = "corr_features"
                assembler = VectorAssembler(inputCols=integer_df.columns, outputCol=vector_col)
                df_vector = assembler.transform(integer_df).select(vector_col)
                
                matrix = Correlation.corr(df_vector, vector_col)
                matrix_array = matrix.collect()[0]["pearson({})".format(vector_col)].values
                matrix_array = matrix_array.reshape((len(integer_cols), len(integer_cols)))

                fig = go.Figure(data=go.Heatmap(z = matrix_array,
                                                x = integer_cols,
                                                y = integer_cols,
                                                hoverongaps = False))
                fig.update_layout(title={'text': "Correlation Plot",
                                         'x':0.5,
                                         'xanchor': 'center',
                                         'yanchor': 'top'},
                                  yaxis_nticks=len(integer_cols),
                                  xaxis_nticks=len(integer_cols))
                fig.write_image("EDA_Static_Images/Correlation_Plot.png")
                fig.write_html("EDA_HTML_Images/Correlation_Plot.html")

create_eda_plots = Create_EDA_Plots()
create_eda_plots.origin_destination_counts(df = df)
create_eda_plots.correlation_matrix(df = df)