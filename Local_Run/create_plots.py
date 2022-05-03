import pandas as pd
import plotly.graph_objs as go
from plotly.subplots import make_subplots
import seaborn as sns
import matplotlib.pyplot as plt

from pyspark.ml.stat import Correlation
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col,isnan, when, count

from spark_session import Create_Spark_Session

create_spark_session = Create_Spark_Session()
df = create_spark_session.create_spark_df()

class Create_EDA_Plots:
        def __init__(self, df):
                # origin_destination_counts
                self.origin_airport_count = df.groupBy('ORIGIN_AIRPORT').count().orderBy('count', ascending=False)
                self.destination_airport_count = df.groupBy('DESTINATION_AIRPORT').count().orderBy('count', ascending=False)

                # correlation_matrix, pairplot
                self.integer_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, IntegerType)]
                self.integer_df = df[[self.integer_cols]]
                self.integer_df_drop_na = self.integer_df.na.drop("any")

                vector_col = "corr_features"
                assembler = VectorAssembler(inputCols = self.integer_df_drop_na.columns, 
                                            outputCol = vector_col)
                df_vector = assembler.transform(self.integer_df_drop_na).select(vector_col)
        
                matrix = Correlation.corr(df_vector, vector_col)
                matrix_array = matrix.collect()[0]["pearson({})".format(vector_col)].values
                self.matrix_array = matrix_array.reshape((len(self.integer_cols), len(self.integer_cols)))

                # summary_statistics
                summary_table = df.summary()
                pandas_summary = summary_table.toPandas()
                summary_pandas_df = pandas_summary.T
                summary_pandas_df.rename(columns = summary_pandas_df.iloc[0], inplace = True)
                summary_pandas_df_table = summary_pandas_df.iloc[1:]
                summary_pandas_df_table.reset_index(inplace = True, drop = False)
                summary_pandas_df_table.rename(columns={summary_pandas_df_table.columns[0]: "Columns" }, inplace = True)
                summary_pandas_df_table['mean'] = summary_pandas_df_table['mean'].astype(float)
                summary_pandas_df_table['mean'] = summary_pandas_df_table['mean'].round(2)
                summary_pandas_df_table['stddev'] = summary_pandas_df_table['stddev'].astype(float)
                summary_pandas_df_table['stddev'] = summary_pandas_df_table['stddev'].round(2)
                self.summary_pandas_df_table = summary_pandas_df_table

                # null_values
                null_counts_df = df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df.columns])
                self.null_counts_pandas = null_counts_df.toPandas()

        def origin_destination_counts(self):
                fig = make_subplots(rows=2, 
                                    cols=1,
                                    specs=[[{"type": "bar"}],
                                           [{"type": "bar"}]],
                                    subplot_titles=("Origin Airport Counts", "Destination Airport Counts"))

                fig.add_trace(go.Bar(x = self.origin_airport_count.toPandas()['ORIGIN_AIRPORT'], 
                                     y = self.origin_airport_count.toPandas()['count']),
                              row=1, col=1)

                fig.add_trace(go.Bar(x = self.destination_airport_count.toPandas()['DESTINATION_AIRPORT'], 
                                     y = self.destination_airport_count.toPandas()['count']),
                              row=2, col=1)

                fig.update_xaxes(title_text = "Origin Aiport", row=1, col=1)
                fig.update_xaxes(title_text = "Destination Aiport", row=2, col=1)

                fig.update_yaxes(title_text = "Count", row=1, col=1)
                fig.update_yaxes(title_text = "Count", row=2, col=1)

                fig.update_layout(height=700, showlegend=False)

                fig.write_image("EDA_Static_Images/Origin_Destination_Airport_Count.png")
                fig.write_html("EDA_HTML_Images/Origin_Destination_Airport_Count.html")

        def correlation_matrix(self):
                fig = go.Figure(data=go.Heatmap(z = self.matrix_array,
                                                x = self.integer_cols,
                                                y = self.integer_cols,
                                                text = self.matrix_array,
                                                hoverongaps = False))
                fig.update_layout(title={'text': "Correlation Plot",
                                         'x':0.5,
                                         'xanchor': 'center',
                                         'yanchor': 'top'},
                                  yaxis_nticks=len(self.integer_cols),
                                  xaxis_nticks=len(self.integer_cols))
                fig.write_image("EDA_Static_Images/Correlation_Plot.png")
                fig.write_html("EDA_HTML_Images/Correlation_Plot.html")

        def pairplot(self):
                fig = sns.PairGrid(self.integer_df_drop_na.limit(2000).toPandas())
                fig.map_diag(sns.histplot)
                fig.map_offdiag(sns.scatterplot)
                plt.savefig('EDA_Static_Images/Pairplot.png')

        def summary_table(self):
                fig = go.Figure(data=[go.Table(header=dict(values=list(self.summary_pandas_df_table.columns)),
                                cells=dict(values=[self.summary_pandas_df_table['Columns'],
                                                   self.summary_pandas_df_table['count'],
                                                   self.summary_pandas_df_table['mean'],
                                                   self.summary_pandas_df_table['stddev'],
                                                   self.summary_pandas_df_table['min'],
                                                   self.summary_pandas_df_table['25%'],
                                                   self.summary_pandas_df_table['50%'],
                                                   self.summary_pandas_df_table['75%'],
                                                   self.summary_pandas_df_table['max']],
                                            font_size=9       
                                                   ))])
                fig.update_layout(width=1500,
                                  height=900)
                fig.write_image("EDA_Static_Images/Summary_Table.png")
                fig.write_html("EDA_HTML_Images/Summary_Table.html")

        def null_values(self):
                fig = go.Figure(data=[go.Bar(x = list(self.null_counts_pandas.columns), 
                                             y = self.null_counts_pandas.values.tolist()[0],
                                             text = self.null_counts_pandas.values.tolist()[0],
                                             textposition = 'auto')])
                fig.update_layout(title={'text': "Null Value Count",
                                        'x':0.5,
                                        'xanchor': 'center',
                                        'yanchor': 'top'},
                                xaxis_title = "Column Names",
                                yaxis_title = "Null Counts")                            

                fig.write_image("EDA_Static_Images/Null_Values.png")
                fig.write_html("EDA_HTML_Images/Null_Values.html")


create_eda_plots = Create_EDA_Plots(df = df)
create_eda_plots.origin_destination_counts()
create_eda_plots.correlation_matrix()
create_eda_plots.pairplot()
create_eda_plots.summary_table()
create_eda_plots.null_values()