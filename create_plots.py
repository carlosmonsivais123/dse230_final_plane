import plotly.graph_objs as go
from plotly.subplots import make_subplots
import seaborn as sns
import matplotlib.pyplot as plt

from pyspark.ml.stat import Correlation
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.types import IntegerType

from spark_session import Create_Spark_Session

create_spark_session = Create_Spark_Session()
df = create_spark_session.create_spark_df()

class Create_EDA_Plots:
        def __init__(self, df):
                # origin_destination_counts
                self.origin_airport_count = df.groupBy('ORIGIN_AIRPORT').count().orderBy('count', ascending=False)
                self.destination_airport_count = df.groupBy('DESTINATION_AIRPORT').count().orderBy('count', ascending=False)

                # correlation_matrix and pairplot
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

create_eda_plots = Create_EDA_Plots(df = df)
create_eda_plots.origin_destination_counts()
create_eda_plots.correlation_matrix()
create_eda_plots.pairplot()