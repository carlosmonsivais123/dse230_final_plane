# Libararies used for spark_commands.py
from pyspark.ml.stat import Correlation
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col, isnan, when, count

class PySpark_Code:
        '''
        Class --> PySpark_Code:
                        This class is in charge of running all the PySpark code from the data transformations, plot files, and 
                        even modeling we will do.

                        Input Variables: 
                                1. df: The PySpark dataframe created from the spark_session.py file.
                                2. spark: The Spark Session created from the spark_session.py file.
        '''

        def __init__(self, df, spark):
                '''initializaer --> __init__:
                        This initializer reads in variables we will be using throughout the rest of the functions below. The
                        dataframe, df and Spark Session spark are the variables we will initialize.
                '''
                self.df = df
                self.spark = spark

        def origin_destination_counts(self):
                '''
                Function --> origin_destination_counts:          
                                This function creates counts for the ORIGIN_ARPORT and DESTINATIO_AIRPORT columns in the dataset and then sends these
                                counts to a specified GCS bucket. These counts are meant to be used for plotting purposes in the counts bar chart.

                                Input Variables
                                        None

                                Output Variables
                                        None
                '''
                origin_airport_count = self.df.groupBy('ORIGIN_AIRPORT').count().orderBy('count', ascending=False)
                destination_airport_count = self.df.groupBy('DESTINATION_AIRPORT').count().orderBy('count', ascending=False)

                origin_airport_count.coalesce(1).write.csv(path = 'gs://plane-pyspark-run/Spark_Data_Output/origin_airport_count.csv', 
                                                           mode = 'overwrite',
                                                           header = True)
                destination_airport_count.coalesce(1).write.csv(path = 'gs://plane-pyspark-run/Spark_Data_Output/destination_airport_count.csv', 
                                                                mode = 'overwrite',
                                                                header = True)

        def correlation_matrix(self):
                '''
                Function --> correlation_matrix:          
                                This function creates the correlation matrix values and then sends these values to a specified GCS bucket. 
                                These counts are meant to be used for plotting purposes in the correlation heatmap.

                                Input Variables
                                        None

                                Output Variables
                                        None
                '''
                integer_cols = [f.name for f in self.df.schema.fields if isinstance(f.dataType, IntegerType)]
                integer_df = self.df[[integer_cols]]
                integer_df_drop_na = integer_df.na.drop("any")

                vector_col = "corr_features"
                assembler = VectorAssembler(inputCols = integer_df_drop_na.columns, 
                                            outputCol = vector_col)
                df_vector = assembler.transform(integer_df_drop_na).select(vector_col)
        
                matrix = Correlation.corr(df_vector, vector_col).collect()[0][0]
                corrmatrix = matrix.toArray().tolist()
                matrix_df = self.spark.createDataFrame(corrmatrix, integer_cols)

                matrix_df.coalesce(1).write.csv(path = 'gs://plane-pyspark-run/Spark_Data_Output/corr_matrix.csv', 
                                                mode = 'overwrite',
                                                header = True)

        def pairplot_rows(self):
                '''
                Function --> pairplot_rows:          
                                This function creates a subset of the dataset and then sends this dataset to a specified GCS bucket. 
                                These counts are meant to be used for plotting purposes in the pairplot.

                                Input Variables
                                        None

                                Output Variables
                                        None
                '''
                integer_cols = [f.name for f in self.df.schema.fields if isinstance(f.dataType, IntegerType)]
                integer_df = self.df[[integer_cols]]
                pairplot = integer_df.limit(2000)

                pairplot.coalesce(1).write.csv(path = 'gs://plane-pyspark-run/Spark_Data_Output/pairplot.csv', 
                                                    mode = 'overwrite',
                                                    header = True)

        def summary_table_vals(self):
                '''
                Function --> summary_table_vals:          
                                This function creates a subset of the dataset and then sends these values to a specified GCS bucket. 
                                These counts are meant to be used for plotting purposes in the summary table plot.

                                Input Variables
                                        None

                                Output Variables
                                        None
                '''
                summary_table = self.df.summary()

                summary_table.coalesce(1).write.csv(path = 'gs://plane-pyspark-run/Spark_Data_Output/summary_table.csv', 
                                                    mode = 'overwrite',
                                                    header = True)

        def null_values(self):
                '''
                Function --> null_values:
                                This function creates counts for each column counting the null values per column and then send these counts to
                                a specified GCS bucket. These counts are meant to be used for plotting pruposes in the null counts bar chart.                               

                                Input Variables
                                        None

                                Output Variables
                                        None
                '''
                null_counts_df = self.df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in self.df.columns])

                null_counts_df.coalesce(1).write.csv(path = 'gs://plane-pyspark-run/Spark_Data_Output/null_value_counts.csv', 
                                                     mode = 'overwrite',
                                                     header = True)