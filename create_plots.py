from pyspark.ml.stat import Correlation
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col,isnan, when, count

from spark_session import Create_Spark_Session

create_spark_session = Create_Spark_Session()
spark_session_outputs = create_spark_session.create_spark_df()
df = spark_session_outputs[0]
spark = spark_session_outputs[1]

class Create_EDA_Plots:
        def __init__(self, df, spark):
                # origin_destination_counts
                self.origin_airport_count = df.groupBy('ORIGIN_AIRPORT').count().orderBy('count', ascending=False)
                self.destination_airport_count = df.groupBy('DESTINATION_AIRPORT').count().orderBy('count', ascending=False)

                # correlation_matrix, pairplot
                integer_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, IntegerType)]
                integer_df = df[[integer_cols]]
                self.integer_df_drop_na = integer_df.na.drop("any")

                vector_col = "corr_features"
                assembler = VectorAssembler(inputCols = self.integer_df_drop_na.columns, 
                                            outputCol = vector_col)
                df_vector = assembler.transform(self.integer_df_drop_na).select(vector_col)
        
                matrix = Correlation.corr(df_vector, vector_col).collect()[0][0]
                corrmatrix = matrix.toArray().tolist()
                self.matrix_df = spark.createDataFrame(corrmatrix, integer_cols)

                # pairplot
                self.pairplot = df.limit(2000)

                # summary_statistics
                self.summary_table = df.summary()

                # null_values
                self.null_counts_df = df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df.columns])

                # train_test_split
                

        def origin_destination_counts(self):
                self.origin_airport_count.coalesce(1).write.csv(path = 'gs://plane-pyspark-run/Spark_Data_Output/origin_airport_count.csv', 
                                                                mode = 'overwrite',
                                                                header = True)
                self.destination_airport_count.coalesce(1).write.csv(path = 'gs://plane-pyspark-run/Spark_Data_Output/destination_airport_count.csv', 
                                                                     mode = 'overwrite',
                                                                     header = True)

        def correlation_matrix(self):
                self.matrix_df.coalesce(1).write.csv(path = 'gs://plane-pyspark-run/Spark_Data_Output/corr_matrix.csv', 
                                                     mode = 'overwrite',
                                                     header = True)

        def pairplot_rows(self):
                self.pairplot.coalesce(1).write.csv(path = 'gs://plane-pyspark-run/Spark_Data_Output/pairplot.csv', 
                                                    mode = 'overwrite',
                                                    header = True)

        def summary_table_vals(self):
                self.summary_table.coalesce(1).write.csv(path = 'gs://plane-pyspark-run/Spark_Data_Output/summary_table.csv', 
                                                         mode = 'overwrite',
                                                         header = True)

        def null_values(self):
                self.null_counts_df.coalesce(1).write.csv(path = 'gs://plane-pyspark-run/Spark_Data_Output/null_value_counts.csv', 
                                                          mode = 'overwrite',
                                                          header = True)


create_eda_plots = Create_EDA_Plots(df = df, spark = spark)
create_eda_plots.origin_destination_counts()
create_eda_plots.correlation_matrix()
create_eda_plots.pairplot_rows()
create_eda_plots.summary_table_vals()
create_eda_plots.null_values()