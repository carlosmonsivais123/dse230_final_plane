# Create_Spark_Session class from dataproc_spark_session.py
from dataproc_spark_session import Create_Spark_Session

# PySpark class from dataproc_spark_commands.py
from features_dataproc_spark_commands import PySpark_Code


#################################################### dataproc_spark_session.py ####################################################
# Creating an instance from the class Create_Spark_Session() from the file dataproc_spark_session.py where we will create the 
# PySpark datafame called df and the Sparke Session called spark.
create_spark_session = Create_Spark_Session()
spark_session_outputs = create_spark_session.create_spark_df()

# PySpark dataframe with flights.csv data.
df_flights = spark_session_outputs[0]
df_airports = spark_session_outputs[1]
df_airlines = spark_session_outputs[2]

# Spark Session we created.
spark = spark_session_outputs[3]

#################################################### dataproc_spark_commands.py ####################################################
# Creating an instance from the class PySpark_Code() from the file dataproc_spark_commands.py where we will run the PySpark code 
# creating the following values and files.
pyspark_code = PySpark_Code(df_flights=df_flights, df_airports=df_airports, df_airlines=df_airlines, spark=spark)
pyspark_code.feature_engineering()
