# Libararies used for spark_session.py
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

class Create_Spark_Session:
    '''
    Class --> Create_Spark_Session:
                This class is in charge of creating the spark session that will be passed into the file called dataproc_spark_commands.py
                which will read in the spark dataframe that is created here called df and the spark session called spark.
                    
                Input Variables
                    None
    '''

    def create_spark_df(self):
        '''
        Function --> create_spark_df:
                        This function uses the schema created below, starts a Spark Session with no configurations inputs to let 
                        GCP do that for us and maximize all nodes and compute instances. Using this Spark Session we are able to read
                        in a file at a given GCS bucket to create a PySpark dataframe. 
                        Input Variables
                            1. data_location: The location of the plane.csv dataset in the GCS bucket.
                        Output Variables
                            1. spark_df: The spark dataframe that was created based on the schema below and the data read in from the GCS bucket.
                            2. spark: This is an SparkSession() global values that is created.
        '''
        # Schema that was created for the plane.csv data set.
        schema = StructType([StructField("YEAR", StringType(), False),
                             StructField("MONTH", StringType(), False),
                             StructField("DAY", StringType(), False),
                             StructField("DAY_OF_WEEK", StringType(), False),
                             StructField("AIRLINE", StringType(), False),
                             StructField("FLIGHT_NUMBER", StringType(), True),
                             StructField("TAIL_NUMBER", StringType(), True),
                             StructField("ORIGIN_AIRPORT", StringType(), False),
                             StructField("DESTINATION_AIRPORT", StringType(), False),
                             StructField("SCHEDULED_DEPARTURE", IntegerType(), False),
                             StructField("DEPARTURE_TIME", IntegerType(), True),
                             StructField("DEPARTURE_DELAY", IntegerType(), True),
                             StructField("TAXI_OUT", IntegerType(), True),
                             StructField("WHEELS_OFF", IntegerType(), True),
                             StructField("SCHEDULED_TIME", IntegerType(), True),
                             StructField("ELAPSED_TIME", IntegerType(), True),
                             StructField("AIR_TIME", IntegerType(), True),
                             StructField("DISTANCE", IntegerType(), True),
                             StructField("WHEELS_ON", IntegerType(), True),
                             StructField("TAXI_IN", IntegerType(), True),
                             StructField("SCHEDULED_ARRIVAL", IntegerType(), True),
                             StructField("ARRIVAL_TIME", IntegerType(), True),
                             StructField("ARRIVAL_DELAY", IntegerType(), True),
                             StructField("DIVERTED", StringType(), True),
                             StructField("CANCELLED", StringType(), True),
                             StructField("CANCELLATION_REASON", StringType(), True),
                             StructField("AIR_SYSTEM_DELAY", IntegerType(), True),
                             StructField("SECURITY_DELAY", IntegerType(), True),
                             StructField("AIRLINE_DELAY", IntegerType(), True),
                             StructField("LATE_AIRCRAFT_DELAY", IntegerType(), True),
                             StructField("WEATHER_DELAY", StringType(), True),
                            ])

        # Spark session created, do not add any instances so DataProc can create efficiencies in the configuration.
        spark = SparkSession.builder.getOrCreate()

        # Pyspark dataframe type using the schema specified above and the GCS bucket location of the flights data.
        spark_df_flights = spark.read.csv("gs://plane-pyspark-run/flight-delays/flights.csv",
                                  header=True,
                                  schema = schema)

        spark_df_airports = spark.read.csv("gs://plane-pyspark-run/flight-delays/airports.csv",
                                  header=True,
                                  inferSchema = True)

        spark_df_airlines = spark.read.csv("gs://plane-pyspark-run/flight-delays/airlines.csv",
                                  header=True,
                                  inferSchema = True)

        # Returns a spark dataframe: spark_df and a Spark Session: spark
        return spark_df_flights, spark_df_airports, spark_df_airlines, spark
