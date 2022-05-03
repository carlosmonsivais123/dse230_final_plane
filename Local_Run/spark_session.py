import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

class Create_Spark_Session:
    def create_spark_df(self):
        conf = pyspark.SparkConf().setAll([
                ('spark.master',   'local[*]'),
                ('spark.app.name', 'PySpark Plane Data')])

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

        spark = SparkSession.builder.config(conf=conf).getOrCreate()
        spark_df = spark.read.csv("flight-delays/flights.csv",
                                  header=True,
                                  schema = schema).cache()

        return spark_df