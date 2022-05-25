import calendar

from pyspark.sql.functions import col, when
from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.classification import LogisticRegression



class PySpark_Code:
        '''
        Class --> PySpark_Code:
                        This class is in charge of running all the PySpark code from the data transformations, plot files, and 
                        even modeling we will do.

                        Input Variables: 
                                1. df: The PySpark dataframe created from the spark_session.py file.
                                2. spark: The Spark Session created from the spark_session.py file.
        '''

        def __init__(self, df_flights, df_airports, df_airlines, spark):
                '''initializaer --> __init__:
                        This initializer reads in variables we will be using throughout the rest of the functions below. The
                        dataframe, df and Spark Session spark are the variables we will initialize.
                '''
                self.flight_df = df_flights
                self.airport_df = df_airports
                self.airline_df = df_airlines
                self.spark = spark


######### Add spark feature engineering here.
        def feature_engineering(self):
                self.flight_df.createOrReplaceTempView("flights")
                self.airline_df.createOrReplaceTempView("airlines")
                self.airport_df.createOrReplaceTempView("airports")

                df = self.spark.sql("""
                SELECT month,
                       day,
                       day_of_week,
                       al.airline,
                       ap1.city    AS city_origin,
                       ap1.state   AS state_origin,
                       ap1.airport AS airport_origin,
                       flights.origin_airport as airport_origin_abbrv,
                       ap2.city    AS city_destination,
                       ap2.state   AS state_destination,
                       ap2.airport AS airport_destination,
                       flights.destination_airport as airport_destination_abbrv,
                       scheduled_departure,
                       Floor(scheduled_departure / 100) AS depature_floored_hour,
                       scheduled_arrival,
                       Floor(scheduled_arrival / 100) AS arrival_floored_hour,
                       distance,
                       diverted,
                       cancelled,
                       arrival_delay
                FROM   flights
                       left join airports ap1
                              ON flights.origin_airport = ap1.iata_code
                       left join airports ap2
                              ON flights.destination_airport = ap2.iata_code
                       left join airlines al
                              ON flights.airline = al.iata_code""")

                ##Create categories for delay time
                df = df.withColumn("arrival_delay_category", (when(col("ARRIVAL_DELAY") <= -15, "Super Early"))
                                   .when(col("ARRIVAL_DELAY") <= -5, "Slightly Early")
                                   .when(col("ARRIVAL_DELAY") <= 5, "On Time")
                                   .when(col("ARRIVAL_DELAY") <= 15, "Slightly Deylayed")
                                   .when(col("ARRIVAL_DELAY") > 15, "Super Delayed")
                                   .when(col("DIVERTED") == 1, "Diverted")
                                   .when(col("CANCELLED") == 1, "Cancelled")
                                   .otherwise("PROBLEM WITH DATA"))
                print("1")

                df_flight_counts = self.spark.sql("""
                SELECT departing.*,
                       flights_arriving
                FROM   (SELECT month,
                               day_of_week,
                               day,
                               origin_airport                   AS airport_abbrv,
                               Floor(scheduled_departure / 100) AS floored_hour,
                               Count(*)                         AS flights_departing
                        FROM   flights
                        GROUP  BY month,
                                  day_of_week,
                                  day,
                                  origin_airport,
                                  floored_hour) departing
                       INNER JOIN (SELECT month,
                                          day_of_week,
                                          day,
                                          destination_airport            AS airport_abbrv,
                                          Floor(scheduled_arrival / 100) AS floored_hour,
                                          Count(*)                       AS flights_arriving
                                   FROM   flights
                                   GROUP  BY month,
                                             day_of_week,
                                             day,
                                             airport_abbrv,
                                             floored_hour) arriving
                               ON departing.month = arriving.month
                                  AND departing.day_of_week = arriving.day_of_week
                                  AND departing.airport_abbrv = arriving.airport_abbrv
                                  AND departing.floored_hour = arriving.floored_hour 
                                  AND departing.floored_hour = arriving.floored_hour
                """)
                print("2")

                df.createOrReplaceTempView("ml_df")
                df_flight_counts.createOrReplaceTempView("flight_counts")

                df_temp = self.spark.sql("""
                SELECT ml.*,
                fc1.flights_arriving as origin_airport_flights_arriving,
                fc1.flights_departing as origin_airport_flights_departing
                from
                ml_df ml
                left join
                flight_counts fc1
                on ml.month = fc1.month
                and ml.airport_origin_abbrv = fc1.airport_abbrv
                and ml.day = fc1.day
                and ml.day_of_week = fc1.day_of_week
                and ml.depature_floored_hour = fc1.floored_hour
                """)

                df_temp.createOrReplaceTempView("df_temp")

                df = self.spark.sql("""
                SELECT dt.*,
                fc2.flights_arriving as destination_airport_flights_arriving,
                fc2.flights_departing as destination_airport_flights_departing
                from
                df_temp dt
                left join
                flight_counts fc2
                on dt.month = fc2.month
                and dt.day = fc2.day
                and dt.airport_destination_abbrv = fc2.airport_abbrv
                and dt.day_of_week = fc2.day_of_week
                and dt.arrival_floored_hour = fc2.floored_hour
                """)
                print("3")
                ##Change months to string months (12-> December) and day_of_week to string day (1->Monday)
                month_name = F.udf(lambda x: calendar.month_name[int(x)])
                day_name = F.udf(lambda x: calendar.day_name[int(x) - 1])

                df = df.withColumn("month", month_name(F.col("month"))).withColumn("day_of_week",
                                                                                   day_name(F.col("day_of_week")))

                cols = ['month',
                        'day_of_week',
                        'airline',
                        'airport_origin',
                        'airport_destination',
                        'depature_floored_hour',
                        'arrival_floored_hour',
                        'distance',
                        'origin_airport_flights_arriving',
                        'origin_airport_flights_departing',
                        'destination_airport_flights_arriving',
                        'destination_airport_flights_departing',
                        'arrival_delay_category']
                df = df[cols]
                print("4")
                # The index of string vlaues multiple columns

                # cat_cols = ['month',
                #             'day_of_week',
                #             'airline',
                #             'airport_origin',
                #             'airport_destination',
                #             'depature_floored_hour',
                #             'arrival_floored_hour']

                cat_cols = ['month',
                            'day_of_week',
                            'airline',
                            'airport_origin',
                            'airport_destination',
                            'depature_floored_hour',
                            'arrival_floored_hour',
                            'arrival_delay_category']

                indexers = [
                        StringIndexer(inputCol=c, outputCol="{0}_indexed".format(c))
                        for c in cat_cols
                ]

                # The encode of indexed vlaues multiple columns
                encoders = [OneHotEncoder(dropLast=False, inputCol=indexer.getOutputCol(),
                                          outputCol="{0}_encoded".format(indexer.getOutputCol()))
                            for indexer in indexers
                            ]


                # feature_cols = [
                #         'distance',
                #         'origin_airport_flights_arriving',
                #         'origin_airport_flights_departing',
                #         'destination_airport_flights_arriving',
                #         'destination_airport_flights_departing',
                #         'month_indexed',
                #         'day_of_week_indexed',
                #         'airline_indexed',
                #         'airport_origin_indexed',
                #         'airport_destination_indexed',
                #         'depature_floored_hour_indexed',
                #         'arrival_floored_hour_indexed',
                #         'month_indexed_encoded',
                #         'day_of_week_indexed_encoded',
                #         'airline_indexed_encoded',
                #         'airport_origin_indexed_encoded',
                #         'airport_destination_indexed_encoded',
                #         'depature_floored_hour_indexed_encoded',
                #         'arrival_floored_hour_indexed_encoded']

                feature_cols = [
                        'distance',
                        'origin_airport_flights_arriving',
                        'origin_airport_flights_departing',
                        'destination_airport_flights_arriving',
                        'destination_airport_flights_departing',
                        'month_indexed',
                        'day_of_week_indexed',
                        'airline_indexed',
                        'airport_origin_indexed',
                        'airport_destination_indexed',
                        'depature_floored_hour_indexed',
                        'arrival_floored_hour_indexed',
                        'month_indexed_encoded',
                        'day_of_week_indexed_encoded',
                        'airline_indexed_encoded',
                        'airport_origin_indexed_encoded',
                        'airport_destination_indexed_encoded',
                        'depature_floored_hour_indexed_encoded',
                        'arrival_floored_hour_indexed_encoded']

                # Vectorizing encoded values
                assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

                pipeline = Pipeline(stages=indexers + encoders + [assembler])

                model = pipeline.fit(df)
                transformed = model.transform(df)

                print(transformed.columns)

                final_cols = ['month',
                              'day_of_week',
                              'airline',
                              'airport_origin',
                              'airport_destination',
                              'depature_floored_hour',
                              'arrival_floored_hour',
                              'distance',
                              'origin_airport_flights_arriving',
                              'origin_airport_flights_departing',
                              'destination_airport_flights_arriving',
                              'destination_airport_flights_departing',
                              'features',
                              'arrival_delay_category',
                              'arrival_delay_category_indexed']
                final_df = transformed[final_cols]


                print(final_df.count())
                
                # final_df.coalesce(1).write.csv(path='gs://plane-pyspark-run/Spark_Data_Output/model_df.csv',
                #                                     mode='overwrite',
                #                                     header=True)

                # lr = LogisticRegression(featuresCol='features', labelCol='arrival_delay_category_indexed', maxIter=1)
                # lrModel = lr.fit(final_df)
                # predictions = lrModel.transform(final_df)
