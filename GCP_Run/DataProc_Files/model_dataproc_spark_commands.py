from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression 
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder

class PySpark_Code:
        '''
        Class --> PySpark_Code:
                        This class is in charge of running all the PySpark code from the data transformations, plot files, and 
                        even modeling we will do.

                        Input Variables: 
                                1. df: The PySpark dataframe created from the spark_session.py file.
                                2. spark: The Spark Session created from the spark_session.py file.
        '''

        def __init__(self, model_df, spark):
                '''initializaer --> __init__:
                        This initializer reads in variables we will be using throughout the rest of the functions below. The
                        dataframe, df and Spark Session spark are the variables we will initialize.
                '''
                self.model_df = model_df
                self.spark = spark


######### Add spark feature engineering here.
        def setup_vector_assembly(self):
                self.model_df = self.model_df.na.drop()

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

                feature_cols = ['distance',
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
                pipeline = Pipeline(stages=indexers + encoders + [assembler.setHandleInvalid("skip")])
                model = pipeline.fit(self.model_df)
                transformed = model.transform(self.model_df)

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

                lr = LogisticRegression(featuresCol='features', labelCol='arrival_delay_category_indexed', maxIter=1)
                lrModel = lr.fit(final_df)
                predictions = lrModel.transform(final_df)

                print(predictions.show(3))