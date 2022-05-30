from pyspark.ml.classification import LogisticRegression 
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler

from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql.types import StringType, FloatType
from pyspark.sql.functions import col, udf, row_number, lit

import pyspark.sql.functions as f
from pyspark.sql.window import Window

from pyspark.mllib.evaluation import MulticlassMetrics

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
                
                self.save_final_model_dataframe = False
                self.save_category_counts = False
                self.save_model_preds = False
                self.save_model = True


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
                assembler = VectorAssembler(inputCols=feature_cols, outputCol="vectorfeatures")
                standard_scaler = StandardScaler(inputCol = 'vectorfeatures', outputCol = 'features')
                pipeline = Pipeline(stages=indexers + encoders + [assembler.setHandleInvalid("skip")] + [standard_scaler])
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
                self.final_df = transformed[final_cols]



                # # This works, just commenting it cuz it takes 4.5 minutes to process the query.
                # ## Sending over final df to gcs bucket
                self.sparse_format_udf = udf(lambda x: str(x), StringType())
                if self.save_final_model_dataframe == True:
                        self.final_df.withColumn('features', self.sparse_format_udf(col('features'))).coalesce(1).write.csv(path='gs://plane-pyspark-run/Spark_Data_Output/final_model_df.csv',
                                        mode='overwrite',
                                        header=True)


                # # Sending over counts df to gcs bucket
                if self.save_category_counts == True:
                        count_groups = self.final_df.groupBy("arrival_delay_category").count()
                        count_groups = count_groups.withColumn('percent', f.col('count')/f.sum('count').over(Window.partitionBy())).orderBy('percent', ascending=False)
                        count_groups.coalesce(1).write.csv(path='gs://plane-pyspark-run/Spark_Data_Output/label_proportion_table.csv',
                                                                        mode='overwrite',
                                                                        header=True)

        def run_logistic_regression_model(self):
                ## Splitting data into training and testing using Stratification
                test_df = self.spark.createDataFrame([], schema=self.final_df.schema)
                train_df = self.spark.createDataFrame([], schema=self.final_df.schema)

                self.final_df.createOrReplaceTempView("df_small")
                df_distinct = self.spark.sql("""SELECT distinct arrival_delay_category from df_small""")

                for i in df_distinct.collect():
                        df_separated = self.final_df.filter(self.final_df.arrival_delay_category == i["arrival_delay_category"])
                        df_test_temp, df_train_temp = df_separated.randomSplit([.2,.8], 100)
                        test_df = test_df.union(df_test_temp)
                        train_df = train_df.union(df_train_temp)
  
                # Creating the Baseline Model
                lr_all = LogisticRegression(featuresCol = 'features', 
                            labelCol='arrival_delay_category_indexed',
                            maxIter=1)
                lr_Model_all = lr_all.fit(train_df)

                predictions_train = lr_Model_all.transform(train_df)
                predictions_train = predictions_train.select('arrival_delay_category_indexed', 
                                                        'features', 
                                                        'rawPrediction', 
                                                        'prediction', 
                                                        'probability')

                predictions_test = lr_Model_all.transform(test_df)
                predictions_test = predictions_test.select('arrival_delay_category_indexed', 
                                                        'features', 
                                                        'rawPrediction', 
                                                        'prediction', 
                                                        'probability')


                if self.save_model_preds == True:
                        predictions_train.withColumn('features', self.sparse_format_udf(col('features')))\
                                .withColumn('rawPrediction', self.sparse_format_udf(col('rawPrediction')))\
                                        .withColumn('probability', self.sparse_format_udf(col('probability')))\
                                                .coalesce(1).write.csv(path='gs://plane-pyspark-run/Spark_Data_Output/predictions_train_table.csv',
                                                                mode='overwrite',
                                                                header=True)
                        # Saving the preditions made on the testing data
                        predictions_test.withColumn('features', self.sparse_format_udf(col('features')))\
                                .withColumn('rawPrediction', self.sparse_format_udf(col('rawPrediction')))\
                                        .withColumn('probability', self.sparse_format_udf(col('probability')))\
                                                .coalesce(1).write.csv(path='gs://plane-pyspark-run/Spark_Data_Output/predictions_test_table.csv',
                                                                        mode='overwrite',
                                                                        header=True)
                if self.save_model == True:
                        lr_Model_all.write().overwrite().save("gs://plane-pyspark-run/Spark_Models/lr_model_all")


                train_accuracy = predictions_train.filter(predictions_train.arrival_delay_category_indexed == predictions_train.prediction).count() / float(predictions_train.count())
                print("Train Accuracy : {}".format(train_accuracy))

                test_accuracy = predictions_test.filter(predictions_test.arrival_delay_category_indexed == predictions_test.prediction).count() / float(predictions_test.count())
                print("Test Accuracy : {}".format(test_accuracy))

                # Prints conufsion Matrix Values
                preds_and_labels = predictions_test.select(['prediction','arrival_delay_category_indexed'])\
                        .withColumn('label', f.col('arrival_delay_category_indexed').cast(FloatType())).orderBy('prediction')

                preds_and_labels = preds_and_labels.select(['prediction','label'])

                metrics = MulticlassMetrics(preds_and_labels.rdd.map(tuple))

                print(metrics.confusionMatrix().toArray())

                # Model metrics
                trainingSummary = lr_Model_all.summary
                accuracy = trainingSummary.accuracy
                falsePositiveRate = trainingSummary.weightedFalsePositiveRate
                truePositiveRate = trainingSummary.weightedTruePositiveRate
                fMeasure = trainingSummary.weightedFMeasure()
                precision = trainingSummary.weightedPrecision
                recall = trainingSummary.weightedRecall
                print("Accuracy: %s\nFPR: %s\nTPR: %s\nF-measure: %s\nPrecision: %s\nRecall: %s"
                % (accuracy, falsePositiveRate, truePositiveRate, fMeasure, precision, recall))


                ## Hyperparameters took to long to run, however it can be done, not fasible, unless you have more power.
                # # Create ParamGrid for Cross Validation
                # paramGrid = (ParamGridBuilder()
                #         .addGrid(lr_all.regParam, [0.01, 0.5])# regularization parameter
                #         .addGrid(lr_all.elasticNetParam, [0.0, 0.5])# Elastic Net Parameter (Ridge = 0)
                #         .addGrid(lr_all.maxIter, [1, 5])#Number of iterations
                #         .build())

                # cv = CrossValidator(estimator=lr_all, estimatorParamMaps=paramGrid, 
                #                 evaluator=evaluator, numFolds=10)

                # cvModel = cv.fit(train_df)

                # predictions = cvModel.transform(test_df)
                # best_model=cvModel.bestModel