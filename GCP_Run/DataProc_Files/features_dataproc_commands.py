# import spark libraries
import sometthing here 


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


######### Add spark feature engineering here.
        def feature_enginerring(self):
                '''
                Function --> feature_engineering:          

                                Input Variables
                                        None

                                Output Variables
                                        None
                '''
            # feature engineering code added here.