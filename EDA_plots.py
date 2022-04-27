import pyspark
from pyspark.sql import SparkSession

# import plotly.plotly as py
import plotly.graph_objs as go

conf = pyspark.SparkConf().setAll([
         ('spark.master',   'local[*]'),
         ('spark.app.name', 'PySpark Demo')])
spark = SparkSession.builder.config(conf=conf).getOrCreate()

df = spark.read.csv("flight-delays/flights.csv",
                    header=True).cache()

origin_airport_count = df.groupBy('ORIGIN_AIRPORT').count().orderBy('count', ascending=False)

fig = go.Figure([go.Bar(x=origin_airport_count.toPandas()['ORIGIN_AIRPORT'], y=origin_airport_count.toPandas()['count'])])
fig.update_layout(title={'text': "Origin Airport Counts",
                         'x':0.5,
                         'xanchor': 'center',
                         'yanchor': 'top'},
                 xaxis_title="Origin Aiport",
                 yaxis_title="Count")
fig.write_image("EDA_Images/Origin_Airport_Count.png")
fig.show()