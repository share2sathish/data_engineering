# -*- coding: utf-8 -*-
"""
Created on Fri Feb 17 12:54:19 2023

@author: Jyo
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

if __name__ == "__main__":
    spark = ( SparkSession
              .builder
              .appName('AuthorsAges')
              .getOrCreate()
            )
    
    data = [("Brooke", 20), ("Denny", 31), ("Jules", 30),("TD", 35), ("Brooke", 25)]
    columns=["Name","age"]
    df_AuthorAge = spark.createDataFrame(data, columns)
    avg_AuthorAge = df_AuthorAge.groupBy("name").agg(avg("age").alias('avg_age'))
    avg_AuthorAge.show()
