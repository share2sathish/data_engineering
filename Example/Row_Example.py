# -*- coding: utf-8 -*-
"""
Created on Fri Feb 17 15:16:22 2023

@author: Jyo
"""

from pyspark.sql import SparkSession, Row
spark = ( SparkSession
         .builder
         .appName("Row Example")
         .getOrCreate()
         )
rows = [Row("Matei Zaharia", "CA"), Row("Reynold Xin", "CA")]
authors_df = spark.createDataFrame(rows, ["Authors", "State"])
print(authors_df[1][1])
authors_df.show()
