# -*- coding: utf-8 -*-
"""
Created on Fri Feb 10 14:01:05 2023

@author: Jyo
"""
from pyspark.sql import SparkSession
spark = ( SparkSession
         .builder
         .appName('find partition)')
         .getOrCreate())
df = spark.range(0, 100, 1, 8)
print(df.rdd.getNumPartitions())

print(df.rdd.glom().collect())

print(spark.conf.get("spark.sql.files.maxPartitionBytes"))