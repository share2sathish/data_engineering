# -*- coding: utf-8 -*-
"""
Created on Fri Feb 17 15:49:55 2023

@author: Jyo
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = ( SparkSession
          .builder
          .master("local[1]")
          .appName("File read and write")
          .getOrCreate()
         )

src_file = 'sf-fire-calls.csv'
schema = StructType([
    StructField('CallNumber',IntegerType(),True),
    StructField('UnitID', StringType(), True),
    StructField('IncidentNumber',IntegerType(),True),
    StructField('CallType', StringType(), True),
    StructField('CallDate', StringType(), True),
    StructField('WatchDate', StringType(), True),
    StructField('CallFinalDisposition', StringType(), True),
    StructField('AvailableDtTm', StringType(), True),
    StructField('Address', StringType(), True),
    StructField('City', StringType(), True),
    StructField('Zipcode', IntegerType(), True),
    StructField('Battalion', StringType(), True),
    StructField('StationArea', StringType(), True),
    StructField('Box', StringType(), True),
    StructField('OriginalPriority', StringType(), True),
    StructField('Priority', StringType(), True),
    StructField('FinalPriority', IntegerType(), True),
    StructField('ALSUnit', BooleanType(), True),
    StructField('CallTypeGroup', StringType(), True),
    StructField('NumAlarms', IntegerType(), True),
    StructField('UnitType', StringType(), True),
    StructField('UnitSequenceInCallDispatch', IntegerType(), True),
    StructField('FirePreventionDistrict', StringType(), True),
    StructField('SupervisorDistrict', StringType(), True),
    StructField('Neighborhood', StringType(), True),
    StructField('Location', StringType(), True),
    StructField('RowID', StringType(), True),
    StructField('Delay', FloatType(), True)
    ])

df = spark.read.csv(src_file, header="true", schema=schema)

df.select(col("CallType")).where(col("CallType").isNotNull())\
    .agg(countDistinct("CallType").alias("unique call type"))\
        .show(20, True)

df.select(col('CallType')).where(col("CallType").isNotNull()).distinct().show(20, True)
  
parquet_path = 'first_parquet_file.parquet'
df.write.mode('overwrite').format("parquet").save(parquet_path)
parqDF=spark.read.parquet(parquet_path)
parqDF.createOrReplaceTempView("ParquetTable")
spark.sql("select CallType from ParquetTable limit 10").show()
      
df2 = spark.read.format("csv") \
      .option("header", True) \
      .schema(schema) \
      .load(src_file)
df2.printSchema()
      

