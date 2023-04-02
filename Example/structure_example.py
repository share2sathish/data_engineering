# -*- coding: utf-8 -*-
"""
Created on Fri Feb 17 13:43:37 2023

@author: Jyo
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, expr, concat

if __name__ == "__main__":    
    spark = (SparkSession
          .builder
          .appName("StructureExample")
          .getOrCreate()
          )
    
    data = [[1, "Jules", "Damji", "https://tinyurl.1", "1/4/2016", 4535, ["twitter","LinkedIn"]],
            [2, "Brooke","Wenig", "https://tinyurl.2", "5/5/2018", 8908, ["twitter","LinkedIn"]],
            [3, "Denny", "Lee", "https://tinyurl.3", "6/7/2019", 7659, ["web","twitter", "FB", "LinkedIn"]],
            [4, "Tathagata", "Das", "https://tinyurl.4", "5/12/2018", 10568,["twitter", "FB"]],
            [5, "Matei","Zaharia", "https://tinyurl.5", "5/14/2014", 40578, ["web","twitter", "FB", "LinkedIn"]],
            [6, "Reynold", "Xin", "https://tinyurl.6", "3/2/2015", 25568,["twitter", "LinkedIn"]]
           ]
    schema = """
                `ID` INT, `FIRST_NAME` STRING, `LAST_NAME` STRING, `URL` STRING,
                 `PUBLISHED` STRING, `HITS` INT, `CAMPAIGN` ARRAY<STRING> 
            """
    
    df = spark.createDataFrame(data=data, schema=schema)
    df.show()
    print(df.printSchema())
    df.select(expr("HITS * 2").alias("NEW HITS")).show()
    df.select(col("FIRST_NAME")).show()
    df.withColumn("AuthorId", expr("FIRST_NAME || LAST_NAME || '_' || ID" ))\
        .select(col("AuthorId")).show()
      
    df.withColumn("bigger HITS", expr("HITS > 10000")).show()
  

    