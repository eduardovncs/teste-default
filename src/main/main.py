from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Teste").getOrCreate()

spark.range(1, 7, 2).display()
