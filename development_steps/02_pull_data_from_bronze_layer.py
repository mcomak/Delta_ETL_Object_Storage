from pyspark.sql import SparkSession, functions
import findspark
import pandas as pd
pd.set_option('display.max_colwidth', None)
pd.set_option('display.max_colwidth', None)

import os
# /opt/manual/spark: this is SPARK_HOME path
findspark.init("/opt/manual/spark")

#spark = SparkSession.builder.config("spark.jars.packages","org.apache.hadoop:hadoop-aws:3.2.0").getOrCreate()


# spark & minio configuration on s3
spark = (SparkSession.builder
    .appName("Spark Minio Test")
    .config("spark.hadoop.fs.s3a.endpoint", "http://172.18.0.3:9000")
    .config("spark.hadoop.fs.s3a.access.key", 'root')
    .config("spark.hadoop.fs.s3a.secret.key", 'root12345')
    .config("spark.hadoop.fs.s3a.path.style.access", True)
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.jars.packages","org.apache.hadoop:hadoop-aws:3.2.0")
    .getOrCreate())

# .config("spark.hadoop.fs.s3a.access.key", os.environ.get('root'))
# .config("spark.hadoop.fs.s3a.secret.key", os.environ.get('root12345'))


# Read csv from minio
# df =(spark.read.format("csv")
#      .option("header", True)
#      .option("inferSchema",True)
#      .load("s3a://dataops/sales/20221023/mall_customers_20221023155942.csv") )
# print(df.limit(10).toPandas())



# Movies Read parquet from minio

# df =(spark.read.format("parquet")
#      .option("header", True)
#      .option("inferSchema",True)
#      .load("s3a://tmdb-bronze/movies/movies_part_20221023-222937.parquet"))
# df.show(10,truncate=False)



credits =(spark.read.format("parquet")
     .option("header", True)
     .option("inferSchema",True)
     .load("s3a://tmdb-bronze/credits/credits_part_20221023-230801.parquet"))
credits.show(10,truncate=False)

credits.printSchema()



# +--------+------+-------+-------------------+------------------------+------+-----+----------------+
# |movie_id|title |cast_id|character          |credit_id               |gender|id   |name            |
# +--------+------+-------+-------------------+------------------------+------+-----+----------------+
# |19995   |Avatar|242    |Jake Sully         |5602a8a7c3a3685532001c9a|2     |65731|Sam Worthington |
# |19995   |Avatar|3      |Neytiri            |52fe48009251416c750ac9cb|1     |8691 |Zoe Saldana     |
# |19995   |Avatar|25     |Dr. Grace Augustine|52fe48009251416c750aca39|1     |10205|Sigourney Weaver|
# |19995   |Avatar|4      |Col. Quaritch      |52fe48009251416c750ac9cf|2     |32747|Stephen Lang    |
# +--------+------+-------+-------------------+------------------------+------+-----+----------------+

spark.stop()

