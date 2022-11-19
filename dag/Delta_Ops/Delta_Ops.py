from pyspark.sql import SparkSession, functions as F
import findspark
import boto3
from datetime import datetime
import time
from pyspark.sql.types import *

# /opt/manual/spark: this is SPARK_HOME path
findspark.init("/opt/manual/spark")

# spark & minio & delta configuration on s3
spark = (SparkSession.builder
         .appName("Spark Minio Delta Handshake")
         .config("spark.hadoop.fs.s3a.endpoint", "http://172.18.0.2:9000")
         .config("spark.hadoop.fs.s3a.access.key", 'root')
         .config("spark.hadoop.fs.s3a.secret.key", 'root12345')
         .config("spark.hadoop.fs.s3a.path.style.access", True)
         .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
         .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.0,io.delta:delta-core_2.12:1.0.0")
         .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
         .getOrCreate())



# delta tables should be import after sparksession

from delta.tables import *

current_ts = datetime.now().strftime('%Y%m%d')

BUCKET = 'tmdb-bronze'
HTTP = 'http://'
ENDPOINT = 'localhost:9000'
AWS_ACCESS_KEY_ID = 'root'
AWS_SECRET_ACCESS_KEY = 'root12345'
REGIONNAME = 'whatever'

# boto3 configrations
session = boto3.Session(
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    aws_session_token=None,
    region_name=REGIONNAME,
    botocore_session=None,
    profile_name=None)

s3client = session.client(
    's3', endpoint_url=f"{HTTP}{ENDPOINT}")

s3resource = session.resource(
    's3', endpoint_url=f"{HTTP}{ENDPOINT}")

bronze_bucket = s3resource.Bucket('tmdb-bronze')
silver_bucket = s3resource.Bucket('tmdb-silver')


def bronze_file_names(table_head):
    file_names = []
    for bronze_bucket_object in bronze_bucket.objects.all():
        key = bronze_bucket_object.key
        if key.find(f"{table_head}/{table_head}_part_{current_ts}-") == 0:
            file_names.append(key[-14:-8])
    return file_names

# Readers
def read_credits(file_time):
    credits = (spark.read.format("parquet")
               .option("header", True)
               .option("inferSchema", True)
               .load(f"s3a://tmdb-bronze/credits/credits_part_{current_ts}-{file_time}.parquet"))

    struct_cast = StructType([StructField('cast_id', IntegerType(), True),
                              StructField('character', StringType(), True),
                              StructField('credit_id', StringType(), True),
                              StructField('gender', IntegerType(), True),
                              StructField('id', IntegerType(), True),
                              StructField('name', StringType(), True),
                              StructField('order', IntegerType(), True)])

    struct_crew = StructType([StructField('credit_id', StringType(), True),
                              StructField('department', StringType(), True),
                              StructField('gender', IntegerType(), True),
                              StructField('id', IntegerType(), True),
                              StructField('job', StringType(), True),
                              StructField('name', StringType(), True)])

    credits_sch = (credits.withColumn('cast', F.from_json(F.col('cast'), ArrayType(struct_cast)))
                   .withColumn('crew', F.from_json(F.col('crew'), ArrayType(struct_crew))))
    return credits_sch


def read_movies(file_time):
    movies = (spark.read.format("parquet")
              .option("header", True)
              .option("inferSchema", True)
              .load(f"s3a://tmdb-bronze/movies/movies_part_{current_ts}-{file_time}.parquet"))
    return movies


# Production of credits tables:
def cast(credits_sch):
    cast_exp = credits_sch.select('movie_id', 'title', F.explode_outer('cast').alias('cast'))
    cast_cols = ["cast_id", "character", "credit_id", "gender", "id", "name", "order"]
    for col in cast_cols:
        cast_exp = cast_exp.withColumn(col, F.col("cast").getItem(col))
    credits_cast = cast_exp.drop("cast").drop("order").na.fill(value="0000000000", subset=["credit_id"])
    return credits_cast, cast.__name__


def crew(credits_sch):
    crew_exp = credits_sch.select('movie_id', 'title', F.explode_outer('crew').alias('crew'))
    crew_cols = ["credit_id", "department", "gender", "id", "job", "name"]
    for col in crew_cols:
        crew_exp = crew_exp.withColumn(col, F.col("crew").getItem(col))
    crew_exp = crew_exp.drop("crew").na.fill(value="0000000000", subset=["credit_id"])

    return crew_exp, crew.__name__


# Production of movie tables:

def movies(movies_raw):
    movies_selected = movies_raw.withColumnRenamed("id", "movie_id").select("movie_id", "title", "budget", "homepage",
                                                                            "original_language", "original_title",
                                                                            "overview", "popularity", "release_date",
                                                                            "revenue", "runtime", "status", "tagline",
                                                                            "vote_average", "vote_count")
    movies_sch = (movies_selected.withColumn("movie_id", F.col("movie_id").cast(StringType()))
                  .withColumn("title", F.col("title").cast(StringType()))
                  .withColumn("budget", F.col("budget").cast(DoubleType()))
                  .withColumn("homepage", F.col("homepage").cast(StringType()))
                  .withColumn("original_language", F.col("original_language").cast(StringType()))
                  .withColumn("original_title", F.col("original_title").cast(StringType()))
                  .withColumn("overview", F.col("overview").cast(StringType()))
                  .withColumn("popularity", F.col("popularity").cast(FloatType()))
                  .withColumn("release_date", F.to_date(F.col("release_date"), "yyyy-M-d"))
                  .withColumn("revenue", F.col("revenue").cast(DoubleType()))
                  .withColumn("runtime", F.col("runtime").cast(IntegerType()))
                  .withColumn("status", F.col("status").cast(StringType()))
                  .withColumn("tagline", F.col("tagline").cast(StringType()))
                  .withColumn("vote_average", F.col("vote_average").cast(FloatType()))
                  .withColumn("vote_count", F.col("vote_count").cast(IntegerType()))
                  )
    return movies_sch, movies.__name__


def genres(movies):
    movies_genres = movies.withColumnRenamed("id", "movie_id").select("movie_id", "genres")
    struct_genres = StructType([StructField('id', IntegerType(), True),
                                StructField('name', StringType(), True)])
    genres_sch = movies_genres.withColumn('movie_id', F.col("movie_id").cast(StringType()))\
        .withColumn('genres',F.from_json(F.col('genres'),ArrayType(struct_genres)))
    genres_exp = genres_sch.select('movie_id', F.explode_outer('genres').alias('genres'))
    genres_cols = ["id", "name"]
    for col in genres_cols:
        genres_exp = genres_exp.withColumn(col, F.col("genres").getItem(col))
    genres_table = genres_exp.drop("genres").na.fill(value=-9999, subset=["id"])

    return genres_table, genres.__name__


def keywords(movies):
    movies_keywords = movies.withColumnRenamed("id", "movie_id").select("movie_id", "keywords")
    struct_keywords = StructType([StructField('id', IntegerType(), True), StructField('name', StringType(), True)])
    keyword_sch = (movies_keywords.withColumn('movie_id', F.col("movie_id").cast(StringType()))
                   .withColumn('keywords', F.from_json(F.col('keywords'), ArrayType(struct_keywords))))
    keyword_exp = keyword_sch.select('movie_id', F.explode_outer('keywords').alias('keywords'))
    keyword_cols = ["id", "name"]
    for col in keyword_cols:
        keyword_exp = keyword_exp.withColumn(col, F.col("keywords").getItem(col))
    keyword_exp.limit(5).toPandas()
    keywords_table = keyword_exp.drop("keywords").na.fill(value=-9999, subset=["id"])

    return keywords_table, keywords.__name__


def production_companies(movies):
    movie_pro_comp = movies.withColumnRenamed("id", "movie_id").select("movie_id", "production_companies")
    struct_pro_comp = StructType([StructField('id', IntegerType(), True), StructField('name', StringType(), True)])
    pro_comp_sch = movie_pro_comp.withColumn('movie_id', F.col("movie_id").cast(StringType())).withColumn(
        'production_companies', F.from_json(F.col('production_companies'), ArrayType(struct_pro_comp)))

    companies_exp = pro_comp_sch.select('movie_id',
                                        F.explode_outer('production_companies').alias('production_companies'))
    companies_cols = ["id", "name"]
    for col in companies_cols:
        companies_exp = companies_exp.withColumn(col, F.col("production_companies").getItem(col))

    companies_table = companies_exp.drop("production_companies").na.fill(value=-9999, subset=["id"])

    return companies_table, production_companies.__name__


def production_countries(movies):
    movie_countries = movies.withColumnRenamed("id", "movie_id").select("movie_id", "production_countries")
    struct_countries = StructType(
        [StructField('iso_3166_1', StringType(), True), StructField('name', StringType(), True)])
    countries_sch = movie_countries.withColumn('movie_id', F.col("movie_id").cast(StringType())).withColumn(
        'production_countries', F.from_json(F.col('production_countries'), ArrayType(struct_countries)))

    countries_exp = countries_sch.select('movie_id',
                                         F.explode_outer('production_countries').alias('production_countries'))
    countries_cols = ["iso_3166_1", "name"]
    for col in countries_cols:
        countries_exp = countries_exp.withColumn(col, F.col("production_countries").getItem(col))

    countries_table = countries_exp.drop("production_countries").na.fill(value="XX", subset=["iso_3166_1"])

    return countries_table, production_countries.__name__


def spoken_languages(movies):
    movie_languages = movies.withColumnRenamed("id", "movie_id").select("movie_id", "spoken_languages")
    struct_languages = StructType(
        [StructField('iso_639_1', StringType(), True), StructField('name', StringType(), True)])
    languages_sch = movie_languages.withColumn('movie_id', F.col("movie_id").cast(StringType())).withColumn(
        'spoken_languages', F.from_json(F.col('spoken_languages'), ArrayType(struct_languages)))

    languages_exp = languages_sch.select('movie_id', F.explode_outer('spoken_languages').alias('spoken_languages'))
    languages_cols = ["iso_639_1", "name"]
    for col in languages_cols:
        languages_exp = languages_exp.withColumn(col, F.col("spoken_languages").getItem(col))

    languages_table = languages_exp.drop("spoken_languages").na.fill(value="XX", subset=["iso_639_1"])

    return languages_table, spoken_languages.__name__


# Writer
def write_delta_to_silver(delta_table, table_name):
    delta_table.write.mode("overwrite").format("delta").save(f"s3a://tmdb-silver/{table_name}")
    # silver_bucket = s3resource.Bucket('tmdb-silver')
    # count = silver_bucket.objects.filter(Prefix=f"{current_ts}/_delta_log")
    # if len(list(count)) == 0:
    #     delta_table.write.mode("overwrite").format("delta").save(f"s3a://tmdb-silver/{current_ts}/")


# Organizers
def credits_organizer():
    file_names = bronze_file_names('credits')
    # print(bronze_file_names('credits'))
    credits_tables = [cast, crew]
    # Comparison columns according to tables:
    table_check_columns = {cast:{"key_1":"credit_id",
                                 "key_2":"movie_id"},
                           crew:{"key_1":"credit_id",
                                 "key_2":"movie_id"}}

    #tmdb-silver/table is empty?
    silver_bucket = s3resource.Bucket('tmdb-silver')


    for table in credits_tables:
        start_time = time.time()
        Is_there_delta = silver_bucket.objects.filter(Prefix=f"{table}/part")
        # print(f"{table} icin calisiyor")
        for i in file_names:
            if i == file_names[0]:
                credits_sch = read_credits(i)
                credits_part_first = table(credits_sch)[0]
                if len(list(Is_there_delta)) == 0:
                    write_delta_to_silver(credits_part_first, table(credits_sch)[1])
                    # print(f"first spark df in bucket {credits_part_first.count()}")
                else:
                    previous_delta_table = DeltaTable.forPath(spark, f"s3a://tmdb-silver/{table.__name__}/")
                    previous_delta_table.alias("p") \
                        .merge(credits_part_first.alias("c"),
                               (f"p.{table_check_columns[table]['key_1']} = c.{table_check_columns[table]['key_1']} AND "
                                f"p.{table_check_columns[table]['key_2']} = c.{table_check_columns[table]['key_2']}")) \
                        .whenMatchedUpdateAll() \
                        .whenNotMatchedInsertAll() \
                        .execute()

            else:
                credits_sch = read_credits(i)
                credits_part_new = table(credits_sch)[0]

                previous_delta_table = DeltaTable.forPath(spark, f"s3a://tmdb-silver/{table.__name__}/")

                previous_delta_table.alias("p") \
                    .merge(credits_part_new.alias("c"),
                           (f"p.{table_check_columns[table]['key_1']} = c.{table_check_columns[table]['key_1']} AND "
                            f"p.{table_check_columns[table]['key_2']} = c.{table_check_columns[table]['key_2']}")) \
                    .whenMatchedUpdateAll() \
                    .whenNotMatchedInsertAll() \
                    .execute()

    print("credits_organizer --- %s seconds ---" % (time.time() - start_time))

def movies_organizer():
    start_time = time.time()
    file_names = bronze_file_names('movies')
    movies_tables = [movies, genres, keywords, production_companies, production_countries, spoken_languages]
    # Comparison columns according to tables:
    table_check_columns = {movies:{"key_1":"movie_id",
                                 "key_2":"original_title"},
                           genres:{"key_1":"movie_id",
                                 "key_2":"id"},
                           keywords: {"key_1": "movie_id",
                                    "key_2": "id"},
                           production_companies: {"key_1": "movie_id",
                                    "key_2": "id"},
                           production_countries: {"key_1": "movie_id",
                                                  "key_2": "iso_3166_1"},
                           spoken_languages: {"key_1": "movie_id",
                                                  "key_2": "iso_639_1"}
                           }

    #tmdb-silver/table is empty?
    silver_bucket = s3resource.Bucket('tmdb-silver')


    for table in movies_tables:
        Is_there_delta = silver_bucket.objects.filter(Prefix=f"{table}/part")
        # print(f"{table} icin calisiyor")
        for i in file_names:
            if i == file_names[0]:
                # print(f"file_names:{file_names}")
                movies_raw = read_movies(i)
                movies_part_new = table(movies_raw)[0]
                # print(f"len(list(Is_there_delta))={len(list(Is_there_delta))}")
                if len(list(Is_there_delta)) == 0:
                    write_delta_to_silver(movies_part_new, table(movies_raw)[1])
                    # print(f"first spark df in bucket {movies_part_new.count()}")
                else:
                    previous_delta_table = DeltaTable.forPath(spark, f"s3a://tmdb-silver/{table.__name__}/")
                    previous_delta_table.alias("p") \
                        .merge(movies_part_new.alias("c"),
                               (f"p.{table_check_columns[table]['key_1']} = c.{table_check_columns[table]['key_1']} AND "
                                f"p.{table_check_columns[table]['key_2']} = c.{table_check_columns[table]['key_2']}")) \
                        .whenMatchedUpdateAll() \
                        .whenNotMatchedInsertAll() \
                        .execute()

            else:
                movies_raw = read_movies(i)
                movies_part_new = table(movies_raw)[0]

                previous_delta_table = DeltaTable.forPath(spark, f"s3a://tmdb-silver/{table.__name__}/")

                previous_delta_table.alias("p") \
                    .merge(movies_part_new.alias("c"),
                           (f"p.{table_check_columns[table]['key_1']} = c.{table_check_columns[table]['key_1']} AND "
                            f"p.{table_check_columns[table]['key_2']} = c.{table_check_columns[table]['key_2']}")) \
                    .whenMatchedUpdateAll() \
                    .whenNotMatchedInsertAll() \
                    .execute()

    print("movies_organizer --- %s seconds ---" % (time.time() - start_time))