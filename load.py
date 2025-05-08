import pandas as pd
from pymongo import MongoClient

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MongoLoader") \
    .getOrCreate()

from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType, BooleanType, TimestampType

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("host_id", IntegerType(), True),
    StructField("host_identity_verified", StringType(), True),
    StructField("neighbourhood_group", StringType(), True),
    StructField("neighbourhood", StringType(), True),
    StructField("latitude", FloatType(), True),
    StructField("longitude", FloatType(), True),
    StructField("country", StringType(), True),
    StructField("country_code", StringType(), True),
    StructField("instant_bookable", BooleanType(), True),
    StructField("cancellation_policy", StringType(), True),
    StructField("room_type", StringType(), True),
    StructField("construction_year", StringType(), True),  # ubah jadi StringType utk hindari error timestamp
    StructField("price", FloatType(), True),
    StructField("service_fee", FloatType(), True),
    StructField("minimum_nights", IntegerType(), True),
    StructField("last_review", TimestampType(), True),
    StructField("reviews_per_month", FloatType(), True),
    StructField("calculated_host_listings_count", IntegerType(), True),
    StructField("availability_365", IntegerType(), True),
    StructField("house_rules", StringType(), True),
    StructField("license", StringType(), True)
])

df = spark.read.csv(f'transfrom_airbnb.csv/part-00000-30aec09e-cd43-4f95-9143-5af58cef2a22-c000.csv', header=True, inferSchema=True, quote='"', escape='"', multiLine=True, encoding="UTF-8")

client = MongoClient("mongodb://admin:adminsukses@ac-rm6y0lf-shard-00-00.5zftqmy.mongodb.net:27017,ac-rm6y0lf-shard-00-01.5zftqmy.mongodb.net:27017,ac-rm6y0lf-shard-00-02.5zftqmy.mongodb.net:27017/?replicaSet=atlas-129mhz-shard-0&ssl=true&authSource=admin")

pdf = df.limit(1000).toPandas()
client = MongoClient("mongodb://admin:adminsukses@ac-rm6y0lf-shard-00-00.5zftqmy.mongodb.net:27017,ac-rm6y0lf-shard-00-01.5zftqmy.mongodb.net:27017,ac-rm6y0lf-shard-00-02.5zftqmy.mongodb.net:27017/?replicaSet=atlas-129mhz-shard-0&ssl=true&authSource=admin")  # sesuaikan URL-nya

document_list = pdf.to_dict(orient='records')
client.sample_mflix.airbnb.insert_many(document_list)