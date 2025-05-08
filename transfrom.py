from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
from pyspark.sql.functions import regexp_replace, trim, col, count, expr

# load data
data = spark.read.csv(f'airbnb.csv', header=True, inferSchema=True, quote='"', escape='"', multiLine=True, encoding="UTF-8")

# ============================


# Exploratory Data Analysis


# ============================

# melihat jumlah rows
data.count()

# melihat dtypes
data.printSchema()

# melihat nama-nama kolom
data.columns

# meubah nama kolom dengan tipe snake case
data = data.withColumnRenamed('NAME', 'name')
data = data.withColumnRenamed('host id', 'host_id')
data = data.withColumnRenamed('host name', 'host_name')
data = data.withColumnRenamed('neighbourhood group', 'neighbourhood_group')
data = data.withColumnRenamed('lat', 'latitude')
data = data.withColumnRenamed('long', 'longitude')
data = data.withColumnRenamed('country code', 'country_code')
data = data.withColumnRenamed('room type', 'room_type')
data = data.withColumnRenamed('Construction year', 'construction_year')
data = data.withColumnRenamed('service fee', 'service_fee')
data = data.withColumnRenamed('minimum nights', 'minimum_nights')
data = data.withColumnRenamed('number of reviews', 'number_of_reviews')
data = data.withColumnRenamed('last review', 'last_review')
data = data.withColumnRenamed('reviews per month', 'reviews_per_month')
data = data.withColumnRenamed('review rate number', 'review_rate_number')
data = data.withColumnRenamed('calculated host listings count', 'calculated_host_listings_count')
data = data.withColumnRenamed('availability 365', 'availability_365')

# melihat jumlah missing values
Dict_Null = {col:data.filter(data[col].isNull()).count() for col in data.columns}
Dict_Null

# ============================

# Drop Data Duplicate

# ============================
# melihat data duplikat
data.count() - data.dropDuplicates().count()

data = data.dropDuplicates()
print("After Dropping Duplicate Rows:")
data.show()

# ============================

# Data Cleaning

# ============================
# imputasi data object dengan unknown
data = data.na.fill({
    'name': 'Unknown',
    'host_identity_verified': 'Unknown',
    'host_name': 'Unknown',
    'neighbourhood_group': 'Unknown',
    'neighbourhood': 'Unknown',
    'country': 'Unknown',
    'country_code': 'Unknown',
    'cancellation_policy': 'Unknown',
    'house_rules': 'Unknown',
    'license': 'Unknown'
})

# imputasi data bool dengan False
data = data.na.fill ({'instant_bookable': False})

# imputasi data float dengan 0.0
data = data.na.fill ({'latitude': 0.0, 'longitude': 0.0, 'reviews_per_month': 0.0})

# imputasi data float dengan 0.0
data = data.na.fill ({'latitude': 0.0, 'longitude': 0.0, 'reviews_per_month': 0.0})

# menghitung mode
mode_value = (
    data.groupBy('construction_year')
    .count()
    .orderBy(col('count').desc())
    .first()[0]
)
# lalu imputasi construction_year menggunakan nilai di atas
data = data.fillna({'construction_year': mode_value})

non_null_date = data.filter(col('last_review').isNotNull())

# menghitung mode
if non_null_date.count() > 0:
    mode_value = (
        non_null_date.groupBy('last_review')
        .agg(count('*').alias('jumlah'))
        .orderBy(col('jumlah').desc())
        .first()[0]
    )

# lalu imputasi last_review menggunakan nilai di atas
data = data.fillna({'last_review': mode_value})

# TAHAP INI UBAH DULU DTYPES lalu ambil median menggunakan percentile_approx
median_value = data.selectExpr("percentile_approx(price, 0.5)").first()[0]
print("Median value:", median_value)

# jika jumlah median valid, imputasi price dengan median
if median_value is not None:
    data = data.fillna({'price': median_value})
else:
    print("Median value is None, imputasi dibatalkan.")

# TAHAP INI UBAH DULU DATYPES lalu ambil median menggunakan percentile_approx
median_value = data.selectExpr("percentile_approx(price, 0.5)").first()[0]
print("Median value:", median_value)

# jika jumlah median valid, imputasi service_fee dengan median
if median_value is not None:
    data = data.fillna({'service_fee': median_value})
else:
    print("Median value is None, imputasi dibatalkan.")

# ============================


# Fix Dtypes


# ============================

data.select('construction_year', 'last_review').show(15)

# replace tanda yang tidak diinginkan
data = data.withColumn("price", regexp_replace("price", "\\$", ""))
data = data.withColumn("price", regexp_replace("price", ",", ""))

# lalu ubah tipe data menjadi float
data = data.withColumn('price', col('price').cast('float'))

# replace tanda yang tidak diinginkan
data = data.withColumn("service_fee", regexp_replace("service_fee", "\\$", ""))
data = data.withColumn("service_fee", regexp_replace("service_fee", ",", ""))

# lalu ubah tipe data service_fee menjadi float
data = data.withColumn('service_fee', col('service_fee').cast('float'))

# ubah tipe data menjadi integer
data = data.withColumn("minimum_nights", col("minimum_nights").cast("int")) \
       .withColumn("number_of_reviews", col("number_of_reviews").cast("int")) \
       .withColumn("review_rate_number", col("review_rate_number").cast("int")) \
       .withColumn("calculated_host_listings_count", col("calculated_host_listings_count").cast("int")) \
       .withColumn("availability_365", col("availability_365").cast("int"))

from pyspark.sql.functions import col, to_timestamp, concat_ws, lit

# meubah tipe data construction_year menjadi timestamp
data = data.withColumn(
    "construction_year",
    to_timestamp(concat_ws("-", col("construction_year").cast("string"), lit("01"), lit("01")), "yyyy-MM-dd")
)

# simpan data
data.write.csv('transfrom_airbnb.csv', header=True)