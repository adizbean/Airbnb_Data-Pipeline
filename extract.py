import pyspark
from pyspark.sql import SparkSession
# load data
def load_extract(file_path):
    spark = SparkSession.builder.getOrCreate()
    data = spark.read.csv(file_path, header=True, inferSchema=True)
    return data
# simpan data
df = load_extract("P2M3_gadist_diasmi_data_raw.csv")
df.write.option("header",True) \
    .csv('airbnb_extract_pipeline.csv', mode='overwrite')