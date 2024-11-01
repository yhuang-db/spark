from pyspark.sql import SparkSession
import datetime
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("-d", type=str, help="test data")
dataset = parser.parse_args().d

ct = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")

spark = SparkSession.builder.appName("LargeRowBenchmark-UDFLength").getOrCreate()


# UDF
def udf_length(text):
    return len(text)


spark.udf.register("udf_length", udf_length)

df = spark.read.parquet(f"{dataset}.parquet")
df.createOrReplaceTempView("T")
df.printSchema()
print("n_row:", df.count(), "n_col:", len(df.columns))

df_length = spark.sql("SELECT udf_length(string_0) FROM T")
df_length.write.parquet(f"output_udf_length_{ct}.parquet", mode="overwrite")
df_length.show(1)
spark.stop()
print(f"\nudf_length@{dataset}: Success\n")
