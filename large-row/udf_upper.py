from pyspark.sql import SparkSession
import datetime
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("-d", type=str, help="test data")
dataset = parser.parse_args().d

ct = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")

spark = SparkSession.builder.appName("LargeRowBenchmark-UDFUpper").getOrCreate()


# UDF
def udf_upper(text):
    return text.upper()


spark.udf.register("udf_upper", udf_upper)

df = spark.read.parquet(f"{dataset}.parquet")
df.createOrReplaceTempView("T")
df.printSchema()
print("n_row:", df.count(), "n_col:", len(df.columns))

df_upper = spark.sql("SELECT udf_upper(string_0) FROM T")
df_upper.write.parquet(f"output_udf_upper_{ct}.parquet", mode="overwrite")
df_upper.show(1)
spark.stop()
print(f"\nudf_upper@{dataset}: Success\n")
