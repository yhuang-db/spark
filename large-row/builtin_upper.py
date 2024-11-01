from pyspark.sql import SparkSession
import datetime
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("-d", type=str, help="test data")
dataset = parser.parse_args().d

ct = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")

spark = SparkSession.builder.appName("LargeRowBenchmark-BuiltinUpper").getOrCreate()

df = spark.read.parquet(f"{dataset}.parquet")
df.createOrReplaceTempView("T")
df.printSchema()
print("n_row:", df.count(), "n_col:", len(df.columns))

df_upper = spark.sql("SELECT upper(string_0) FROM T")
df_upper.write.parquet(f"output_blt_upper_{ct}.parquet", mode="overwrite")
df_upper.show(1)
spark.stop()
print(f"\nbuiltin_upper@{dataset}: Success\n")
