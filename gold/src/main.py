#gold/src/main.py
from pyspark.sql import SparkSession
import logging
logging.getLogger("py4j").setLevel(logging.ERROR)

def main():

    spark = (
        SparkSession.builder
        .appName("Breweries Gold Aggregation")
        .config("spark.sql.files.ignoreCorruptFiles", "true")
        .config("spark.sql.files.ignoreMissingFiles", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    silver_path = "/app/datalake/silver"
    gold_path = "/app/datalake/gold"

    # Lê todos os parquet da Silver
    df = spark.read.parquet(f"{silver_path}/**/*.parquet")
    # Agregação
    df_agg = (
        df.groupBy("state_province", "brewery_type")
          .count()
          .withColumnRenamed("count", "total_breweries")
    )

    (
        df_agg
        .coalesce(1)
        .write.mode("overwrite")
        .option("header", "true")
        .csv(f"{gold_path}/breweries_agg")
    )

    print("✅ Gold criada com sucesso.")

    spark.stop()


if __name__ == "__main__":
    main()