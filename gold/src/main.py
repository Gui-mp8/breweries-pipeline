#gold/src/main.py
from pyspark.sql import SparkSession
from transformers.breweries_aggregation_transformer import BreweriesAggregationTransformer

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
    transformer = BreweriesAggregationTransformer()
    df_agg = transformer.transform(df)

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