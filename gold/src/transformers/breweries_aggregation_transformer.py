# gold/src/transformers/breweries_aggregation_transformer.py
from pyspark.sql import DataFrame

class BreweriesAggregationTransformer:

    def transform(self, df: DataFrame) -> DataFrame:
        return (
            df.groupBy("state", "brewery_type")
              .count()
              .withColumnRenamed("count", "total_breweries")
        )
