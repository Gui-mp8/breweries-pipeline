# gold/src/transformers/breweries_aggregation_transformer.py
from pyspark.sql import DataFrame

class BreweriesAggregationTransformer:
    """
    Transformer responsible for aggregating breweries data
    to count the total number of breweries per state and type.
    """

    def transform(self, df: DataFrame) -> DataFrame:
        return (
            df.groupBy("state_province", "brewery_type")
              .count()
              .withColumnRenamed("count", "total_breweries")
        )
