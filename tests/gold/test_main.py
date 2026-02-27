import pytest
from pyspark.sql import SparkSession
import os

from transformers.breweries_aggregation_transformer import BreweriesAggregationTransformer

@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .appName("Breweries Gold Aggregation - Test")
        .master("local[1]")
        .getOrCreate()
    )

def test_transform_data(spark):
    
    # Mock some data as if it just came from Silver parquet
    data = [
        ("1", "Texas", "micro"),
        ("2", "Texas", "micro"),
        ("3", "California", "large"),
        ("4", "California", "micro")
    ]
    df = spark.createDataFrame(data, ["id", "location", "brewery_type"])
    
    # Perform transformation
    transformer = BreweriesAggregationTransformer()
    df_agg = transformer.transform(df)
    
    # Collect results
    results = df_agg.collect()
    
    # Verify aggregation
    # California should have 1 large and 1 micro
    # Texas should have 2 micro
    assert len(results) == 3
    
    ca_large = [r for r in results if r["location"] == "California" and r["brewery_type"] == "large"][0]
    assert ca_large["total_breweries"] == 1
    
    ca_micro = [r for r in results if r["location"] == "California" and r["brewery_type"] == "micro"][0]
    assert ca_micro["total_breweries"] == 1
    
    tx_micro = [r for r in results if r["location"] == "Texas" and r["brewery_type"] == "micro"][0]
    assert tx_micro["total_breweries"] == 2
