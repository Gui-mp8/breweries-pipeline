import pytest
import pandas as pd
import pyarrow.parquet as pq
from pathlib import Path
import json

from transformers.jsonl_parquet_transformer import JsonlParquetTransformer

def test_jsonl_parquet_transformer(tmp_path):
    # Setup paths
    bronze_dir = tmp_path / "bronze"
    silver_dir = tmp_path / "silver"
    bronze_dir.mkdir()
    silver_dir.mkdir()
    
    # Create dummy bronze jsonl file
    dummy_data = [
        {"id": "1", "name": "Brewery X", "state_province": "Texas"},
        {"id": "2", "name": "Brewery Y", "state_province": None},  # To test fillna
        {"id": "3", "name": "Brewery Z", "state_province": "California"}
    ]
    
    file_path = bronze_dir / "breweries_20230101.jsonl"
    with open(file_path, "w") as f:
        for record in dummy_data:
            f.write(json.dumps(record) + "\n")
            
    # Execute transformation
    transformer = JsonlParquetTransformer(
        bronze_path=str(bronze_dir),
        silver_path=str(silver_dir)
    )
    transformer.transform()
    
    # Assertions
    # Expect Hive-style directories due to partitioning by location
    assert (silver_dir / "location=Texas").exists()
    assert (silver_dir / "location=unknown").exists()
    assert (silver_dir / "location=California").exists()
    
    # Read one of the parquet files to verify content
    california_files = list((silver_dir / "location=California").glob("*.parquet"))
    assert len(california_files) > 0
    
    df_ca = pd.read_parquet(california_files[0])
    assert len(df_ca) == 1
    assert df_ca.iloc[0]["name"] == "Brewery Z"
