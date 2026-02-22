#silver/src/main.py
from transformers.jsonl_parquet_transformer import JsonlParquetTransformer

def main():
    transformer = JsonlParquetTransformer(
        bronze_path="/app/datalake/bronze",
        silver_path="/app/datalake/silver"
    )
    transformer.transform()

if __name__ == "__main__":
    main()