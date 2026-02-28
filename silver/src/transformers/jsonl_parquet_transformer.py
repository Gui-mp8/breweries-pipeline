#silver/src/transformers/jsonl_parquet_transformer.py
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
from pathlib import Path

class JsonlParquetTransformer:
    def __init__(self, **kwargs):
        self.bronze_path = kwargs.get("bronze_path")
        self.silver_path = kwargs.get("silver_path")

    def transform(self):

        bronze_path = Path(self.bronze_path)
        silver_path = Path(self.silver_path)

        silver_path.mkdir(parents=True, exist_ok=True)

        json_files = sorted(bronze_path.glob("breweries_*.jsonl"))

        if not json_files:
            print("❌ Nenhum arquivo encontrado no bronze.")
            return

        latest_file = json_files[-1]
        print(f"📂 Lendo: {latest_file}")

        df = pd.read_json(latest_file, lines=True)

        df["location"] = df["state_province"].fillna("unknown")

        table = pa.Table.from_pandas(df)

        partitioning = ds.partitioning(
            pa.schema([("location", pa.string())]), flavor="hive"
        )

        ds.write_dataset(
            table,
            base_dir=str(silver_path),
            format="parquet",
            partitioning=partitioning,
            existing_data_behavior="overwrite_or_ignore"
        )

        print("✅ Conversão para Parquet concluída.")