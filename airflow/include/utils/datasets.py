#datasets.py
from airflow.datasets import Dataset

def bronze_dataset(project_name:str, schema_key: str) -> Dataset:
    # O que importa é o URI ser consistente entre produtor/consumidor
    return Dataset(f"bq://{project_name}/bronze/{schema_key}")

def prata_dataset(project_name:str, schema_key: str) -> Dataset:
    return Dataset(f"bq://{project_name}/prata/{schema_key}")

def ouro_dataset(project_name:str, schema_key: str) -> Dataset:
    return Dataset(f"bq://{project_name}/ouro/{schema_key}")