from datetime import timedelta
import pendulum
from pathlib import Path
import os

from airflow.decorators import dag
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

from include.utils.datasets import bronze_dataset

PROJECT_ROOT = Path(os.environ["PROJECT_ROOT"])
DATALAKE_PATH = str((PROJECT_ROOT / "datalake").resolve())

default_args = {
    "owner": "Guilherme Machado Pires",
    "start_date": pendulum.datetime(2026, 2, 21, tz="America/Sao_Paulo"),
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "email": ["seu-email@dominio.com"],
    "email_on_failure": True
}

@dag(
    dag_id="breweries_bronze_all_breweries",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["BREWERY", "BRONZE"],
)
def breweries():

    DockerOperator(
        task_id="run_breweries_extraction",
        image="breweries-bronze",
        api_version="auto",
        auto_remove="force",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        mount_tmp_dir=False,
        mounts=[
            Mount(
                source=DATALAKE_PATH,
                target="/app/datalake",
                type="bind",
            )
        ],
        outlets=bronze_dataset(project_name="breweries", schema_key="all_breweries")
    )

breweries()
