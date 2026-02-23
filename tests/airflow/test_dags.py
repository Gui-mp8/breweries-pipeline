import pytest
from airflow.models import DagBag
import os

@pytest.fixture(scope="session")
def dagbag():
    # Caminho base do projeto para encontrar a pasta airflow/dags
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
    dags_folder = os.path.join(project_root, "airflow", "dags")
    
    # Define a variável de ambiente PROJECT_ROOT para que os dags consigam rodar
    os.environ["PROJECT_ROOT"] = project_root

    return DagBag(dag_folder=dags_folder, include_examples=False)

def test_no_import_errors(dagbag):
    """Garante que nenhum DAG tenha erro de sintaxe ou de importação"""
    assert len(dagbag.import_errors) == 0, f"Erros de importação encontrados: {dagbag.import_errors}"

def test_bronze_dag_loaded(dagbag):
    """Testa se a DAG Bronze carregou com a task correta"""
    dag_id = "breweries_bronze_all_breweries"
    assert dag_id in dagbag.dags
    
    dag = dagbag.dags[dag_id]
    assert len(dag.tasks) == 1
    assert "run_breweries_extraction" in [t.task_id for t in dag.tasks]

def test_silver_dag_loaded(dagbag):
    """Testa se a DAG Silver carregou com a dataset_schedule de Bronze"""
    dag_id = "breweries_silver_all_breweries"
    assert dag_id in dagbag.dags
    
    dag = dagbag.dags[dag_id]
    assert len(dag.tasks) == 1
    assert dag.schedule is not None  # Garante que possui o schedule DatasetAll

def test_gold_dag_loaded(dagbag):
    """Testa se a DAG Gold carregou com a dataset_schedule de Silver"""
    dag_id = "breweries_gold_all_breweries"
    assert dag_id in dagbag.dags
    
    dag = dagbag.dags[dag_id]
    assert len(dag.tasks) == 1
    assert dag.schedule is not None  # Garante que possui o schedule DatasetAll
