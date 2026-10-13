
import logging
from pendulum import datetime

from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from datetime import timedelta


logger = logging.getLogger(__name__)

RUTA_DBT = "/opt/airflow/dags/data/dbt_project"
DBT_COMMAND = "set -euo pipefail"

default_args = {
    'retries': 2,
    'retry_delay': timedelta(seconds=30),
}


@dag(
    dag_id="04_dag_gold",
    schedule=None,
    start_date=datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    default_args=default_args,
    tags=["gold", "dbt", "transformacion"],
)
def dag_gold():

    dbt_run_crypto = BashOperator(
        task_id="dbt_run_crypto",
        bash_command=(
            f"{DBT_COMMAND}\n"
            f"cd {RUTA_DBT}\n"
            "dbt run --select gold_crypto --profiles-dir ."
        ),
    )

    dbt_test_crypto = BashOperator(
        task_id="dbt_test_crypto",
        bash_command=(
            f"{DBT_COMMAND}\n"
            f"cd {RUTA_DBT}\n"
            "dbt test --select gold_crypto --profiles-dir ."
        ),
    )

    dbt_run_github = BashOperator(
        task_id="dbt_run_github",
        bash_command=(
            f"{DBT_COMMAND}\n"
            f"cd {RUTA_DBT}\n"
            "dbt run --select gold_github --profiles-dir ."
        ),
    )

    dbt_test_github = BashOperator(
        task_id="dbt_test_github",
        bash_command=(
            f"{DBT_COMMAND}\n"
            f"cd {RUTA_DBT}\n"
            "dbt test --select gold_github --profiles-dir ."
        ),
    )

    cadena_crypto = dbt_run_crypto >> dbt_test_crypto
    cadena_github = dbt_run_github >> dbt_test_github

    [cadena_crypto, cadena_github]
    


dag_gold()
