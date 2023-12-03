import pendulum
from h_dags.data_transformation import transformer_dag
from airflow.decorators import dag


transformer_dag = dag(
    dag_id=f"data_transformation",
    start_date=pendulum.datetime(2023, 12, 3, tz="UTC"),
    schedule_interval="0 3 * * *",
    catchup=False,
    tags=["transformation"],
    is_paused_upon_creation=True,
    max_active_runs=1,
)(transformer_dag)
transformer_dag()
