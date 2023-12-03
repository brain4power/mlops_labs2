from __future__ import annotations

import os
import json
import uuid
import logging
from typing import Any, Sequence

from airflow.exceptions import AirflowFailException
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.hooks.sql import return_single_query_results
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow_provider_kafka.operators.produce_to_topic import ProduceToTopicOperator

logger = logging.getLogger(__name__)

# get Kafka configuration information
connection_config = {
    "bootstrap.servers": os.getenv("KAFKA_HOST", "kafka"),
}


def produce_transformation(record_id, from_page):
    logger.info(f"produce_transformation record_id: {record_id}")
    yield (
        json.dumps(
            {
                "message_id": record_id,
            }
        ),
        json.dumps(
            {
                "from_page": from_page,
            }
        ),
    )


def create_record_id(**kwargs):
    ti = kwargs["ti"]
    ti.xcom_push("record_id", str(uuid.uuid4()))


class PostgresOperatorXCOM(PostgresOperator):
    def execute(self, context):
        self.log.info("Executing: %s", self.sql)
        hook = self.get_db_hook()
        if self.split_statements is not None:
            extra_kwargs = {"split_statements": self.split_statements}
        else:
            extra_kwargs = {}
        output = hook.run(
            sql=self.sql,
            autocommit=self.autocommit,
            parameters=self.parameters,
            handler=self.handler if self.do_xcom_push else None,
            return_last=self.return_last,
            **extra_kwargs,
        )
        if not self.do_xcom_push:
            return None
        if return_single_query_results(self.sql, self.return_last, self.split_statements):
            # For simplicity, we pass always list as input to _process_output, regardless if
            # single query results are going to be returned, and we return the first element
            # of the list in this case from the (always) list returned by _process_output
            return self._process_output([output], hook.descriptions, context)[-1]
        return self._process_output(output, hook.descriptions, context)

    def _process_output(self, results: list[Any], descriptions: list[Sequence[Sequence] | None], context) -> list[Any]:
        logger.info(f"get store_id results: {results}")
        min_page = results[0][0][0] if results is not None else None
        self.xcom_push(context=context, key="min_page", value=min_page)
        return results


def task_status_handler(state: str):
    if state == "ERROR":
        logger.info(f"Caught ERROR task state. Stop flow.")
        raise AirflowFailException()
    return state


def transformer_dag():
    # get min page number
    sql = """
           SELECT coalesce(min(to_page), 700001) as min_page
           FROM h_service_tasks
           WHERE state != 'ERROR';
           """
    get_min_handled_page = PostgresOperatorXCOM(
        task_id="get_min_handled_page",
        postgres_conn_id="h_db_conn",
        sql=sql,
    )
    # create init_transformation record
    create_transformation_record_id = PythonOperator(
        task_id="create_transformation_record_id", python_callable=create_record_id
    )
    create_record_sql = f"""
    INSERT INTO h_service_tasks (record_id, state, from_page) values 
    ('{{{{ti.xcom_pull(key='record_id', task_ids='create_transformation_record_id')}}}}', 'INIT', '{{{{ti.xcom_pull(key='min_page', task_ids='get_min_handled_page')}}}}' - 1);
    """  # noqa: E501
    create_transformation_task_record = PostgresOperator(
        task_id=f"create_transformation_task_record",
        postgres_conn_id="h_db_conn",
        sql=create_record_sql,
    )
    # send task to kafka
    produce_topic = os.getenv("TF_TOPIC_NAME")
    init_transformation = ProduceToTopicOperator(
        task_id=f"produce_to_{produce_topic}",
        topic=produce_topic,
        producer_function="h_dags.data_transformation.produce_transformation",
        producer_function_kwargs={
            "record_id": "{{ti.xcom_pull(key='record_id', task_ids='create_transformation_record_id')}}",
            "from_page": "{{ti.xcom_pull(key='min_page', task_ids='get_min_handled_page')}}",
        },
        kafka_config=connection_config,
    )
    # catch task done
    scrape_done_sql = f"""
    SELECT state FROM h_service_tasks 
    WHERE record_id='{{{{ti.xcom_pull(key='record_id', task_ids='create_transformation_record_id')}}}}' AND state in ('DONE', 'ERROR');
    """  # noqa: E501
    waiting_transformation_done = SqlSensor(
        task_id=f"waiting_scrape_done",
        conn_id="h_db_conn",
        sql=scrape_done_sql,
        success=task_status_handler,
        mode="reschedule",
    )
    (
        get_min_handled_page
        >> create_transformation_record_id
        >> create_transformation_task_record
        >> init_transformation
        >> waiting_transformation_done
    )
