from datetime import timedelta

import airflow
from airflow import DAG
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor

DEFAULT_ARGS = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False
}

SPARK_TEST_STEPS = [
    {
        'Name': 'crunch_data',
        'Type': 'Spark',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                's3://waliboxbigdatatest-redshift/test_folder',
                'data_parsing_in_pyspark.py',
                '10'
            ]
        }
    }
]

JOB_FLOW_OVERRIDES = {
    'Name': 'Parse_for_Redshift'                                #Done - this will be the name of the cluster
}

with DAG(
    dag_id='build_out_emr_cluster',
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(hours=2),
    schedule_interval='0 3 * * *'
) as dag:

    cluster_creator = EmrCreateJobFlowOperator(
        task_id='create_job_flow',                  # Done
        job_flow_overrides=JOB_FLOW_OVERRIDES,      # Done
        aws_conn_id='aws_default',                  # Done
        emr_conn_id='emr_default'                   # Done
    )

    step_adder = EmrAddStepsOperator(
        task_id='add_steps',                                                                                # Done
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_job_flow', key='return_value') }}",        # Done
        aws_conn_id='aws_default',                                                                          # Done
        steps=SPARK_TEST_STEPS  
    )

    step_checker = EmrStepSensor(
        task_id='watch_step',
        job_flow_id="{{ task_instance.xcom_pull('create_job_flow', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')[0] }}",
        aws_conn_id='aws_default'
    )

    cluster_remover = EmrTerminateJobFlowOperator(
        task_id='remove_cluster',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_job_flow', key='return_value') }}",
        aws_conn_id='aws_default'
    )

    cluster_creator >> step_adder >> step_checker >> cluster_remover