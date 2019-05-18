import logging

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

from airflow.operators.python_operator import PythonOperator

from launcher.launcher import ContainerLauncher
from launcher.docker import do_test_docker

default_args = {
    'owner': 'airflow',
    'start_date': datetime.now(),
}


def read_xcoms(**context):
    data = context['task_instance'].xcom_pull(task_ids=context['task'].upstream_task_ids, key='result')
    for xcom in data:
        logging.info(f'I have received data: {xcom}')


with DAG('gnucash_pipe', default_args=default_args) as dag:

    t1 = PythonOperator(
        task_id='gnucash_extract_from_itau',
        provide_context=True,
        python_callable=ContainerLauncher('gnucash_extract_from_itau', 
                                          run_kwargs={
                                              'volumes':{'/home/pv/Dropbox/documentos/financas/controle': {'bind': '/home/pv/Dropbox/documentos/financas/controle', 'mode': 'rw'}},
                                              'network_mode':'host' #This actually enables local postgres to be found (not airflow db server)
    }
                                          ).run
    )

