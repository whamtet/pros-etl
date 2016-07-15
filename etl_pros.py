from airflow import DAG
from airflow import configuration
from airflow.operators import BashOperator, PythonOperator, HiveOperator
from airflow.operators import SqoopOperator
from airflow.hooks import JdbcHook, HiveCliHook
from datetime import datetime, timedelta
from imp import load_source
from os.path import realpath,dirname

# Boilerplate
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.combine(datetime.today() - timedelta(1), datetime.min.time()),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

dag = DAG(
        'etl_pros',
        default_args=default_args,
        schedule_interval='0 23 * * *',
        max_active_runs=1)

aml_utils = load_source(
    'aml_utils',
    "{pf}/asiamiles_airflow_extensions/utils.py".format(
        pf=configuration.get('core', 'plugins_folder')))

mod_config = aml_utils.load_config(
    "{dag_folder}/pros_etl.cfg".format(
        dag_folder=dirname(realpath(__file__))))

hdfs_home=mod_config['hadoop']['hdfs_home']

copy_rsynced_files_to_hadoop = BashOperator(
    task_id="copy_rsynced_files_to_hadoop",
    bash_command="hadoop fs -put -f /data1/staging/pros/* pros",
    dag=dag)

#spark-shell --master yarn-client

update_seat_idx = BashOperator(
  task_id="update_seat_idx",
  bash_command="cat /data1/airflow/dags/pros-etl/pros_seat_index_hist_load.scala | spark-shell --master yarn-client",
  dag=dag)

update_curve = BashOperator(
  task_id="update_curve",
  bash_command="cat /data1/airflow/dags/pros-etl/pros_bid_price_hist_load.scala | spark-shell --master yarn-client",
  dag=dag)

update_seat_idx.set_upstream(copy_rsynced_files_to_hadoop)
update_curve.set_upstream(copy_rsynced_files_to_hadoop)
