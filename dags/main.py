from airflow import DAG
import pendulum
from datetime import datetime, timedelta
from api.video_stats import get_playlist_id, get_video_ids, extract_video_data, save_to_json
from datawarehouse.dwh import core_tables, staging_table
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from dataquality.soda import yt_elt_data_quality
local_tz = pendulum.timezone("Europe/Malta")

staging_schema = "staging"
core_schema = "core"
default_args = {
    "owner": "data engineer",
    "depends_on_past": False,
    "email_on_failure": False,
    "email": "data@engineer.com",
    "start_date": datetime(2026, 9, 22, tzinfo=local_tz)
}

with DAG(
    dag_id="produce_json",
    default_args=default_args,
    description="DAG to produce json file with raw data",
    schedule='0 14 * * *',
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=1)
) as dag_produce:
    playlist_id = get_playlist_id()
    videos_id = get_video_ids(playlist_id)
    extract_data = extract_video_data(videos_id)
    save_to_json_task = save_to_json(extract_data)

    trigger_update_db = TriggerDagRunOperator(
        task_id="trigger_update_db",
        trigger_dag_id="update_db",
    )

    playlist_id >> videos_id >> extract_data >> save_to_json_task >> trigger_update_db


with DAG(
    dag_id="update_db",
    default_args=default_args,
    description="update database",
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=1)
) as dag_update:
    update_staging = staging_table()
    update_core = core_tables()

    trigger_data_quality = TriggerDagRunOperator(
        task_id="trigger_data_quality",
        trigger_dag_id="data_quality_check",
    )
    update_staging >> update_core >> trigger_data_quality

with DAG(
    dag_id="data_quality_check",
    default_args=default_args,
    description="Check data quality elt pipeline",
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=1)
) as dag_quality:
    soda_validate_staging = yt_elt_data_quality(staging_schema)
    soda_validate_core = yt_elt_data_quality(core_schema)

    soda_validate_staging >> soda_validate_core