
import pytz

import pandas as pd

from datetime import datetime, timedelta, date


def get_offset(date_time) -> float:
    return -(
            AS_TZ.localize(date_time) - GMT_TZ.localize(date_time)
        ).seconds /60 /60


SERVICE: str = 'data-modelos-autos'

AS_TZ: pytz = pytz.timezone("America/Santiago")
GMT_TZ: pytz = pytz.timezone("GMT")

START_TIME_SERVER = pd.to_datetime("2024-06-03 01:00:00")
START_TIME = START_TIME_SERVER + pd.Timedelta(get_offset(START_TIME_SERVER), unit='H')

DEFAULT_AIRFLOW_ARGS = {
    "owner": "airflow",
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "admin@localhost.com",
    "retries": 2,
    "retry_delay": timedelta(minutes=5)
}

URL_API = 'https://k51qryqov3.execute-api.ap-southeast-2.amazonaws.com/prod/makes/ckl2phsabijs71623vk0?modelsPage=1'

TABLE_DISPOSITION = "WRITE_TRUNCATE"
TABLE_ID = "car-models.dataset.table_name"

MODELS_SCHEMA = {
    "id": {"type": "string"},
    "name": {"type": "string"},
    "image": {"type": "string"},
    "make": {"type": "string"},
    "makeId": {"type": "string"},
    "makeImage": {"type": "string"},
    "votes": {"type": "integer"},
    "rank": {"type": "integer"},
    "engineVol": {"type": "float"},
    "comments": {"type": "array"},
    "totalComments": {"type": "integer"}     
}
