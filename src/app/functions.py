

import logging
import warnings
import urllib3
import requests
import json

import pandas as pd

from jsonschema import Draft202012Validator
from google.cloud import bigquery

from app.config import (
    SERVICE, 
    URL_API, 
    TABLE_DISPOSITION,
    TABLE_ID,
    MODELS_SCHEMA
)


warnings.filterwarnings('ignore')

urllib3.disable_warnings()


logger = logging.getLogger(SERVICE)

draft_202012_validator = Draft202012Validator(MODELS_SCHEMA)

bigquery_client = bigquery.Client()



def get_slack_message() -> str:
    return "Datos de Modelos de Autos Actualizados Correctamente"

def get_json_df(json_data: json) -> pd.DataFrame :
    return pd.read_json(json.dumps(json_data), orient='index').T

def clean_comments(comment: str) -> str :
    return comment.replace('\n',' ').replace('  ','').replace('_','')

def get_df_models() -> pd.DataFrame :
    '''
    Summary:
        Get DataFrame from API JSON Models
    Returns:
        df_models (pd.DataFrame):
            DataFrame with Models Information
    '''
    try:
        # Getting JSON from API
        api_request = requests.get(URL_API)
        api_json = api_request.json()

        # Getting DataFrame from JSON
        df_api = get_json_df(api_json)
        df_api_models = get_json_df(df_api.loc[0,'models'])
        models_json = df_api_models.loc[0,'models']

        # Validating JSON usign previously defined JSON Schema
        if not draft_202012_validator.is_valid(models_json) :
            logger.error('Error en la validación del JSON de la API')
            raise ValueError('Error en la validación del JSON de la API')

        # Cleaning Comments and Creating final Models DataFrame
        df_models = pd.DataFrame(models_json)
        df_models['comments'] = pd.Series([[
            clean_comments(comment) for comment in df_models.loc[row,'comments']] for row in range(len(df_models))
        ])
        logger.info(f"Dataframe de Modelos de Autos creado correctamente")
        return df_models
    
    except Exception as e:
        logger.error(f"Error creando Dataframe de Modelos de Autos: {str(e)}")
        return pd.DataFrame()


def insert_data_to_bigquery() -> None :
    """
    Summary:
        Insert Data to BigQuery
    """
    try:

        # Downloading Cars Model Data and defining BigQuery Client and Jobs
        # Finally, excecuting Bigquery Job
        df_models = get_df_models() 
        job_config = bigquery.LoadJobConfig(write_disposition = TABLE_DISPOSITION)
        job = bigquery_client.load_table_from_dataframe(df_models, TABLE_ID, job_config = job_config) 
        logger.info(f"Datos insertados correctamente en BigQuery: {job.result()}")

    except Exception as e:
        logger.error(f"Error insertando datos en BigQuery: {str(e)}")
        return None
