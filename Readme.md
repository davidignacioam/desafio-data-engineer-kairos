# Descripción

El presente proyecto se desarrolla como postulación al cargo de data engineer. Corresponde a un prototipo de código Airflow donde se desarrolla un pipeline que descarga datos desde una API, luego los transforma y finalmente los inserta en una tabla de Google Bigquery.

Dado que el proyecto es sólo un prototipo, se asignan variables dummy en todo lo que tiene que ver con detalles técnicos de configuración de Bigquery. Así mismo, se agregan dos DAGs en Airflow que también poseen datos dummy, ya que el fin es sólo ejemplificar la forma consecutiva de operar un aviso en relación al status general del pipeline.

# Ejecución con `python`:

1. Instalar las dependencias: `pip install -r src/requirements.txt`
2. Ejecutar el proyecto: `python src/main.py`