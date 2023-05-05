import pandas as pd
from pandas import DataFrame
import logging
from datetime import datetime
MSG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s (Line: %(lineno)d)"
DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
logging.basicConfig(filename='execution.log', format=MSG_FORMAT, datefmt=DATETIME_FORMAT,
                    encoding='utf-8', level=logging.INFO)
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def read_file(path: str) -> DataFrame:
    """Función que lee un archivo CSV

    Args:
        path (str): Ruta de lectura del archivo
    Returns:
        DataFrame: Data frame con la info del CSV
    """
    try:
        logger.info(f'leyendo archivo en la ruta {path}')
        pandas_df = pd.read_csv(path)
        return pandas_df
    except Exception as error:
        logger.error(f'no se pudo leer el path: {path}, {error}')


def clean_columns(df: DataFrame, columns: list) -> DataFrame:
    """Esta función cambia los nombres de las columnas

    Args:
        df (DataFrame): Dataframe con información
        columns (list): columnas nuevas

    Returns:
        DataFrame: dtaa frame con nuevas columnas
    """
    try:
        logger.info(f'renombrando columnas en el df con {"-".join(columns)}')
        df.columns = columns
        return df
    except Exception as error:
        logger.error(f'No se pudo actualizar las columnas {error}')

    return df


def transform_data(df: DataFrame) -> DataFrame:
    """Agrega columna de fecha de proceso en un archivo maestro

    Args:
        df (DataFrame): datos a tratar

    Returns:
        DataFrame: nuevo dataframe con columna de fecha
    """
    fecha_proceso = datetime.today()
    logger.info(
        f'Creando la columna fecha de proceso con la fecha {fecha_proceso}')
    df['fecha_proceso'] = fecha_proceso
    return df


def run():
    columns = ['fipstxt', 'state', 'county_name',
               'persistent_poverty', 'persistent_related_child', 'metro_nonmetro_status']
    path = './2015_persistent_povert.csv'
    df = read_file(path)
    df = clean_columns(df, columns)
    df = transform_data(df)
    print(df.dtypes)


run()
