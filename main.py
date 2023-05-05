import pandas as pd
from pandas import DataFrame
import logging

logging.basicConfig(filename='execution.log',
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
        logger.info(f'renombrando columnas en el df con "-".join({columns})')
        df.columns = columns
        return df
    except Exception as error:
        logger.error(f'No se pudo actualizar las columnas {error}')

    return df


"fipstxt", "state", "County Name", "Persistent Poverty  0=Not persistent poverty 1=Persistent pover", "Persistent Related Child Poverty 0=Not persistent child poverty", "Metro-nonmetro status, 2013 0=Nonmetro 1=Metro"


def run():
    columns = ['fipstxt', 'state', 'county_name',
               'persistent_poverty', 'persistent_related_child', 'metro_nonmetro_status']
    path = './2015_persistent_povert.csv'
    df = read_file(path)
    df = clean_columns(df, columns)
    print(df.dtypes)


run()
