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


def run():
    path = './2015_persistent_povert.csv'
    df = read_file(path)
    print(df.columns)


run()
