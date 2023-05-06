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
    df['year'] = fecha_proceso.year
    df['month'] = fecha_proceso.month
    df['day'] = fecha_proceso.day
    return df


def resultand_dataframe(path: str, columns: list) -> DataFrame:
    df = read_file(path)
    df = clean_columns(
        df, columns)
    df = transform_data(df)
    return df


def run():
    persistent_povert_columns = ['fipstxt', 'state', 'county_name',
                                 'persistent_poverty', 'persistent_related_child', 'metro_nonmetro_status']
    county_columns = [
        "fipstxt", "state", "county_name", "metro_nonmetro_status", "economic_types", "economic_type_label", "farming_2015_update", "Mining_2015-Update", "manufacturing_2015_update", "government_2015_update", "recreation_2015_update", "nonspecialized_2015_update", "low_education_2015_update", "low_employment_cnty_2008_2012_25_64", "pop_loss_2010", "retirement_dest_2015_update", "persistent_poverty_2013", "persistent_related_child_poverty_2013"
    ]
    county_path = './county_typology_codes.csv'
    persistent_povert_path = './2015_persistent_povert.csv'
    persistent_povert_df = resultand_dataframe(
        path=persistent_povert_path, columns=persistent_povert_columns)
    county_df = resultand_dataframe(path=county_path, columns=county_columns)

    df_merge = persistent_povert_df.merge(
        county_df, how='inner', left_on='county_name', right_on='county_name')

    df_merge.to_parquet('./merge_paritions', compression='snappy',
                        partition_cols=['year_y', 'month_y', 'day_y'])

    print(df_merge.columns)


run()
