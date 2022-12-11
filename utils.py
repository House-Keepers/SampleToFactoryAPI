import math
from typing import List

from functools import lru_cache
import pandas as pd
import pyodbc

CONN_STRING = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=house-keepers.database.windows.net;DATABASE=Keepers_hb;UID=house-keeper;PWD=Password1'
MAX_WIND_DEVIATION = 45


def get_sensor_samples(sensor_id: str) -> List[pd.DataFrame]:
    """
    query the DB for all the sensor's samples between QUERYTIME and now
    """
    query = f"""SELECT * FROM keepers_hb.dbo.alerts WHERE StationId = {sensor_id} AND Timestamp > DATEADD(day,-{QUERYTIME}, GETDATE()) AND WindDirection is not null"""
    with pyodbc.connect(CONN_STRING + ';CHARSET=UTF16') as conn:
        result = pd.read_sql(query, conn)
    grouped_alerts = result.groupby(by="Pk")
    samples_df = [group for _, group in grouped_alerts]
    return samples_df


def check_materials(pollution_data: pd.DataFrame, factories_data: pd.DataFrame) -> pd.DataFrame:
    """
    check if the material emitted from the factories is the same material the sensor alerted
    :param pollution_data: data from sensor during a pollution event
    :param factories_data: the factory we check for sources
    :return: factories that have the same materials
    """
    pollution_data = break_channel_id(pollution_data)
    pollution_materials = pollution_data.loc[pollution_data[pollution_data['AlertState'] > 0].index, ['BrokenChannel', 'pollutant_symbol', 'Id']]
    if pollution_materials.empty:
        return pollution_materials
    factories_pollutants = factories_data[['name', 'pollutant_name']].drop_duplicates()
    factories_pollutants['crossed_pollutants'] = None
    factories_pollutants['num_pollutants'] = None
    for factory_name, factory_pollutants in factories_pollutants.groupby(by='name'):
        crossed_pollutant = cross_check_materials(factory_pollutants, pollution_materials)
        if not crossed_pollutant.empty:
            crossed_pollutant_str = ','.join(crossed_pollutant.apply(str))
            factories_pollutants.loc[
                factories_pollutants['name'] == factory_name, 'crossed_pollutants'] = crossed_pollutant_str
            factories_pollutants.loc[factories_pollutants['name'] == factory_name, 'num_pollutants'] = len(
                crossed_pollutant)
    return factories_pollutants.dropna(subset="crossed_pollutants")


def cross_check_materials(factory_pollutants: pd.DataFrame, pollution_materials: pd.DataFrame) -> pd.Series:
    """
    check if the factory pollutants are the same as the alert cause materials
    :param factory_pollutants: the names of the pollutants emitted from the factory
    :param pollution_materials: the materials the sensor alerted about
    """
    factory_pollutants['pollutant_name'] = factory_pollutants['pollutant_name'].apply(
        lambda x: x.strip().replace("  ", " "))
    crossed_materials = pollution_materials['BrokenChannel'].apply(
        lambda x: any([fa_pol in x for fa_pol in factory_pollutants['pollutant_name']]))
    return pollution_materials.loc[crossed_materials, 'Id']


def break_channel_id(pollution_data: pd.DataFrame) -> pd.DataFrame:
    """
    break the encoded values of column ChannelId
    :param pollution_data: data from sensor during a pollution event
    :return: pollution_data with BrokenChannel column
    """
    pollution_data.reset_index(drop=True, inplace=True)
    channels_data = get_channels_data()
    merged_data = pollution_data.merge(channels_data, how="left", left_on='Id', right_on='Id')
    pollution_data['BrokenChannel'] = merged_data['Description']
    pollution_data['pollutant_symbol'] = merged_data['Name_x']
    return pollution_data


@lru_cache()
def get_channels_data() -> pd.DataFrame:
    query = 'SELECT * FROM keepers_hb.dbo.channels'
    with pyodbc.connect(CONN_STRING) as conn:
        channels_data = pd.read_sql(query, conn)
    return channels_data


def factory_in_wind(sensor: pd.Series, factories_data: pd.Series, wind_dir: pd.Series) -> pd.DataFrame:
    """
    check if the angle between the factories and the sensor is within MAX_WIND_DEVIATION
    :param sensor: the sensor we check
    :param factories_data: data about all the factories that are close to the sensor
    :param wind_dir: wind direction of sample
    :return: data about all the factories in acceptable wind direction
    """
    wind_dir = (wind_dir - 180) % 360
    factories_data['angle_diff'] = None
    factories_coords = factories_data.drop_duplicates(subset=["x", "y", "name"])
    for _, factory_coords in factories_coords.iterrows():
        wind_area = wind_dir - MAX_WIND_DEVIATION, wind_dir + MAX_WIND_DEVIATION
        delta_x = sensor['longitude'] - factory_coords['y']
        delta_y = sensor['latitude'] - factory_coords['x']
        angle = math.atan(delta_y / delta_x) * 180 / math.pi
        angle %= 360
        print("Sensor To Factory Angle:", angle, "\n", "Reciprocal Wind Direction:", wind_dir,"\n--------------------")
        if wind_area[0] < angle < wind_area[1]:
            angle_diff = angle - wind_dir
            angle_diff = (angle_diff + 180) % 360 if abs(angle_diff) > 180 else angle_diff
            factories_data.loc[factories_data["name"] == factory_coords['name'], "angle_diff"] = angle_diff
    return factories_data.dropna(subset="angle_diff")


@lru_cache()
def get_close_sensors_data() -> pd.DataFrame:
    # query = 'SELECT * FROM keepers_hb.dbo.station_by_factories'
    # with pyodbc.connect(CONN_STRING) as conn:
    #     sensors_data = pd.read_sql(query, conn)
    # return sensors_data
    return pd.read_csv("sensors_close_to_factories.csv")


@lru_cache()
def get_factories_data() -> pd.DataFrame:
    # query = 'SELECT * FROM keepers_hb.dbo.factories'
    # with pyodbc.connect(CONN_STRING) as conn:
    #     factories_data = pd.read_sql(query, conn)
    # return factories_data
    return pd.read_csv("factory_data.csv")


# fill cache
get_channels_data()
get_factories_data()
get_close_sensors_data()
