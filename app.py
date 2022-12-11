import uvicorn
from fastapi import FastAPI, HTTPException
from cProfile import Profile
from coupled_factory import CoupledFactory
from sample import Sample
from typing import Union
from utils import *

p = Profile()
app = FastAPI()


@app.get("/")
def is_alive():
    return "I'm Alive!"


@app.post("/get_factory", response_model=List[List[CoupledFactory]])
def main(samples: List[Sample]) -> List[List[CoupledFactory]]:
    output_list = []
    for sample in samples:
        output_list.append(process_sample(sample))
    return output_list


def process_sample(sample: Sample) -> List[CoupledFactory]:
    close_sensors = get_close_sensors_data()
    factories = get_factories_data()
    sensor_data = close_sensors[close_sensors['serialCode'] == sample.SerialCode]
    factories_data = factories[factories['name'].isin(sensor_data['factory_name'])]
    sample_wind_dir = sample.channels['WindDirection'].iloc[0]
    if sample_wind_dir is None or sensor_data.empty:
        return []
    factories_in_wind = factory_in_wind(sensor_data.iloc[0], factories_data, sample_wind_dir)
    factories_in_materials = check_materials(sample.channels, factories_data)
    if not factories_in_materials.empty and not factories_in_wind.empty:
        pollution_sources = factories_in_wind.merge(factories_in_materials, on='name')[['name', 'crossed_pollutants', 'num_pollutants', 'angle_diff']].drop_duplicates()
        pollution_sources_per_factory = []
        for _, df in pollution_sources.groupby(by='name'):
            pollution_sources_per_factory.extend(df.to_dict("records"))
        return pollution_sources_per_factory
    else:
        return []


if __name__ == '__main__':

    uvicorn.run("app:app", host="0.0.0.0", port=7070)