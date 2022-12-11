from typing import List, Dict, Any, Optional, Union
from pydantic import BaseModel, validator
import pandas as pd


class Channel(BaseModel):
    Name: str
    Id: int
    Value: float
    Valid: bool
    units: str
    AlertState: int
    WindDirection: Union[float, None]
    WindSpeed: Union[float, None]


class Sample(BaseModel):
    datetime: str
    SerialCode: int
    channels: List[Channel]

    @validator("channels", always=True)
    def channels_to_df(cls, v, values, **kwargs) -> pd.DataFrame:
        return pd.DataFrame([channel.dict() for channel in v])
