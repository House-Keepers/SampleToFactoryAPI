from pydantic import BaseModel


class CoupledFactory(BaseModel):
    name: str
    crossed_pollutants: str
    num_pollutants: int
    angle_diff: float
