from pydantic import BaseModel


class Song(BaseModel):
    popularity: int
    days_since_release: int
    day_0: int
    day_1: int
    day_2: int
    day_3: int
    day_4: int
    day_5: int
    day_6: int

