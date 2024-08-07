from pydantic import BaseModel


class OneWeekSong(BaseModel):
    popularity: int
    days_since_release: int
    day_0: int
    day_1: int
    day_2: int
    day_3: int
    day_4: int
    day_5: int
    day_6: int


class ThreeWeekSong(BaseModel):
    popularity: int
    days_since_release: int
    day_0: int
    day_1: int
    day_2: int
    day_3: int
    day_4: int
    day_5: int
    day_6: int
    day_7: int
    day_8: int
    day_9: int
    day_10: int
    day_11: int
    day_12: int
    day_13: int
    day_14: int
    day_15: int
    day_16: int
    day_17: int
    day_18: int
    day_19: int
    day_20: int
