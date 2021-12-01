import pandas
import pytz
from typing import Tuple
from timezonefinder import TimezoneFinder

from newcell.messages.gps_message import GpsMessage

LABEL_DATETIME = "datetime"
LABEL_LATITUDE = "latitude"
LABEL_LONGITUDE = "longitude"
LABEL_SPEED = "speed"

HEADER_OCH = [LABEL_DATETIME, LABEL_LATITUDE, LABEL_LONGITUDE, LABEL_SPEED]

class GpsMessageManager:

    # TODO: (if needed) gps message validation
    # length / value validities

    def __init__(self):
        self.messages = []

    @staticmethod
    def adjust_timezone(dataframe: pandas.DataFrame, tz) -> pandas.DataFrame:
        start_at = dataframe.at[0, LABEL_DATETIME]
        dataframe[LABEL_DATETIME] += tz.utcoffset(start_at)

        return dataframe

    @staticmethod
    def adjust_timezone_by_coordinate(dataframe: pandas.DataFrame, lat: float, long: float) -> pandas.DataFrame:
        tz = pytz.timezone(TimezoneFinder().timezone_at(lat=lat, lng=long))

        return GpsMessageManager.adjust_timezone(dataframe, tz)

    @staticmethod
    def mean_coordinate(dataframe: pandas.DataFrame) -> Tuple[float, float]:
        return dataframe[[LABEL_LATITUDE, LABEL_LONGITUDE]].mean()

    def add_message(self, payload: bytes) -> None:
        self.messages.append(GpsMessage.create(payload).export_row())

    def export_dataframe(self) -> pandas.DataFrame:
        gps_message_dataframe = pandas.DataFrame(self.messages, columns=HEADER_OCH)

        self.messages = []

        return gps_message_dataframe
