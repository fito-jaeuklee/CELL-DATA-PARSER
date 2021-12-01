import pandas
import pytz
from datetime import datetime
from timezonefinder import TimezoneFinder

from newcell.messages.bs_message import BsMessage


LABEL_DATETIME = 'datetime'
LABEL_BS_OPERATION_TIME = 'operation_time'
LABEL_BS_HR = 'hr'

HEADER_BS = [LABEL_DATETIME, LABEL_BS_OPERATION_TIME, LABEL_BS_HR]

class BsMessageManager:

    # TODO: (if needed) bs message validation
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

        return BsMessageManager.adjust_timezone(dataframe, tz)

    @staticmethod
    def adjust_date(dataframe: pandas.DataFrame, date: datetime) -> pandas.DataFrame:
        dataframe[LABEL_DATETIME] = dataframe[LABEL_DATETIME].map(
            lambda dt: dt.replace(year=date.year, month=date.month, day=date.day),
        )

        return dataframe

    def add_message(self, payload: bytes) -> None:
        self.messages.append(BsMessage.create(payload).export_row())

    def export_dataframe(self) -> pandas.DataFrame:
        gps_message_dataframe = pandas.DataFrame(self.messages, columns=HEADER_BS)

        self.messages = []

        return gps_message_dataframe
