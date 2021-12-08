import abc
from datetime import datetime, timedelta

import pandas as pd
import pytz
from timezonefinder import TimezoneFinder

LABEL_DATETIME = "datetime"


class MessageManager(abc.ABC):

    # TODO: (if needed) imu message validation
    # length / value validities

    @abc.abstractmethod
    def add_message(self, payload: bytes) -> None:
        pass

    @abc.abstractmethod
    def export_dataframe(self) -> pd.DataFrame:
        pass

    @staticmethod
    def adjust_timezone(dataframe: pd.DataFrame, tz) -> pd.DataFrame:
        start_at = dataframe.at[0, LABEL_DATETIME]
        dataframe[LABEL_DATETIME] += tz.utcoffset(start_at)

        return dataframe

    @staticmethod
    def adjust_timezone_by_coordinate(
        dataframe: pd.DataFrame, lat: float, long: float
    ) -> pd.DataFrame:
        tz = pytz.timezone(TimezoneFinder().timezone_at(lat=lat, lng=long))

        return MessageManager.adjust_timezone(dataframe, tz)

    @staticmethod
    def adjust_date(dataframe: pd.DataFrame, date: datetime) -> pd.DataFrame:
        date_delta: timedelta = date.date() - dataframe.at[0, LABEL_DATETIME].date()

        dataframe[LABEL_DATETIME] = dataframe[LABEL_DATETIME].map(
            lambda dt: dt + date_delta,
        )

        return dataframe
