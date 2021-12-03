import pandas
import pytz
from datetime import datetime, timedelta
from timezonefinder import TimezoneFinder

from newcell.messages.imu_message import ImuMessage


LABEL_DATETIME = 'datetime'
LABEL_IMU_AX = 'acc_x'
LABEL_IMU_AY = 'acc_y'
LABEL_IMU_AZ = 'acc_z'
LABEL_IMU_GX = 'gyro_x'
LABEL_IMU_GY = 'gyro_y'
LABEL_IMU_GZ = 'gyro_z'
LABEL_IMU_MX = 'magnet_x'
LABEL_IMU_MY = 'magnet_y'
LABEL_IMU_MZ = 'magnet_z'

HEADER_IMU = [LABEL_DATETIME,
              LABEL_IMU_AX, LABEL_IMU_AY, LABEL_IMU_AZ,
              LABEL_IMU_GX, LABEL_IMU_GY, LABEL_IMU_GZ,
              LABEL_IMU_MX, LABEL_IMU_MY, LABEL_IMU_MZ]

class ImuMessageManager:

    # TODO: (if needed) imu message validation
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

        return ImuMessageManager.adjust_timezone(dataframe, tz)

    @staticmethod
    def adjust_date(dataframe: pandas.DataFrame, date: datetime) -> pandas.DataFrame:
        date_delta: timedelta = date.date() - dataframe.at[0, LABEL_DATETIME].date()

        dataframe[LABEL_DATETIME] = dataframe[LABEL_DATETIME].map(
            lambda dt: dt + date_delta,
        )

        return dataframe

    def add_message(self, payload: bytes) -> None:
        self.messages.append(ImuMessage.create(payload).export_row())

    def export_dataframe(self) -> pandas.DataFrame:
        gps_message_dataframe = pandas.DataFrame(self.messages, columns=HEADER_IMU)

        self.messages = []

        return gps_message_dataframe
