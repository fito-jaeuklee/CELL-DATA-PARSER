import pandas
from typing import Tuple

from newcell.managers.message_manager import MessageManager
from newcell.messages.gps_message import GpsMessage


LABEL_DATETIME = "datetime"
LABEL_LATITUDE = "latitude"
LABEL_LONGITUDE = "longitude"
LABEL_SPEED = "speed"

HEADER_OCH = [LABEL_DATETIME, LABEL_LATITUDE, LABEL_LONGITUDE, LABEL_SPEED]

@MessageManager.register
class GpsMessageManager:
    def __init__(self):
        self.messages = []

    @staticmethod
    def mean_coordinate(dataframe: pandas.DataFrame) -> Tuple[float, float]:
        return dataframe[[LABEL_LATITUDE, LABEL_LONGITUDE]].mean()

    def add_message(self, payload: bytes) -> None:
        self.messages.append(GpsMessage.create(payload).export_row())

    def export_dataframe(self) -> pandas.DataFrame:
        gps_message_dataframe = pandas.DataFrame(self.messages, columns=HEADER_OCH)

        self.messages = []

        return gps_message_dataframe
