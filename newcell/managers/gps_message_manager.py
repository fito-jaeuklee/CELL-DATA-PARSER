from typing import Tuple

import pandas as pd

from newcell.managers.message_manager import MessageManager
from newcell.messages.export_columns import GPS_EXPORT_COLUMNS
from newcell.messages.gps_message import GpsMessage

LABEL_LATITUDE = "latitude"
LABEL_LONGITUDE = "longitude"


@MessageManager.register
class GpsMessageManager:
    def __init__(self):
        self.messages = []

    @staticmethod
    def mean_coordinate(dataframe: pd.DataFrame) -> Tuple[float, float]:
        return dataframe[[LABEL_LATITUDE, LABEL_LONGITUDE]].mean()

    def add_message(self, payload: bytes) -> None:
        self.messages.append(GpsMessage.create(payload).export_row())

    def export_dataframe(self) -> pd.DataFrame:
        gps_message_dataframe = pd.DataFrame(self.messages, columns=GPS_EXPORT_COLUMNS)

        self.messages = []

        return gps_message_dataframe
