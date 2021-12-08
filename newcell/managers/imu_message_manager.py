import pandas as pd

from newcell.managers.message_manager import MessageManager
from newcell.messages.export_columns import IMU_EXPORT_COLUMNS
from newcell.messages.imu_message import ImuMessage


@MessageManager.register
class ImuMessageManager:
    def __init__(self):
        self.messages = []

    def add_message(self, payload: bytes) -> None:
        self.messages.append(ImuMessage.create(payload).export_row())

    def export_dataframe(self) -> pd.DataFrame:
        gps_message_dataframe = pd.DataFrame(self.messages, columns=IMU_EXPORT_COLUMNS)

        self.messages = []

        return gps_message_dataframe
