import pandas as pd

from newcell.managers.message_manager import MessageManager
from newcell.messages.bs_message import BsMessage
from newcell.messages.export_columns import BS_EXPORT_COLUMNS


@MessageManager.register
class BsMessageManager:
    def __init__(self):
        self.messages = []

    def add_message(self, payload: bytes) -> None:
        self.messages.append(BsMessage.create(payload).export_row())

    def export_dataframe(self) -> pd.DataFrame:
        gps_message_dataframe = pd.DataFrame(self.messages, columns=BS_EXPORT_COLUMNS)

        self.messages = []

        return gps_message_dataframe
