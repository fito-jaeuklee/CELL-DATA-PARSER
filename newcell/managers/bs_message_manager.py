import pandas

from newcell.managers.message_manager import MessageManager
from newcell.messages.bs_message import BsMessage


LABEL_DATETIME = 'datetime'
LABEL_BS_OPERATION_TIME = 'operation_time'
LABEL_BS_HR = 'hr'

HEADER_BS = [LABEL_DATETIME, LABEL_BS_OPERATION_TIME, LABEL_BS_HR]

@MessageManager.register
class BsMessageManager:
    def __init__(self):
        self.messages = []

    def add_message(self, payload: bytes) -> None:
        self.messages.append(BsMessage.create(payload).export_row())

    def export_dataframe(self) -> pandas.DataFrame:
        gps_message_dataframe = pandas.DataFrame(self.messages, columns=HEADER_BS)

        self.messages = []

        return gps_message_dataframe
