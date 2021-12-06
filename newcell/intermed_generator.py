from typing import Tuple

import pandas as pd

from newcell.managers.bs_message_manager import BsMessageManager
from newcell.managers.gps_message_manager import GpsMessageManager
from newcell.managers.imu_message_manager import ImuMessageManager
from newcell.managers.message_manager import MessageManager
from newcell.messages.end_message import EndMessage
from newcell.messages.start_message import StartMessage

HEADER_DELIMITER = b"COACH"
GPS_MSG_TYPE = 0x01
IMU_MSG_TYPE = 0x02
BS_MSG_TYPE = 0x03
START_MSG_TYPE = 0x04
END_MSG_TYPE = 0x05

MSG_TYPE_OFFSET = len(HEADER_DELIMITER)
MSG_LENGTH_OFFSET = MSG_TYPE_OFFSET + 1
MSG_PAYLOAD_OFFSET = MSG_LENGTH_OFFSET + 1


class IntermedGenerator:
    gps_message_manager = GpsMessageManager()
    imu_message_manager = ImuMessageManager()
    bs_message_manager = BsMessageManager()

    managers = {
        GPS_MSG_TYPE: gps_message_manager,
        IMU_MSG_TYPE: imu_message_manager,
        BS_MSG_TYPE: bs_message_manager,
    }

    @staticmethod
    def _crc_check(data_to_check, crc_value):
        crc = 0

        for each_byte in data_to_check:
            crc ^= each_byte

        return crc == crc_value

    @staticmethod
    def find_next_message(buffer: memoryview, cursor):
        msg_first_index = buffer.obj.index(HEADER_DELIMITER, cursor)

        msg_type_index = msg_first_index + MSG_TYPE_OFFSET
        msg_length_index = msg_first_index + MSG_LENGTH_OFFSET
        msg_payload_index = msg_first_index + MSG_PAYLOAD_OFFSET

        msg_length = buffer[msg_length_index]
        msg_crc_index = msg_payload_index + msg_length

        msg_type = buffer[msg_type_index]
        crc_data = buffer[msg_type_index:msg_crc_index]
        payload = buffer[msg_payload_index:msg_crc_index]
        crc = buffer[msg_crc_index]

        return msg_crc_index, (msg_type, crc_data, payload, crc)

    @staticmethod
    def raw_parser(file_contents: memoryview):
        cursor = 0

        try:
            while True:
                cursor, message = IntermedGenerator.find_next_message(
                    file_contents, cursor
                )

                yield message
        except Exception:
            return

    @classmethod
    def prepare_dataframes_to_yield(cls) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        gps_dataframe = cls.gps_message_manager.export_dataframe()
        imu_dataframe = cls.imu_message_manager.export_dataframe()
        bs_dataframe = cls.bs_message_manager.export_dataframe()

        if not (gps_dataframe.empty or imu_dataframe.empty):
            first_datetime = gps_dataframe.at[0, "datetime"]

            imu_dataframe = MessageManager.adjust_date(
                imu_dataframe,
                first_datetime,
            )

        return (
            gps_dataframe,
            imu_dataframe,
            bs_dataframe,
        )

    @classmethod
    def parse(cls, file_contents):
        mv_contents = memoryview(file_contents)

        start_msg = None

        for (message_type, crc_data, payload, crc) in IntermedGenerator.raw_parser(
            mv_contents
        ):
            if not IntermedGenerator._crc_check(crc_data, crc):
                # do error handling (logging??)
                continue

            if message_type == START_MSG_TYPE:
                start_msg = StartMessage.create(payload)
            elif message_type == END_MSG_TYPE:
                end_message = EndMessage.create(payload)
                yield (
                    start_msg,
                    *cls.prepare_dataframes_to_yield(),
                    end_message,
                )
            elif message_type in cls.managers:
                cls.managers[message_type].add_message(payload)
            else:
                print(f"ERROR: message type is { message_type }")
