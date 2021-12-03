from newcell.managers.gps_message_manager import GpsMessageManager
from newcell.managers.imu_message_manager import ImuMessageManager
from newcell.managers.bs_message_manager import BsMessageManager
from newcell.messages.start_message import StartMessage
from newcell.messages.end_message import EndMessage


HEADER_DELIMITER = b"COACH"
GPS_MSG_TYPE = 0x01
IMU_MSG_TYPE = 0x02
BS_MSG_TYPE = 0x03
START_MSG_TYPE = 0x04
END_MSG_TYPE = 0x05

PAYLOAD_START_IDX = 2
CRC_IDX = -3

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
    def raw_parser(file_contents: memoryview):
        MSG_TYPE_OFFSET = len(HEADER_DELIMITER)
        MSG_LENGTH_OFFSET = MSG_TYPE_OFFSET + 1
        MSG_PAYLOAD_OFFSET = MSG_LENGTH_OFFSET + 1

        cursor = 0

        try:
            while True:
                msg_first_index = file_contents.obj.index(HEADER_DELIMITER, cursor)

                msg_type_index = msg_first_index + MSG_TYPE_OFFSET
                msg_length_index = msg_first_index + MSG_LENGTH_OFFSET
                msg_payload_index = msg_first_index + MSG_PAYLOAD_OFFSET

                msg_length = file_contents[msg_length_index]
                msg_crc_index = msg_payload_index + msg_length

                msg_type = file_contents[msg_type_index]
                crc_data = file_contents[msg_type_index:msg_crc_index]
                payload = file_contents[msg_payload_index:msg_crc_index]
                crc = file_contents[msg_crc_index]

                yield (msg_type, crc_data, payload, crc)

                cursor = msg_crc_index
        except Exception:
            return

    @classmethod
    def parse(cls, file_contents):
        mv_contents = memoryview(file_contents)

        start_msg = None

        for (message_type, crc_data, payload, crc) in IntermedGenerator.raw_parser(mv_contents):
            if not IntermedGenerator._crc_check(crc_data, crc):
                # do error handling (logging??)
                continue

            if message_type == START_MSG_TYPE:
                start_msg = StartMessage.create(payload)
            elif message_type == END_MSG_TYPE:
                end_message = EndMessage.create(payload)
                yield (
                    start_msg,
                    cls.gps_message_manager.export_dataframe(),
                    cls.imu_message_manager.export_dataframe(),
                    cls.bs_message_manager.export_dataframe(),
                    end_message,
                )
            elif message_type in cls.managers:
                cls.managers[message_type].add_message(payload)
            else:
                print(f"ERROR: message type is { message_type }")
