from newcell.managers.gps_message_manager import GpsMessageManager
from newcell.managers.imu_message_manager import ImuMessageManager
from newcell.managers.bs_message_manager import BsMessageManager
from newcell.messages.start_message import StartMessage
#from newcell.messages.end_message import EndMessage


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
    def _strip_off_bytes(message: bytes):
        data = message[:CRC_IDX]
        crc = message[CRC_IDX]

        return (data, crc)

    @staticmethod
    def _crc_check(data_to_check, crc_value):
        crc = 0

        for each_byte in data_to_check:
            crc ^= each_byte

        return crc == crc_value

    @classmethod
    def parse(cls, file_contents):
        messages = file_contents.split(HEADER_DELIMITER) 

        start_msg = None

        for message in messages:
            data, crc = IntermedGenerator._strip_off_bytes(message)

            # XXX end message should be followed by "\r\n" ?? -> check!
            if data[0] != END_MSG_TYPE and not IntermedGenerator._crc_check(data, crc):
                # do error handling (logging??)
                continue

            payload = data[PAYLOAD_START_IDX:]
            message_type = data[0]

            if message_type == START_MSG_TYPE:
                start_msg = StartMessage.create(payload)
            elif message_type == END_MSG_TYPE:
                yield (
                    start_msg,
                    cls.gps_message_manager.export_dataframe(),
                    cls.imu_message_manager.export_dataframe(),
                    cls.bs_message_manager.export_dataframe(),
                )
            elif message_type in cls.managers:
                cls.managers[message_type].add_message(payload)
            else:
                print(f"ERROR: message type is { message_type }")
