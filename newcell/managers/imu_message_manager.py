import pandas

from newcell.managers.message_manager import MessageManager
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

@MessageManager.register
class ImuMessageManager:
    def __init__(self):
        self.messages = []

    def add_message(self, payload: bytes) -> None:
        self.messages.append(ImuMessage.create(payload).export_row())

    def export_dataframe(self) -> pandas.DataFrame:
        gps_message_dataframe = pandas.DataFrame(self.messages, columns=HEADER_IMU)

        self.messages = []

        return gps_message_dataframe
