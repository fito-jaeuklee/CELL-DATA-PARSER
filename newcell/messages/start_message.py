import struct
from typing import NamedTuple


class StartMessage(NamedTuple):
    cell_serial_number: int
    product_id: bytes
    version_id: bytes
    product_version: bytes
    firmware_major_version: int
    firmware_minor_version: int
    gnss_fix_time: int
    used_nand_block_size: int
    used_nand_page_size: int
    ble_id: bytes
    imu_cal_accx: float
    imu_cal_accy: float
    imu_cal_accz: float
    imu_cal_gyrx: float
    imu_cal_gyry: float
    imu_cal_gyrz: float
    imu_cal_magx: float
    imu_cal_magy: float
    imu_cal_magz: float

    start_message_struct = struct.Struct("<H2s2s2sBBHHH24sfffffffff")

    @classmethod
    def create(cls, payload):
        return cls._make(cls.start_message_struct.unpack(payload))

    @property
    def device_model(self):
        decoded_product_id = self.product_id.decode("utf-8")[::-1]
        decoded_version_id = self.version_id.decode("utf-8")[::-1]
        return f"{decoded_product_id}{decoded_version_id}"

    @property
    def device_version(self):
        return self.product_version.decode("utf-8")[::-1]

    @property
    def device_number(self):
        # XXX: ??
        return self.cell_serial_number

    @property
    def firmware_version(self):
        return f"v{self.firmware_major_version}.{self.firmware_minor_version}"

    @property
    def ble_hr_id(self):
        return self.ble_id.decode("utf-8").strip("\x00")

    @property
    def imu_accel_calibration(self):
        return (self.imu_cal_accx, self.imu_cal_accy, self.imu_cal_accz)

    @property
    def imu_gyro_calibration(self):
        return (self.imu_cal_gyrx, self.imu_cal_gyry, self.imu_cal_gyrz)

    @property
    def imu_magnet_calibration(self):
        return (self.imu_cal_magx, self.imu_cal_magy, self.imu_cal_magz)
