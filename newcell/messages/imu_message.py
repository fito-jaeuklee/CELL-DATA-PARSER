import struct
from datetime import datetime
from typing import NamedTuple


class ImuMessage(NamedTuple):
    time_utc: int
    accel_x: int
    accel_y: int
    accel_z: int
    gyro_x: int
    gyro_y: int
    gyro_z: int
    mag_x: int
    mag_y: int
    mag_z: int

    imu_message_struct_first = struct.Struct(">ihhhhhh")
    imu_message_struct_second = struct.Struct("<hhh")
    part_boundary_index = -6

    @classmethod
    def create(cls, payload: bytes):
        # length validation??
        return cls._make(
            cls.imu_message_struct_first.unpack(payload[:cls.part_boundary_index]) +
            cls.imu_message_struct_second.unpack(payload[cls.part_boundary_index:]),
        )

    def export_row(self):
        return (self.datetime, *self.accel, *self.gyro, *self.magnet)

    @property
    def accel(self):
        return (self.accel_x, self.accel_y, self.accel_z)

    @property
    def gyro(self):
        return (self.gyro_x, self.gyro_y, self.gyro_z)

    @property
    def magnet(self):
        return (self.mag_x, self.mag_y, self.mag_z)

    @property
    def datetime(self) -> datetime:
        utc_milisec = self.time_utc & 0x000000FF
        utc_second = (self.time_utc >> 8) & 0x000000FF
        utc_minute = (self.time_utc >> 16) & 0x000000FF
        utc_hour = (self.time_utc >> 24) & 0x000000FF

        time_string = f"{utc_hour:02d}{utc_minute:02d}{utc_second:02d}.{utc_milisec:02d}"

        datetime_string = f"{time_string}"

        return datetime.strptime(datetime_string, "%H%M%S.%f")
