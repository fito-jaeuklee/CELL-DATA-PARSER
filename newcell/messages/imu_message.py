import struct
from datetime import datetime
from typing import NamedTuple

from newcell.messages.export_columns import IMU_EXPORT_COLUMNS


class ImuMessage(NamedTuple):
    time_utc: int
    accel_x: int
    accel_y: int
    accel_z: int
    gyro_x: int
    gyro_y: int
    gyro_z: int
    magnet_x: int
    magnet_y: int
    magnet_z: int

    time_utc_struct = struct.Struct("<i")
    accel_gyro_struct = struct.Struct(">hhhhhh")
    magnet_struct = struct.Struct("<hhh")

    utc_end_index = time_utc_struct.size
    magnet_start_index = -magnet_struct.size

    @classmethod
    def create(cls, payload: bytes):
        return cls._make(
            cls.time_utc_struct.unpack(payload[: cls.utc_end_index])
            + cls.accel_gyro_struct.unpack(
                payload[cls.utc_end_index : cls.magnet_start_index]
            )
            + cls.magnet_struct.unpack(payload[cls.magnet_start_index :])
        )

    def export_row(self):
        return (getattr(self, column) for column in IMU_EXPORT_COLUMNS)

    @property
    def accel(self):
        return (self.accel_x, self.accel_y, self.accel_z)

    @property
    def gyro(self):
        return (self.gyro_x, self.gyro_y, self.gyro_z)

    @property
    def magnet(self):
        return (self.magnet_x, self.magnet_y, self.magnet_z)

    @property
    def datetime(self) -> datetime:
        utc_time_value = self.time_utc / 1e2

        datetime_string = f"{utc_time_value:.02f}"

        return datetime.strptime(datetime_string, "%H%M%S.%f")
