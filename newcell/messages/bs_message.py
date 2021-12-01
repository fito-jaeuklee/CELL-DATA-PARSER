import struct
from datetime import datetime
from typing import NamedTuple


class BsMessage(NamedTuple):
    date: int
    time_utc: int
    operation_time: int
    hr: int
    battery: int
    cell_temperature: int
    reserve_1: int
    reserve_2: int
    reserve_3: int

    bs_message_struct = struct.Struct("<IiIHHHHHH")

    @classmethod
    def create(cls, payload):
        return cls._make(cls.bs_message_struct.unpack(payload))

    def export_row(self):
        return (self.datetime, self.operation_time, self.hr)

    @property
    def datetime(self) -> datetime:
        year = self.date & 0x0000FFFF
        month = (self.date >> 16) & 0x00FF
        day = (self.date >> 24) & 0x00FF

        utc_hour = self.time_utc & 0x000000FF
        utc_minute = (self.time_utc >> 8) & 0x000000FF
        utc_second = (self.time_utc >> 16) & 0x000000FF
        utc_milisec = (self.time_utc >> 24) & 0x000000FF

        date_string = f"{day:02d}{month:02d}{year}"
        time_string = f"{utc_hour:02d}{utc_minute:02d}{utc_second:02d}.{utc_milisec:02d}"

        datetime_string = f"{date_string} {time_string}"

        return datetime.strptime(datetime_string, "%d%m%Y %H%M%S.%f")
