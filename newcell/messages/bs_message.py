import struct
from datetime import datetime
from typing import NamedTuple

from newcell.messages.export_columns import BS_EXPORT_COLUMNS

BATTERY_SCALING = 1e2
CELL_TEMPERATURE_SCALING = 1e2


class BsMessage(NamedTuple):
    date: int
    time_utc: int
    operation_time: int
    hr: int
    battery_scaled: int
    cell_temperature_scaled: int
    reserve_1: int
    reserve_2: int
    reserve_3: int

    bs_message_struct = struct.Struct("<IiIHHHHHH")

    @classmethod
    def create(cls, payload):
        return cls._make(cls.bs_message_struct.unpack(payload))

    def export_row(self):
        return (getattr(self, column) for column in BS_EXPORT_COLUMNS)

    @property
    def battery(self) -> float:
        return self.battery_scaled / BATTERY_SCALING

    @property
    def cell_temperature(self) -> float:
        return self.cell_temperature_scaled / CELL_TEMPERATURE_SCALING

    @property
    def datetime(self) -> datetime:
        year = self.date & 0x0000FFFF
        month = (self.date >> 16) & 0x00FF
        day = (self.date >> 24) & 0x00FF

        time_value = self.time_utc / 1e2

        date_string = f"{day:02d}{month:02d}{year}"
        datetime_string = f"{date_string} {time_value:.02f}"

        return datetime.strptime(datetime_string, "%d%m%Y %H%M%S.%f")
