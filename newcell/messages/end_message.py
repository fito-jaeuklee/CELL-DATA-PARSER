import struct
from typing import NamedTuple


class EndMessage(NamedTuple):
    power_off: bytes
    battery_voltage: int
    operation_time: int
    used_nand_block_size: int
    used_nand_page_size: int

    end_message_struct = struct.Struct("<1sHHHH")

    @classmethod
    def create(cls, payload):
        return cls._make(cls.end_message_struct.unpack(payload))

    @property
    def power_off_indication(self) -> str:
        return self.power_off.decode('utf-8')
