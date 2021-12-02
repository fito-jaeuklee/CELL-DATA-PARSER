import struct
from datetime import datetime
from typing import NamedTuple


COORDINATE_SCALING = 1e7
MINUTE_SCALING = 1e5
SECONDS_PER_MINUTE = 60

class GpsMessage(NamedTuple):
    date: int
    time_utc: int
    gps_nmea_latitude: int
    gps_nmea_longitude: int
    height: int
    h_acc: int
    v_acc: int
    speed_of_ground: int
    corse_angle: int
    vertical_velocity: int
    hdop: int
    vdop: int
    tdop: int
    navigation_stellites: int
    tracked_satellites: int
    avg_cn0: int
    fix_mode: int

    gps_message_struct = struct.Struct("<IiiiIHHihIHHHBBBB")

    @classmethod
    def create(cls, payload: bytes):
        # length validation??
        return cls._make(cls.gps_message_struct.unpack(payload))

    def export_row(self):
        return (self.datetime, self.latitude, self.longitude, self.speed)

    @staticmethod
    def convert_nmea_to_decimal_degrees(nmea_value) -> float:
        sign = 1 if nmea_value > 0 else -1

        degrees, minutes = divmod(abs(nmea_value), COORDINATE_SCALING)
        converted = degrees + minutes / (MINUTE_SCALING * SECONDS_PER_MINUTE)

        return sign * converted

    @property
    def latitude(self) -> float:
        return self.convert_nmea_to_decimal_degrees(self.gps_nmea_latitude)

    @property
    def longitude(self) -> float:
        return self.convert_nmea_to_decimal_degrees(self.gps_nmea_longitude)

    @property
    def speed(self) -> float:
        return self.speed_of_ground

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
