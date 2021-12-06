import struct
from datetime import datetime
from typing import NamedTuple

from newcell.messages.export_columns import GPS_EXPORT_COLUMNS

COORDINATE_SCALING = 1e10
MINUTE_SCALING = 1e8
SECONDS_PER_MINUTE = 60

HEIGHT_SCALING = 1e3
H_ACC_SCALING = 1e2
V_ACC_SCALING = 1e2
SPEED_OF_GROUND_SCALING = 1e3
CORSE_ANGLE_SCALING = 1e2
V_VEL_SCALING = 1e3
HDOP_SCALING = 1e2
VDOP_SCALING = 1e2
TDOP_SCALING = 1e2
AVG_CN0_SCALING = 1e2


class GpsMessage(NamedTuple):
    date: int
    time_utc: int
    gps_nmea_latitude: int
    gps_nmea_longitude: int
    height_scaled: int
    h_acc_scaled: int
    v_acc_scaled: int
    sog_scaled: int
    corse_angle_scaled: int
    vertical_velocity_scaled: int
    hdop_scaled: int
    vdop_scaled: int
    tdop_scaled: int
    navigation_stellites: int
    tracked_satellites: int
    avg_cn0_scaled: int
    pos_mode: int

    gps_message_struct = struct.Struct("<IiqqIHHihiHHHBBBB")

    @classmethod
    def create(cls, payload: bytes):
        return cls._make(cls.gps_message_struct.unpack(payload))

    def export_row(self):
        return (getattr(self, column) for column in GPS_EXPORT_COLUMNS)

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
        return self.sog_scaled / SPEED_OF_GROUND_SCALING

    @property
    def height(self) -> float:
        return self.height_scaled / HEIGHT_SCALING

    @property
    def h_acc(self) -> float:
        return self.h_acc_scaled / H_ACC_SCALING

    @property
    def v_acc(self) -> float:
        return self.v_acc_scaled / V_ACC_SCALING

    @property
    def corse_angle(self) -> float:
        return self.corse_angle_scaled / CORSE_ANGLE_SCALING

    @property
    def vertical_velocity(self) -> float:
        return self.vertical_velocity_scaled / V_VEL_SCALING

    @property
    def hdop(self) -> float:
        return self.hdop_scaled / HDOP_SCALING

    @property
    def vdop(self) -> float:
        return self.vdop_scaled / VDOP_SCALING

    @property
    def tdop(self) -> float:
        return self.tdop_scaled / TDOP_SCALING

    @property
    def avg_cn0(self) -> float:
        return self.avg_cn0_scaled / AVG_CN0_SCALING

    @property
    def datetime(self) -> datetime:
        year = self.date & 0x0000FFFF
        month = (self.date >> 16) & 0x00FF
        day = (self.date >> 24) & 0x00FF

        time_value = self.time_utc / 1e2

        date_string = f"{day:02d}{month:02d}{year}"
        datetime_string = f"{date_string} {time_value:.02f}"

        return datetime.strptime(datetime_string, "%d%m%Y %H%M%S.%f")
