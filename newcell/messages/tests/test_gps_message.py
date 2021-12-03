import pytest
from datetime import datetime

from newcell.messages.gps_message import GpsMessage


gps_message_bytes = b"\xe4\x07\x02\x13j\xdb\t\x01P\xcf\x02\x13W\x00\x00\x00(\xb3\xa7\xdb'\x01\x00\x00\xde\x13\x02\x00|\x15\xec\x13\xd6\x06\x00\x00Pr\x00\x00\x00\x00\xd1\x00\xda\x00\xaf\x00\x06\x06)A"

expected_gps_message = GpsMessage(
    date=318900196,
    time_utc=17423210,
    gps_nmea_latitude=373981106000,
    gps_nmea_longitude=1270700553000,
    height_scaled=136158,
    h_acc_scaled=5500,
    v_acc_scaled=5100,
    sog_scaled=1750,
    corse_angle_scaled=29264,
    vertical_velocity_scaled=0,
    hdop_scaled=209,
    vdop_scaled=218,
    tdop_scaled=175,
    navigation_stellites=6,
    tracked_satellites=6,
    avg_cn0_scaled=41,
    fix_mode=65,
)

class TestGpsMessage:
    def test_bytes_message_parsed_correctly(self):
        # When: GpsMessage.create is called on raw gps message
        actual_gps_message = GpsMessage.create(gps_message_bytes)

        # Then: parsed message should eqaul to expected
        assert actual_gps_message == expected_gps_message

    """
    examples:
    3724.53246,N,12641.84210,E
    lat= 37.40887433333333, lon= 126.69736833333333
    
    3724.53246,S,12641.84210,W
    lat= -37.40887433333333, lon= -126.69736833333333

    3723.28606,N,12659.30276,E
    lat= 37.388101, lon= 126.98837933333333

    3723.28606,S,12659.30276,W
    lat= -37.388101, lon= -126.98837933333333

    3723.28614,N,12659.30277,E
    lat= 37.388102333333336, lon= 126.9883795

    3723.28614,S,12659.30277,W
    lat= -37.388102333333336, lon= -126.9883795

    3723.28651,N,12659.30284,E
    lat= 37.3881085, lon= 126.98838066666667

    3723.28651,S,12659.30284,W
    lat= -37.3881085, lon= -126.98838066666667

    3723.27819,N,12659.30087,E
    lat= 37.38796983333334, lon= 126.98834783333334

    3723.27819,S,12659.30087,W
    lat= -37.38796983333334, lon= -126.98834783333334
    """
    @pytest.mark.parametrize(
        "nmea_format, decimal_format",
        [
            (372453246000, 37.40887433333333),
            (1264184210000, 126.69736833333333),
            (-372453246000, -37.40887433333333),
            (-1264184210000, -126.69736833333333),
            (372328606000, 37.388101),
            (1265930276000, 126.98837933333333),
            (-372328606000, -37.388101),
            (-1265930276000, -126.98837933333333),
            (372328614000, 37.388102333333336),
            (1265930277000, 126.9883795),
            (-372328614000, -37.388102333333336),
            (-1265930277000, -126.9883795),
            (372328651000, 37.3881085),
            (1265930284000, 126.98838066666667),
            (-372328651000, -37.3881085),
            (-1265930284000, -126.98838066666667),
            (372327819000, 37.38796983333334),
            (1265930087000, 126.98834783333334),
            (-372327819000, -37.38796983333334),
            (-1265930087000, -126.98834783333334),
        ]
    )
    def test_convert_nmea_to_decimal_degrees(self, nmea_format, decimal_format):
        # Given: nmea_format in dd.mmmmmmm * 10 ^ 7 format

        # When: call convert_nmea_to_dicimal_degreees static method on nmea_format
        actual_decimal = GpsMessage.convert_nmea_to_decimal_degrees(nmea_format)

        # Then: the result matches
        assert actual_decimal == decimal_format

    def test_latitude(self):
        # Given: parsed and created GpsMessage from raw bytes of message
        gps_message = GpsMessage.create(gps_message_bytes)
        expected_latitude = 37.663517666666664

        # When: call latitude property
        actual_latitude = gps_message.latitude

        # Then: latitude matches
        assert actual_latitude == expected_latitude

    def test_longitude(self):
        # Given: parsed and created GpsMessage from raw bytes of message
        gps_message = GpsMessage.create(gps_message_bytes)
        expected_longitude = 127.11675883333334

        # When: call longitude property
        actual_longitude = gps_message.longitude

        # Then: longitude matches
        assert actual_longitude == expected_longitude

    def test_datetime(self):
        # Given: parsed and created GpsMessage from raw bytes of message
        gps_message = GpsMessage.create(gps_message_bytes)
        expected_datetime = datetime(2020, 2, 19, 17, 42, 32, 100000)

        # When: call datetime property
        actual_datetime = gps_message.datetime

        # Then: datetime matches
        assert actual_datetime == expected_datetime

    def test_speed(self):
        # Given: parsed and created GpsMessage from raw bytes of message
        gps_message = GpsMessage.create(gps_message_bytes)
        expected_speed = 1.75

        # When: call datetime property
        actual_speed = gps_message.speed

        # Then: datetime matches
        assert actual_speed == expected_speed

    def test_export_row(self):
        # Given: parsed and created GpsMessage from raw bytes of message
        gps_message = GpsMessage.create(gps_message_bytes)
        expected_exported_row = (
            datetime(2020, 2, 19, 17, 42, 32, 100000),
            37.663517666666664,
            127.11675883333334,
            1.75,
        )

        # When: call export_row method
        actual_exported_row = gps_message.export_row()

        # Then: exported row matches
        assert actual_exported_row == expected_exported_row

    @pytest.mark.parametrize(
        "message_property, expected_value",
        [
            ("height", 136.158),
            ("h_acc", 55.0),
            ("v_acc", 51.0),
            ("corse_angle", 292.64),
            ("vertical_velocity", 0.0),
            ("hdop", 2.09),
            ("vdop", 2.18),
            ("tdop", 1.75),
            ("avg_cn0", 0.41),
        ]
    )
    def test_scaled_value(self, message_property, expected_value):
       # Given: parsed and created GpsMessage from raw bytes of message
       gps_message = GpsMessage.create(gps_message_bytes)

       # Then: each property's actual value should eqauls to expected value
       assert getattr(gps_message, message_property) == expected_value
