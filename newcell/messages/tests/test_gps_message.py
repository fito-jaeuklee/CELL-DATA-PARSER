import pytest
from datetime import datetime

from newcell.messages.gps_message import GpsMessage


gps_message_bytes = b'\xe4\x07\x04\x0b\x00\x08\x1a\x1e~/3\x16\x92\xebYKUf\x00\x002\x00D\x00\xab\x06\x00\x00\x00\x00|\xfe\xff\xff\xc1\x008\x01\x04\x01\x05\x00\x00A'

expected_gps_message = GpsMessage(
    date=184813540,
    time_utc=505022464,
    gps_nmea_latitude=372453246,
    gps_nmea_longitude=1264184210,
    height=26197,
    h_acc=50,
    v_acc=68,
    speed_of_ground=1707,
    corse_angle=0,
    vertical_velocity=4294966908,
    hdop=193,
    vdop=312,
    tdop=260,
    navigation_stellites=5,
    tracked_satellites=0,
    avg_cn0=0,
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
            (372453246, 37.4088743),
            (1264184210, 126.6973683),
            (-372453246, -37.4088743),
            (-1264184210, -126.6973683),
            (372328606, 37.388101),
            (1265930276, 126.9883793),
            (-372328606, -37.388101),
            (-1265930276, -126.9883793),
            (372328614, 37.3881023),
            (1265930277, 126.9883795),
            (-372328614, -37.3881023),
            (-1265930277, -126.9883795),
            (372328651, 37.3881085),
            (1265930284, 126.9883807),
            (-372328651, -37.3881085),
            (-1265930284, -126.9883807),
            (372327819, 37.3879698),
            (1265930087, 126.9883478),
            (-372327819, -37.3879698),
            (-1265930087,-126.9883478),
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
        expected_latitude = 37.4088743

        # When: call latitude property
        actual_latitude = gps_message.latitude

        # Then: latitude matches
        assert actual_latitude == expected_latitude

    def test_longitude(self):
        # Given: parsed and created GpsMessage from raw bytes of message
        gps_message = GpsMessage.create(gps_message_bytes)
        expected_longitude = 126.6973683

        # When: call longitude property
        actual_longitude = gps_message.longitude

        # Then: longitude matches
        assert actual_longitude == expected_longitude

    def test_datetime(self):
        # Given: parsed and created GpsMessage from raw bytes of message
        gps_message = GpsMessage.create(gps_message_bytes)
        expected_datetime = datetime(2020, 4, 11, 0, 8, 26, 300000)

        # When: call datetime property
        actual_datetime = gps_message.datetime

        # Then: datetime matches
        assert actual_datetime == expected_datetime

    def test_speed(self):
        # Given: parsed and created GpsMessage from raw bytes of message
        gps_message = GpsMessage.create(gps_message_bytes)
        expected_speed = 1707

        # When: call datetime property
        actual_speed = gps_message.speed

        # Then: datetime matches
        assert actual_speed == expected_speed

    def test_export_row(self):
        # Given: parsed and created GpsMessage from raw bytes of message
        gps_message = GpsMessage.create(gps_message_bytes)
        expected_exported_row = (datetime(2020, 4, 11, 0, 8, 26, 300000), 37.4088743, 126.6973683, 1707)

        # When: call export_row method
        actual_exported_row = gps_message.export_row()

        # Then: exported row matches
        assert actual_exported_row == expected_exported_row
