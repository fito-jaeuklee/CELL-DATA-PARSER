import pandas
import pytz
from datetime import datetime
from timezonefinder import TimezoneFinder

from newcell.managers.gps_message_manager import (
    GpsMessageManager,
    HEADER_OCH,
)


raw_gps_messages = [
    b'\xe4\x07\x04\x0b\x00\x08"P~03\x16@\xebYKOj\x00\x00\x19\x00\'\x00\x08\x00\x00\x00\x00\x00\xfe\xff\xff\xff\xc1\x008\x01\x04\x01\x05\x00\x00A',
    b'\xe4\x07\x04\x0b\x00\x08"Z~03\x16@\xebYK@j\x00\x00\x18\x00\'\x009\x00\x00\x00\x00\x00\x03\x00\x00\x00\xc1\x008\x01\x04\x01\x05\x00\x00A',
    b'\xe4\x07\x04\x0b\x00\x08#\x00\x7f03\x16@\xebYKAj\x00\x00\x18\x00&\x007\x00\x00\x00\x00\x00\x00\x00\x00\x00\xc1\x008\x01\x04\x01\x05\x00\x00A',
    b'\xe4\x07\x04\x0b\x00\x08#\n\x7f03\x16A\xebYK6j\x00\x00\x18\x00&\x00+\x00\x00\x00\x00\x00\x02\x00\x00\x00\xc1\x008\x01\x04\x01\x05\x00\x00A',
    b'\xe4\x07\x04\x0b\x00\x08#\x14\x7f03\x16A\xebYK+j\x00\x00\x18\x00&\x002\x00\x00\x00\x00\x00\xfe\xff\xff\xff\xc1\x008\x01\x04\x01\x05\x00\x00A',
]

expected_exported_rows = [
    (datetime(2020, 4, 11, 0, 8, 34, 800000), 37.408917, 126.6973547, 8),
    (datetime(2020, 4, 11, 0, 8, 34, 900000), 37.408917, 126.6973547, 57),
    (datetime(2020, 4, 11, 0, 8, 35), 37.4089172, 126.6973547, 55),
    (datetime(2020, 4, 11, 0, 8, 35, 100000), 37.4089172, 126.6973548, 43),
    (datetime(2020, 4, 11, 0, 8, 35, 200000), 37.4089172, 126.6973548, 50),
]

seoul_utc_time_difference = 9

expected_time_adjusted_rows = [
    (datetime(2020, 4, 11, seoul_utc_time_difference, 8, 34, 800000), 37.408917, 126.6973547, 8),
    (datetime(2020, 4, 11, seoul_utc_time_difference, 8, 34, 900000), 37.408917, 126.6973547, 57),
    (datetime(2020, 4, 11, seoul_utc_time_difference, 8, 35), 37.4089172, 126.6973547, 55),
    (datetime(2020, 4, 11, seoul_utc_time_difference, 8, 35, 100000), 37.4089172, 126.6973548, 43),
    (datetime(2020, 4, 11, seoul_utc_time_difference, 8, 35, 200000), 37.4089172, 126.6973548, 50),
]

class TestGpsMessageManager:
    def test_add_message(self):
        # Given: GpsMessageManager instance and raw gps messages
        gps_message_manager = GpsMessageManager()

        # When: GpsMessageManager.add_message is called on raw gps messages
        for raw_message in raw_gps_messages:
            gps_message_manager.add_message(raw_message)

        # Then: exported messages should eqaul as expected list of tuples
        assert gps_message_manager.messages == expected_exported_rows

    def test_export_dataframe(self):
        # Given: GpsMessageManager instance and raw gps messages are added to it
        gps_message_manager = GpsMessageManager()

        for raw_message in raw_gps_messages:
            gps_message_manager.add_message(raw_message)

        expected_dataframe = pandas.DataFrame(expected_exported_rows, columns=HEADER_OCH)

        # When: GpsMessageManager.export_dataframe is called
        actual_dataframe = gps_message_manager.export_dataframe()

        # Then: exported dataframe should returned as expected
        assert actual_dataframe.equals(expected_dataframe)

        # And: messages list should empty
        assert not gps_message_manager.messages

    def test_mean_coordinate(self):
        # Given: exported dataframe
        dataframe = pandas.DataFrame(expected_exported_rows, columns=HEADER_OCH)

        expected_lat = 37.40891712
        expected_long = 126.69735474

        # When: GpsMessageManager.mean_coordinate is called
        actual_lat, actual_long = GpsMessageManager.mean_coordinate(dataframe)

        # Then: mean value of coordinates match
        assert (actual_lat, actual_long) == (expected_lat, expected_long)

    def test_adjust_timezone(self):
        expected_dataframe = pandas.DataFrame(expected_time_adjusted_rows, columns=HEADER_OCH)

        # Given: exported dataframe
        tz = pytz.timezone(TimezoneFinder().timezone_at(lat=37.40891712, lng=126.69735474))

        dataframe = pandas.DataFrame(expected_exported_rows, columns=HEADER_OCH)

        # When: GpsMessageManager.adjust_timezone is called
        actual_dataframe = GpsMessageManager.adjust_timezone(dataframe, tz)

        # Then: the result sould be the same with expected one
        assert actual_dataframe.equals(expected_dataframe)

    def test_adjust_timezone_by_coordinate(self):
        expected_dataframe = pandas.DataFrame(expected_time_adjusted_rows, columns=HEADER_OCH)

        # Given: exported dataframe
        dataframe = pandas.DataFrame(expected_exported_rows, columns=HEADER_OCH)

        # When: GpsMessageManager.adjust_timezone_by_coordinate is called
        actual_dataframe = GpsMessageManager.adjust_timezone_by_coordinate(dataframe, lat=37.4, long=126.7)

        # Then: the result sould be the same with expected one
        assert actual_dataframe.equals(expected_dataframe)
