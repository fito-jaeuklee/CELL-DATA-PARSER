from datetime import datetime, timedelta

import pandas
import pytz
from timezonefinder import TimezoneFinder

from newcell.managers.gps_message_manager import GpsMessageManager
from newcell.managers.message_manager import MessageManager
from newcell.messages.export_columns import GPS_EXPORT_COLUMNS

raw_gps_messages = [
    b"\xe4\x07\x02\x13j\xdb\t\x01P\xcf\x02\x13W\x00\x00\x00(\xb3\xa7\xdb'\x01\x00\x00\xde\x13\x02\x00|\x15\xec\x13\xd6\x06\x00\x00Pr\x00\x00\x00\x00\xd1\x00\xda\x00\xaf\x00\x06\x06)A",
    b"\xe4\x07\x02\x13t\xdb\t\x010\xfe\x02\x13W\x00\x00\x008]\xa7\xdb'\x01\x00\x00H\x0e\x02\x00\xcc\x10\x04\x10M\x07\x00\x00\x10l\x00\x00\x00\x00\xd1\x00\xda\x00\xaf\x00\x06\x06)A",
    b"\xe4\x07\x02\x13~\xdb\t\x01 T\x03\x13W\x00\x00\x00X\xb7\xa5\xdb'\x01\x00\x00Y\x0c\x02\x00\xac\rH\r\x08\x03\x00\x00\x10l\x00\x00\x00\x00\xd1\x00\xda\x00\xaf\x00\x06\x06)A",
    b"\xe4\x07\x02\x13\x88\xdb\t\x01@\xa2\x03\x13W\x00\x00\x00\x80\xe0\xa4\xdb'\x01\x00\x00m\x03\x02\x00T\x0bT\x0b\x05\x03\x00\x00Cq\x00\x00\x00\x00\xd1\x00\xda\x00\xaf\x00\x06\x06*A",
    b"\xe4\x07\x02\x13\x92\xdb\t\x010\xf8\x03\x13W\x00\x00\x00\xf0\xf1\xa5\xdb'\x01\x00\x00\x9a\xfb\x01\x00\xc4\t`\t\x1e\x01\x00\x00Cq\x00\x00\x00\x00\xd1\x00\xda\x00\xaf\x00\x06\x06*A",
]

expected_exported_rows = [
    (
        datetime(2020, 2, 19, 17, 42, 32, 100000),
        37.663517666666664, 127.11675883333334, 1.75, 136.158, 55.0,
        51.0, 292.64, 0.0, 2.09, 2.18, 1.75, 6, 6, 41, 65,
    ),
    (
        datetime(2020, 2, 19, 17, 42, 32, 200000),
        37.663519666666666, 127.11675516666666, 1.869, 134.728, 43.0,
        41.0, 276.64, 0.0, 2.09, 2.18, 1.75, 6, 6, 41, 65,
    ),
    (
        datetime(2020, 2, 19, 17, 42, 32, 300000),
        37.66352333333333, 127.11673716666667, 0.776, 134.233, 35.0,
        34.0, 276.64, 0.0, 2.09, 2.18, 1.75, 6, 6, 41, 65,
    ),
    (
        datetime(2020, 2, 19, 17, 42, 32, 400000),
        37.66352666666667, 127.116728, 0.773, 131.949, 29.0, 29.0,
        289.95, 0.0, 2.09, 2.18, 1.75, 6, 6, 42, 65,
    ),
    (
        datetime(2020, 2, 19, 17, 42, 32, 500000),
        37.663530333333334, 127.11673966666666, 0.286, 129.946, 25.0, 24.0,
        289.95, 0.0, 2.09, 2.18, 1.75, 6, 6, 42, 65,
    ),
]

seoul_utc_time_difference = 9

expected_time_adjusted_rows = [
    (row[0] + timedelta(hours=seoul_utc_time_difference),) + row[1:]
    for row in expected_exported_rows
]


class TestGpsMessageManager:
    def test_add_message(self):
        # Given: GpsMessageManager instance and raw gps messages
        gps_message_manager = GpsMessageManager()

        # When: GpsMessageManager.add_message is called on raw gps messages
        for raw_message in raw_gps_messages:
            gps_message_manager.add_message(raw_message)

        # Then: exported messages should eqaul as expected list of tuples
        assert [tuple(row) for row in gps_message_manager.messages] == expected_exported_rows

    def test_export_dataframe(self):
        # Given: GpsMessageManager instance and raw gps messages are added to it
        gps_message_manager = GpsMessageManager()

        for raw_message in raw_gps_messages:
            gps_message_manager.add_message(raw_message)

        expected_dataframe = pandas.DataFrame(
            expected_exported_rows, columns=GPS_EXPORT_COLUMNS
        )

        # When: GpsMessageManager.export_dataframe is called
        actual_dataframe = gps_message_manager.export_dataframe()

        # Then: exported dataframe should returned as expected
        assert actual_dataframe.equals(expected_dataframe)

        # And: messages list should empty
        assert not gps_message_manager.messages

    def test_mean_coordinate(self):
        # Given: exported dataframe
        dataframe = pandas.DataFrame(expected_exported_rows, columns=GPS_EXPORT_COLUMNS)

        expected_lat = 37.66352353333333
        expected_long = 127.11674376666664

        # When: GpsMessageManager.mean_coordinate is called
        actual_lat, actual_long = GpsMessageManager.mean_coordinate(dataframe)

        # Then: mean value of coordinates match
        assert (actual_lat, actual_long) == (expected_lat, expected_long)

    def test_adjust_timezone(self):
        expected_dataframe = pandas.DataFrame(
            expected_time_adjusted_rows, columns=GPS_EXPORT_COLUMNS
        )

        # Given: exported dataframe
        tz = pytz.timezone(
            TimezoneFinder().timezone_at(lat=37.40891712, lng=126.69735474)
        )

        dataframe = pandas.DataFrame(expected_exported_rows, columns=GPS_EXPORT_COLUMNS)

        # When: GpsMessageManager.adjust_timezone is called
        actual_dataframe = MessageManager.adjust_timezone(dataframe, tz)

        # Then: the result sould be the same with expected one
        assert actual_dataframe.equals(expected_dataframe)

    def test_adjust_timezone_by_coordinate(self):
        expected_dataframe = pandas.DataFrame(
            expected_time_adjusted_rows, columns=GPS_EXPORT_COLUMNS
        )

        # Given: exported dataframe
        dataframe = pandas.DataFrame(expected_exported_rows, columns=GPS_EXPORT_COLUMNS)

        # When: GpsMessageManager.adjust_timezone_by_coordinate is called
        actual_dataframe = MessageManager.adjust_timezone_by_coordinate(
            dataframe, lat=37.4, long=126.7
        )

        # Then: the result sould be the same with expected one
        assert actual_dataframe.equals(expected_dataframe)
