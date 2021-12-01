import pandas
import pytz
from datetime import datetime
from timezonefinder import TimezoneFinder

from newcell.managers.bs_message_manager import (
    BsMessageManager,
    HEADER_BS,
)


raw_bs_messages = [
    b'\xe4\x07\x04\x0b\x00\x08\x1f\n\xa9\x08\x00\x00\x00\x00\x90\x01\x00\x00\x00\x00\x00\x00\x00\x00',
    b'\xe4\x07\x04\x0b\x00\x08 \n\xaa\x08\x00\x00\x00\x00\x90\x01\x00\x00\x00\x00\x00\x00\x00\x00',
    b'\xe4\x07\x04\x0b\x00\x08!\n\xab\x08\x00\x00\x00\x00\x90\x01\x00\x00\x00\x00\x00\x00\x00\x00',
    b'\xe4\x07\x04\x0b\x00\x08"\n\xac\x08\x00\x00\x00\x00\x90\x01\x00\x00\x00\x00\x00\x00\x00\x00',
    b'\xe4\x07\x04\x0b\x00\x08#\n\xad\x08\x00\x00\x00\x00\x90\x01\x00\x00\x00\x00\x00\x00\x00\x00',
]

expected_exported_rows = [
    (datetime(2020, 4, 11, 0, 8, 31, 100000), 2217, 0),
    (datetime(2020, 4, 11, 0, 8, 32, 100000), 2218, 0),
    (datetime(2020, 4, 11, 0, 8, 33, 100000), 2219, 0),
    (datetime(2020, 4, 11, 0, 8, 34, 100000), 2220, 0),
    (datetime(2020, 4, 11, 0, 8, 35, 100000), 2221, 0),
]

seoul_utc_time_difference = 9

expected_time_adjusted_rows = [
    (datetime(2020, 4, 11, seoul_utc_time_difference, 8, 31, 100000), 2217, 0),
    (datetime(2020, 4, 11, seoul_utc_time_difference, 8, 32, 100000), 2218, 0),
    (datetime(2020, 4, 11, seoul_utc_time_difference, 8, 33, 100000), 2219, 0),
    (datetime(2020, 4, 11, seoul_utc_time_difference, 8, 34, 100000), 2220, 0),
    (datetime(2020, 4, 11, seoul_utc_time_difference, 8, 35, 100000), 2221, 0),
]

class TestBsMessageManager:
    def test_add_message(self):
        # Given: BsMessageManager instance and raw bs messages
        bs_message_manager = BsMessageManager()

        # When: BsMessageManager.add_message is called on raw bs messages
        for raw_message in raw_bs_messages:
            bs_message_manager.add_message(raw_message)

        # Then: exported messages should eqaul as expected list of tuples
        assert bs_message_manager.messages == expected_exported_rows

    def test_export_dataframe(self):
        # Given: BsMessageManager instance and raw bs messages are added to it
        bs_message_manager = BsMessageManager()

        for raw_message in raw_bs_messages:
            bs_message_manager.add_message(raw_message)

        expected_dataframe = pandas.DataFrame(expected_exported_rows, columns=HEADER_BS)

        # When: BsMessageManager.export_dataframe is called
        actual_dataframe = bs_message_manager.export_dataframe()

        # Then: exported dataframe should returned as expected
        assert actual_dataframe.equals(expected_dataframe)

        # And: messages list should empty
        assert not bs_message_manager.messages

    def test_adjust_timezone(self):
        expected_dataframe = pandas.DataFrame(expected_time_adjusted_rows, columns=HEADER_BS)

        # Given: exported dataframe (date adjusted)
        tz = pytz.timezone(TimezoneFinder().timezone_at(lat=37.40891712, lng=126.69735474))

        dataframe = pandas.DataFrame(expected_exported_rows, columns=HEADER_BS)

        # When: BsMessageManager.adjust_timezone is called
        actual_dataframe = BsMessageManager.adjust_timezone(dataframe, tz)

        # Then: the result sould be the same with expected one
        assert actual_dataframe.equals(expected_dataframe)

    def test_adjust_timezone_by_coordinate(self):
        expected_dataframe = pandas.DataFrame(expected_time_adjusted_rows, columns=HEADER_BS)

        # Given: exported dataframe (date adjusted)
        dataframe = pandas.DataFrame(expected_exported_rows, columns=HEADER_BS)

        # When: BsMessageManager.adjust_timezone_by_coordinate is called
        actual_dataframe = BsMessageManager.adjust_timezone_by_coordinate(dataframe, lat=37.4, long=126.7)

        # Then: the result sould be the same with expected one
        assert actual_dataframe.equals(expected_dataframe)
