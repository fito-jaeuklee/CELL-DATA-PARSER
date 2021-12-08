from datetime import datetime, timedelta

import pandas as pd
import pytz
from timezonefinder import TimezoneFinder

from newcell.managers.bs_message_manager import BsMessageManager
from newcell.managers.message_manager import MessageManager
from newcell.messages.export_columns import BS_EXPORT_COLUMNS

raw_bs_messages = [
    b"\xe4\x07\x02\x13t\xdb\t\x01\x01\x00\x00\x00\x00\x00[\x01\xfe\xfe\x00\x00\x00\x00\x00\x00",
    b"\xe4\x07\x02\x13\xce\xdb\t\x01\x02\x00\x00\x00\x00\x00[\x01\xfe\xfe\x00\x00\x00\x00\x00\x00",
    b"\xe4\x07\x02\x132\xdc\t\x01\x03\x00\x00\x00\x00\x00[\x01\xfe\xfe\x00\x00\x00\x00\x00\x00",
    b"\xe4\x07\x02\x13\x96\xdc\t\x01\x04\x00\x00\x00\x00\x00[\x01\xfe\xfe\x00\x00\x00\x00\x00\x00",
    b"\xe4\x07\x02\x13\xfa\xdc\t\x01\x05\x00\x00\x00\x00\x00Z\x01\xfe\xfe\x00\x00\x00\x00\x00\x00",
]

expected_exported_rows = [
    (datetime(2020, 2, 19, 17, 42, 32, 200000), 1, 0, 3.47, 652.78, 0, 0, 0),
    (datetime(2020, 2, 19, 17, 42, 33, 100000), 2, 0, 3.47, 652.78, 0, 0, 0),
    (datetime(2020, 2, 19, 17, 42, 34, 100000), 3, 0, 3.47, 652.78, 0, 0, 0),
    (datetime(2020, 2, 19, 17, 42, 35, 100000), 4, 0, 3.47, 652.78, 0, 0, 0),
    (datetime(2020, 2, 19, 17, 42, 36, 100000), 5, 0, 3.46, 652.78, 0, 0, 0)
]

seoul_utc_time_difference = 9

expected_time_adjusted_rows = [
    (row[0] + timedelta(hours=seoul_utc_time_difference),) + row[1:]
    for row in expected_exported_rows
]


class TestBsMessageManager:
    def test_add_message(self):
        # Given: BsMessageManager instance and raw bs messages
        bs_message_manager = BsMessageManager()

        # When: BsMessageManager.add_message is called on raw bs messages
        for raw_message in raw_bs_messages:
            bs_message_manager.add_message(raw_message)

        # Then: exported messages should eqaul as expected list of tuples
        assert [tuple(row) for row in bs_message_manager.messages] == expected_exported_rows

    def test_export_dataframe(self):
        # Given: BsMessageManager instance and raw bs messages are added to it
        bs_message_manager = BsMessageManager()

        for raw_message in raw_bs_messages:
            bs_message_manager.add_message(raw_message)

        expected_dataframe = pd.DataFrame(expected_exported_rows, columns=BS_EXPORT_COLUMNS)

        # When: BsMessageManager.export_dataframe is called
        actual_dataframe = bs_message_manager.export_dataframe()

        # Then: exported dataframe should returned as expected
        assert actual_dataframe.equals(expected_dataframe)

        # And: messages list should empty
        assert not bs_message_manager.messages

    def test_adjust_timezone(self):
        expected_dataframe = pd.DataFrame(
            expected_time_adjusted_rows, columns=BS_EXPORT_COLUMNS
        )

        # Given: exported dataframe (date adjusted)
        tz = pytz.timezone(
            TimezoneFinder().timezone_at(lat=37.40891712, lng=126.69735474)
        )

        dataframe = pd.DataFrame(expected_exported_rows, columns=BS_EXPORT_COLUMNS)

        # When: BsMessageManager.adjust_timezone is called
        actual_dataframe = MessageManager.adjust_timezone(dataframe, tz)

        # Then: the result sould be the same with expected one
        assert actual_dataframe.equals(expected_dataframe)

    def test_adjust_timezone_by_coordinate(self):
        expected_dataframe = pd.DataFrame(
            expected_time_adjusted_rows, columns=BS_EXPORT_COLUMNS
        )

        # Given: exported dataframe (date adjusted)
        dataframe = pd.DataFrame(expected_exported_rows, columns=BS_EXPORT_COLUMNS)

        # When: BsMessageManager.adjust_timezone_by_coordinate is called
        actual_dataframe = MessageManager.adjust_timezone_by_coordinate(
            dataframe, lat=37.4, long=126.7
        )

        # Then: the result sould be the same with expected one
        assert actual_dataframe.equals(expected_dataframe)
