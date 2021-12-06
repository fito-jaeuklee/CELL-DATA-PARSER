from datetime import datetime, timedelta

import pandas as pd
import pytz
from timezonefinder import TimezoneFinder

from newcell.managers.imu_message_manager import ImuMessageManager
from newcell.managers.message_manager import MessageManager
from newcell.messages.export_columns import IMU_EXPORT_COLUMNS

raw_imu_messages = [
    b"\xb2\xdb\t\x01\xff6\x00'\xf7(\xff\x95\x00\x04\xff\xf7M\x01\xf1\x00\x9d\xfe",
    b"\xb3\xdb\t\x01\xff3\x00*\xf7!\xff\x96\x00\x05\xff\xf8M\x01\xf1\x00\x9d\xfe",
    b"\xb4\xdb\t\x01\xff3\x00*\xf7 \xff\x96\x00\x04\xff\xf8M\x01\xf1\x00\x9d\xfe",
    b"\xb5\xdb\t\x01\xff3\x00(\xf7\x19\xff\x96\x00\x03\xff\xf9M\x01\xf1\x00\x9d\xfe",
    b"\xb6\xdb\t\x01\xff6\x00*\xf7\x18\xff\x96\x00\x04\xff\xf8M\x01\xf1\x00\x9d\xfe",
]

expected_exported_rows = [
    (datetime(1900, 1, 1, 17, 42, 32, 820000), -202, 39, -2264, -107, 4, -9, 333, 241, -355),
    (datetime(1900, 1, 1, 17, 42, 32, 830000), -205, 42, -2271, -106, 5, -8, 333, 241, -355),
    (datetime(1900, 1, 1, 17, 42, 32, 840000), -205, 42, -2272, -106, 4, -8, 333, 241, -355),
    (datetime(1900, 1, 1, 17, 42, 32, 850000), -205, 40, -2279, -106, 3, -7, 333, 241, -355),
    (datetime(1900, 1, 1, 17, 42, 32, 860000), -202, 42, -2280, -106, 4, -8, 333, 241, -355),
]

date_to_adjust = datetime(2020, 4, 11, 17, 42, 32, 810000)

expected_date_adjusted_rows = [
    (datetime(2020, 4, 11, 17, 42, 32, 820000), -202, 39, -2264, -107, 4, -9, 333, 241, -355),
    (datetime(2020, 4, 11, 17, 42, 32, 830000), -205, 42, -2271, -106, 5, -8, 333, 241, -355),
    (datetime(2020, 4, 11, 17, 42, 32, 840000), -205, 42, -2272, -106, 4, -8, 333, 241, -355),
    (datetime(2020, 4, 11, 17, 42, 32, 850000), -205, 40, -2279, -106, 3, -7, 333, 241, -355),
    (datetime(2020, 4, 11, 17, 42, 32, 860000), -202, 42, -2280, -106, 4, -8, 333, 241, -355),
]

seoul_utc_time_difference = 9

expected_datetime_adjusted_rows = [
    (row[0] + timedelta(hours=seoul_utc_time_difference),) + row[1:]
    for row in expected_date_adjusted_rows
]


class TestImuMessageManager:
    def test_add_message(self):
        # Given: ImuMessageManager instance and raw imu messages
        imu_message_manager = ImuMessageManager()

        # When: ImuMessageManager.add_message is called on raw imu messages
        for raw_message in raw_imu_messages:
            imu_message_manager.add_message(raw_message)

        # Then: exported messages should eqaul as expected list of tuples
        assert [tuple(row) for row in imu_message_manager.messages] == expected_exported_rows

    def test_export_dataframe(self):
        # Given: ImuMessageManager instance and raw imu messages are added to it
        imu_message_manager = ImuMessageManager()

        for raw_message in raw_imu_messages:
            imu_message_manager.add_message(raw_message)

        expected_dataframe = pd.DataFrame(
            expected_exported_rows, columns=IMU_EXPORT_COLUMNS
        )

        # When: ImuMessageManager.export_dataframe is called
        actual_dataframe = imu_message_manager.export_dataframe()

        # Then: exported dataframe should returned as expected
        assert actual_dataframe.equals(expected_dataframe)

        # And: messages list should empty
        assert not imu_message_manager.messages

    def test_adjust_date(self):
        expected_dataframe = pd.DataFrame(
            expected_date_adjusted_rows, columns=IMU_EXPORT_COLUMNS
        )

        # Given: exported dataframe
        dataframe = pd.DataFrame(expected_exported_rows, columns=IMU_EXPORT_COLUMNS)

        # When: ImuMessageManager.adjust_timezone is called
        actual_dataframe = MessageManager.adjust_date(dataframe, date_to_adjust)

        # Then: the result sould be the same with expected one
        assert actual_dataframe.equals(expected_dataframe)

    def test_adjust_date_when_date_changes_in_series_of_datetime(self):
        exptected_rows = [
            (datetime(2020, 4, 11, 23, 59, 59, 990000), -202, 39, -2264, -107, 4, -9, 333, 241, -355),
            (datetime(2020, 4, 12, 00, 00, 00, 000000), -205, 42, -2271, -106, 5, -8, 333, 241, -355),
        ]

        expected_dataframe = pd.DataFrame(exptected_rows, columns=IMU_EXPORT_COLUMNS)

        # Given: sample exported rows in which the date changes & reference date
        date_changing_input_rows = [
            (datetime(1900, 1, 1, 23, 59, 59, 990000), -202, 39, -2264, -107, 4, -9, 333, 241, -355),
            (datetime(1900, 1, 2, 00, 00, 00, 000000), -205, 42, -2271, -106, 5, -8, 333, 241, -355),
        ]

        reference_date = datetime(2020, 4, 11, 17, 42, 32, 810000)

        # When: ImuMessageManager.adjust_date is called
        actual_dataframe = MessageManager.adjust_date(
            pd.DataFrame(date_changing_input_rows, columns=IMU_EXPORT_COLUMNS),
            reference_date,
        )

        # Then: date is adjusted correctly
        assert actual_dataframe.equals(expected_dataframe)

    def test_adjust_timezone(self):
        expected_dataframe = pd.DataFrame(
            expected_datetime_adjusted_rows, columns=IMU_EXPORT_COLUMNS
        )

        # Given: exported dataframe (date adjusted)
        tz = pytz.timezone(
            TimezoneFinder().timezone_at(lat=37.40891712, lng=126.69735474)
        )

        dataframe = pd.DataFrame(expected_date_adjusted_rows, columns=IMU_EXPORT_COLUMNS)

        # When: ImuMessageManager.adjust_timezone is called
        actual_dataframe = MessageManager.adjust_timezone(dataframe, tz)

        # Then: the result sould be the same with expected one
        assert actual_dataframe.equals(expected_dataframe)

    def test_adjust_timezone_by_coordinate(self):
        expected_dataframe = pd.DataFrame(
            expected_datetime_adjusted_rows, columns=IMU_EXPORT_COLUMNS
        )

        # Given: exported dataframe (date adjusted)
        dataframe = pd.DataFrame(expected_date_adjusted_rows, columns=IMU_EXPORT_COLUMNS)

        # When: ImuMessageManager.adjust_timezone_by_coordinate is called
        actual_dataframe = MessageManager.adjust_timezone_by_coordinate(
            dataframe, lat=37.4, long=126.7
        )

        # Then: the result sould be the same with expected one
        assert actual_dataframe.equals(expected_dataframe)
