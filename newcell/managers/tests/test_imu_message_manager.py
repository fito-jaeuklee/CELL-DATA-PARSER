import pandas
import pytz
from datetime import datetime
from timezonefinder import TimezoneFinder

from newcell.managers.imu_message_manager import (
    ImuMessageManager,
    HEADER_IMU,
)


raw_imu_messages = [
    b'\x00\x08#!\xff\xca\x00\x14\xf7\x82\xff\xec\x00\x01\x00\x00\xd3\xff\xdb\x00a\xfe',
    b'\x00\x08#"\xff\xce\x00\x13\xf7{\xff\xf1\xff\xff\x00\x01\xd3\xff\xdb\x00a\xfe',
    b'\x00\x08##\xff\xcc\x00\x14\xf7\x7f\xff\xec\x00\x00\x00\x01\xd3\xff\xdb\x00a\xfe',
    b'\x00\x08#$\xff\xcd\x00\x15\xf7\x84\xff\xf1\x00\x02\x00\x01\xd3\xff\xdb\x00a\xfe',
    b'\x00\x08#%\xff\xcc\x00\x13\xf7\x81\xff\xed\x00\x00\x00\x00\xd3\xff\xdb\x00a\xfe',
]

expected_exported_rows = [
    (datetime(1900, 1, 1, 0, 8, 35, 330000), -54, 20, -2174, -20, 1, 0, -45, 219, -415),
    (datetime(1900, 1, 1, 0, 8, 35, 340000), -50, 19, -2181, -15, -1, 1, -45, 219, -415),
    (datetime(1900, 1, 1, 0, 8, 35, 350000), -52, 20, -2177, -20, 0, 1, -45, 219, -415),
    (datetime(1900, 1, 1, 0, 8, 35, 360000), -51, 21, -2172, -15, 2, 1, -45, 219, -415),
    (datetime(1900, 1, 1, 0, 8, 35, 370000), -52, 19, -2175, -19, 0, 0, -45, 219, -415),
]

date_to_adjust = datetime(2020, 4, 11, 0, 8, 34, 800000)

expected_date_adjusted_rows = [
    (datetime(2020, 4, 11, 0, 8, 35, 330000), -54, 20, -2174, -20, 1, 0, -45, 219, -415),
    (datetime(2020, 4, 11, 0, 8, 35, 340000), -50, 19, -2181, -15, -1, 1, -45, 219, -415),
    (datetime(2020, 4, 11, 0, 8, 35, 350000), -52, 20, -2177, -20, 0, 1, -45, 219, -415),
    (datetime(2020, 4, 11, 0, 8, 35, 360000), -51, 21, -2172, -15, 2, 1, -45, 219, -415),
    (datetime(2020, 4, 11, 0, 8, 35, 370000), -52, 19, -2175, -19, 0, 0, -45, 219, -415),
]

seoul_utc_time_difference = 9

expected_datetime_adjusted_rows = [
    (datetime(2020, 4, 11, seoul_utc_time_difference, 8, 35, 330000), -54, 20, -2174, -20, 1, 0, -45, 219, -415),
    (datetime(2020, 4, 11, seoul_utc_time_difference, 8, 35, 340000), -50, 19, -2181, -15, -1, 1, -45, 219, -415),
    (datetime(2020, 4, 11, seoul_utc_time_difference, 8, 35, 350000), -52, 20, -2177, -20, 0, 1, -45, 219, -415),
    (datetime(2020, 4, 11, seoul_utc_time_difference, 8, 35, 360000), -51, 21, -2172, -15, 2, 1, -45, 219, -415),
    (datetime(2020, 4, 11, seoul_utc_time_difference, 8, 35, 370000), -52, 19, -2175, -19, 0, 0, -45, 219, -415),
]

class TestImuMessageManager:
    def test_add_message(self):
        # Given: ImuMessageManager instance and raw imu messages
        imu_message_manager = ImuMessageManager()

        # When: ImuMessageManager.add_message is called on raw imu messages
        for raw_message in raw_imu_messages:
            imu_message_manager.add_message(raw_message)

        # Then: exported messages should eqaul as expected list of tuples
        assert imu_message_manager.messages == expected_exported_rows

    def test_export_dataframe(self):
        # Given: ImuMessageManager instance and raw imu messages are added to it
        imu_message_manager = ImuMessageManager()

        for raw_message in raw_imu_messages:
            imu_message_manager.add_message(raw_message)

        expected_dataframe = pandas.DataFrame(expected_exported_rows, columns=HEADER_IMU)

        # When: ImuMessageManager.export_dataframe is called
        actual_dataframe = imu_message_manager.export_dataframe()

        # Then: exported dataframe should returned as expected
        assert actual_dataframe.equals(expected_dataframe)

        # And: messages list should empty
        assert not imu_message_manager.messages

    def test_adjust_date(self):
        expected_dataframe = pandas.DataFrame(expected_date_adjusted_rows, columns=HEADER_IMU)

        # Given: exported dataframe
        dataframe = pandas.DataFrame(expected_exported_rows, columns=HEADER_IMU)

        # When: ImuMessageManager.adjust_timezone is called
        actual_dataframe = ImuMessageManager.adjust_date(dataframe, date_to_adjust)

        # Then: the result sould be the same with expected one
        assert actual_dataframe.equals(expected_dataframe)

    def test_adjust_timezone(self):
        expected_dataframe = pandas.DataFrame(expected_datetime_adjusted_rows, columns=HEADER_IMU)

        # Given: exported dataframe (date adjusted)
        tz = pytz.timezone(TimezoneFinder().timezone_at(lat=37.40891712, lng=126.69735474))

        dataframe = pandas.DataFrame(expected_date_adjusted_rows, columns=HEADER_IMU)

        # When: ImuMessageManager.adjust_timezone is called
        actual_dataframe = ImuMessageManager.adjust_timezone(dataframe, tz)

        # Then: the result sould be the same with expected one
        assert actual_dataframe.equals(expected_dataframe)

    def test_adjust_timezone_by_coordinate(self):
        expected_dataframe = pandas.DataFrame(expected_datetime_adjusted_rows, columns=HEADER_IMU)

        # Given: exported dataframe (date adjusted)
        dataframe = pandas.DataFrame(expected_date_adjusted_rows, columns=HEADER_IMU)

        # When: ImuMessageManager.adjust_timezone_by_coordinate is called
        actual_dataframe = ImuMessageManager.adjust_timezone_by_coordinate(dataframe, lat=37.4, long=126.7)

        # Then: the result sould be the same with expected one
        assert actual_dataframe.equals(expected_dataframe)
