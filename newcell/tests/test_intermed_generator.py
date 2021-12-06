from datetime import datetime

from newcell.intermed_generator import START_MSG_TYPE, IntermedGenerator
from newcell.messages.export_columns import (BS_EXPORT_COLUMNS,
                                             GPS_EXPORT_COLUMNS,
                                             IMU_EXPORT_COLUMNS)
from newcell.messages.start_message import StartMessage


dummy_on_top = b"\xca`\x02\xe0\x01I"
start_message = b"COACH\x04L\xae\x08LCXBA3\x03\x00\x15\x00\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\x00\xa0\x9c\xbd\x00\x00|<\x00@\xeb\xbd\xee\x82\xbf\xc0/\xf8\xab>/\xf8\xab\xbf\x00\x80\x96?\x00\x80\x97?\x00\x80\x91?\xf5\r\n"
gps_messge = b"COACH\x014\xe4\x07\x02\x13j\xdb\t\x01P\xcf\x02\x13W\x00\x00\x00(\xb3\xa7\xdb'\x01\x00\x00\xde\x13\x02\x00|\x15\xec\x13\xd6\x06\x00\x00Pr\x00\x00\x00\x00\xd1\x00\xda\x00\xaf\x00\x06\x06)A\x01\r\n"
imu_message = b"COACH\x02\x16j\xdb\t\x01\xff1\x00(\xf7\x1c\xff\x95\x00\x03\xff\xf7P\x01\xfc\x00\xa4\xfe6\r\n"
bs_message = b"COACH\x03\x18\xe4\x07\x02\x13t\xdb\t\x01\x01\x00\x00\x00\x00\x00[\x01\xfe\xfe\x00\x00\x00\x00\x00\x00\x15\r\n"
end_message = b"COACH\x05\tNX\x017\x00\x02\x00&\x00\x08\r\n\xff@@DUMMY@DUMMY@DUMMY@@@@@@@DUMMY@DUMMY@DUMMY@DUMMY@DUMMY@@"

row_bytes = (
    dummy_on_top +
    start_message +
    gps_messge +
    imu_message +
    bs_message +
    end_message
)


class TestIntermedGenerator:
    @staticmethod
    def _auxiliary_crc_calculator(data):
        crc = 0

        for each_byte in data:
            crc ^= each_byte

        return crc

    def test_crc_check_on_dummy(self):
        # Given: CRC value comes at last
        dummy_data = b"ABCDEDFGHIJLMNOP"

        expected_crc_value = TestIntermedGenerator._auxiliary_crc_calculator(dummy_data)

        # When: IntermedGenerator._crc_check is called
        actual_test_result = IntermedGenerator._crc_check(dummy_data, expected_crc_value)

        # Then: the test should be passed
        assert actual_test_result == True

        # And when: IntermedGenerator._crc_check is called on wrong crc value
        wrong_crc_value = expected_crc_value - 1
        actual_false_result = IntermedGenerator._crc_check(dummy_data, wrong_crc_value)

        # Then: the test should be failed
        actual_false_result == False

    def test_find_next_message(self):
        # Given: .ftg file buffer: memory view
        memview = memoryview(row_bytes)

        # When: IntermedGenerator.find_next_message is called on memoryview: from the first
        new_cursor, message = IntermedGenerator.find_next_message(memview, 0)
        msg_type, crc_data, payload, crc = message

        # Then: some tests should be passed
        assert new_cursor > len(payload)
        assert msg_type == START_MSG_TYPE
        assert IntermedGenerator._crc_check(crc_data, crc) == True
        assert StartMessage.start_message_struct.size == len(payload)

    def test_raw_parser(self):
        # Given: .ftg file buffer: memory view
        memview = memoryview(row_bytes)

        # When: row_parser is called and all of its contents are consumed
        result = tuple(IntermedGenerator.raw_parser(memview))

        # Then: all messages with b'COACH' prefix should be parsed: length should be 5
        assert len(result) == 5

    def test_prepare_dataframes_to_yield_when_all_managers_empty(self):
        # Given: make sure that every manager's messages are empty
        IntermedGenerator.gps_message_manager.messages = []
        IntermedGenerator.imu_message_manager.messages = []
        IntermedGenerator.bs_message_manager.messages = []

        # When: call prepare_dataframes_to_yield to retrieve dataframes
        gps_df, imu_df, bs_df = IntermedGenerator.prepare_dataframes_to_yield()

        # Then: all datafrmes are empty
        assert gps_df.empty == True
        assert imu_df.empty == True
        assert bs_df.empty == True

    def test_prepare_dataframes_to_yield(self):
        gps_row = (
            datetime(2020, 2, 19, 17, 42, 32, 100000),
            37.663517666666664, 127.11675883333334, 1.75, 136.158, 55.0,
            51.0, 292.64, 0.0, 2.09, 2.18, 1.75, 6, 6, 0.41, 65,
        )     
        imu_row = (
            datetime(1900, 1, 1, 17, 42, 32, 820000),
            -202, 39, -2264, -107, 4, -9, 333, 241, -355,
        )
        bs_row = (datetime(2020, 2, 19, 17, 42, 32, 200000), 1, 0, 3.47, 652.78, 0, 0, 0)

        # Given: each managers has a exported record
        IntermedGenerator.gps_message_manager.messages.append(gps_row)
        IntermedGenerator.imu_message_manager.messages.append(imu_row)
        IntermedGenerator.bs_message_manager.messages.append(bs_row)

        # When: call prepare_dataframes_to_yield to retrieve dataframes
        gps_df, imu_df, bs_df = IntermedGenerator.prepare_dataframes_to_yield()

        # Then: each dataframes has one row
        assert len(gps_df) == 1
        assert len(imu_df) == 1
        assert len(bs_df) == 1

        # And: imu's datetime has adjusted
        assert imu_df.at[0, "datetime"] == datetime(2020, 2, 19, 17, 42, 32, 820000)

    def test_parse(self):
        # When: file buffer with 5 types of messages are passed to parse method
        parse_generator = IntermedGenerator.parse(row_bytes)

        # Then: the generator should yield a 5-tuple of start msg, 3 dataframes, and end msg
        result = next(parse_generator)

        assert len(result) == 5
        assert result[0] is not None
        assert result[1].size == len(GPS_EXPORT_COLUMNS)
        assert result[2].size == len(IMU_EXPORT_COLUMNS)
        assert result[3].size == len(BS_EXPORT_COLUMNS)
