from datetime import datetime

from newcell.messages.bs_message import BsMessage


bs_message_bytes = b'\xe4\x07\x04\x0b\x00\x08#\n\xad\x08\x00\x00\x00\x00\x90\x01\x00\x00\x00\x00\x00\x00\x00\x00'

expected_bs_message = BsMessage(
    date=184813540,
    time_utc=170067968,
    operation_time=2221,
    hr=0,
    battery=400,
    cell_temperature=0,
    reserve_1=0,
    reserve_2=0,
    reserve_3=0,
)

class TestBsMessage:
    def test_bytes_message_parsed_correctly(self):
        # When: BsMessage.create is called on raw bs message
        actual_bs_message = BsMessage.create(bs_message_bytes)

        # Then: parsed message should eqaul to expected
        assert actual_bs_message == expected_bs_message

    def test_datetime(self):
        # Given: parsed and created BsMessage from raw bytes of message
        bs_message = BsMessage.create(bs_message_bytes)
        expected_datetime = datetime(2020, 4, 11, 0, 8, 35, 100000)

        # When: call datetime property
        actual_datetime = bs_message.datetime

        # Then: datetime matches
        assert actual_datetime == expected_datetime

    def test_export_row(self):
        # Given: parsed and created BsMessage from raw bytes of message
        bs_message = BsMessage.create(bs_message_bytes)
        expected_exported_row = (datetime(2020, 4, 11, 0, 8, 35, 100000), 2221, 0)

        # When: call export_row method
        actual_exported_row = bs_message.export_row()

        # Then: exported row matches
        assert actual_exported_row == expected_exported_row
