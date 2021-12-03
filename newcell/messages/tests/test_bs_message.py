from datetime import datetime

from newcell.messages.bs_message import BsMessage


bs_message_bytes = b'\xe4\x07\x02\x13t\xdb\t\x01\x01\x00\x00\x00\x00\x00[\x01\xfe\xfe\x00\x00\x00\x00\x00\x00'

expected_bs_message = BsMessage(
    date=318900196,
    time_utc=17423220,
    operation_time=1,
    hr=0,
    battery_scaled=347,
    cell_temperature_scaled=65278,
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
        expected_datetime = datetime(2020, 2, 19, 17, 42, 32, 200000)

        # When: call datetime property
        actual_datetime = bs_message.datetime

        # Then: datetime matches
        assert actual_datetime == expected_datetime

    def test_export_row(self):
        # Given: parsed and created BsMessage from raw bytes of message
        bs_message = BsMessage.create(bs_message_bytes)
        expected_exported_row = (datetime(2020, 2, 19, 17, 42, 32, 200000), 1, 0)

        # When: call export_row method
        actual_exported_row = bs_message.export_row()

        # Then: exported row matches
        assert actual_exported_row == expected_exported_row

    def test_battery(self):
        # Given: parsed and created BsMessage from raw bytes of message
        bs_message = BsMessage.create(bs_message_bytes)
        expected_battery = 3.47

        # When: call battery method
        autual_battery = bs_message.battery

        # Then: battery value matches
        assert autual_battery == expected_battery

    def test_cell_temparature(self):
        # Given: parsed and created BsMessage from raw bytes of message
        bs_message = BsMessage.create(bs_message_bytes)
        expected_cell_temparature = 652.78

        # When: call battery method
        autual_cell_temparature = bs_message.cell_temperature

        # Then: battery value matches
        assert autual_cell_temparature == expected_cell_temparature
