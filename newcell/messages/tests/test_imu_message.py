from datetime import datetime

from newcell.messages.imu_message import ImuMessage


imu_message_bytes = b'\x00\x08\x1a(\xff\xd1\x00\x15\xf7l\xff\xf0\x00\x04\x00\x01\xc3\xff\xd1\x00I\xfe'

expected_imu_message = ImuMessage(
    time_utc=530984,
    accel_x=-47,
    accel_y=21,
    accel_z=-2196,
    gyro_x=-16,
    gyro_y=4,
    gyro_z=1,
    mag_x=-61,
    mag_y=209,
    mag_z=-439,
)

class TestImuMessage:
    def test_bytes_message_parsed_correctly(self):
        # When: ImuMessage.create is called on raw imu message
        actual_imu_message = ImuMessage.create(imu_message_bytes)

        # Then: parsed message should eqaul to expected
        assert actual_imu_message == expected_imu_message

    def test_accel(self):
        # Given: parsed and created ImuMessage from raw bytes of message
        imu_message = ImuMessage.create(imu_message_bytes)
        expected_accel = (-47, 21, -2196)

        # When: call accel property
        actual_accel = imu_message.accel

        # Then: accel of (x, y, z) format matches
        assert actual_accel == expected_accel

    def test_gyro(self):
        # Given: parsed and created ImuMessage from raw bytes of message
        imu_message = ImuMessage.create(imu_message_bytes)
        expected_gyro = (-16, 4, 1)

        # When: call gyro property
        actual_gyro = imu_message.gyro

        # Then: gyro of (x, y, z) format matches
        assert actual_gyro == expected_gyro

    def test_magnet(self):
        # Given: parsed and created ImuMessage from raw bytes of message
        imu_message = ImuMessage.create(imu_message_bytes)
        expected_magnet = (-61, 209, -439) 

        # When: call magnet property
        actual_magnet = imu_message.magnet

        # Then: magnet of (x, y, z) format matches
        assert actual_magnet == expected_magnet

    def test_datetime(self):
        # Given: parsed and created ImuMessage from raw bytes of message
        imu_message = ImuMessage.create(imu_message_bytes)
        expected_datetime = datetime(1900, 1, 1, 0, 8, 26, 400000)

        # When: call datetime property
        actual_datetime = imu_message.datetime

        # Then: datetime matches
        assert actual_datetime == expected_datetime

    def test_export_row(self):
        # Given: parsed and created ImuMessage from raw bytes of message
        imu_message = ImuMessage.create(imu_message_bytes)
        expected_exported_row = (datetime(1900, 1, 1, 0, 8, 26, 400000), -47, 21, -2196, -16, 4, 1, -61, 209, -439)

        # When: call export_row method
        actual_exported_row = imu_message.export_row()

        # Then: exported row matches
        assert actual_exported_row == expected_exported_row
