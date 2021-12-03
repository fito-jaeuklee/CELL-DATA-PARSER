from datetime import datetime

from newcell.messages.imu_message import ImuMessage


imu_message_bytes = b'j\xdb\t\x01\xff1\x00(\xf7\x1c\xff\x95\x00\x03\xff\xf7P\x01\xfc\x00\xa4\xfe'

expected_imu_message = ImuMessage(
    time_utc=17423210,
    accel_x=-207,
    accel_y=40,
    accel_z=-2276,
    gyro_x=-107,
    gyro_y=3,
    gyro_z=-9,
    mag_x=336,
    mag_y=252,
    mag_z=-348,
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
        expected_accel = (-207, 40, -2276)

        # When: call accel property
        actual_accel = imu_message.accel

        # Then: accel of (x, y, z) format matches
        assert actual_accel == expected_accel

    def test_gyro(self):
        # Given: parsed and created ImuMessage from raw bytes of message
        imu_message = ImuMessage.create(imu_message_bytes)
        expected_gyro = (-107, 3, -9)

        # When: call gyro property
        actual_gyro = imu_message.gyro

        # Then: gyro of (x, y, z) format matches
        assert actual_gyro == expected_gyro

    def test_magnet(self):
        # Given: parsed and created ImuMessage from raw bytes of message
        imu_message = ImuMessage.create(imu_message_bytes)
        expected_magnet = (336, 252, -348)

        # When: call magnet property
        actual_magnet = imu_message.magnet

        # Then: magnet of (x, y, z) format matches
        assert actual_magnet == expected_magnet

    def test_datetime(self):
        # Given: parsed and created ImuMessage from raw bytes of message
        imu_message = ImuMessage.create(imu_message_bytes)
        expected_datetime = datetime(1900, 1, 1, 17, 42, 32, 100000)

        # When: call datetime property
        actual_datetime = imu_message.datetime

        # Then: datetime matches
        assert actual_datetime == expected_datetime

    def test_export_row(self):
        # Given: parsed and created ImuMessage from raw bytes of message
        imu_message = ImuMessage.create(imu_message_bytes)
        expected_exported_row = (
            datetime(1900, 1, 1, 17, 42, 32, 100000), -207, 40, -2276, -107, 3, -9, 336, 252, -348,
        )

        # When: call export_row method
        actual_exported_row = imu_message.export_row()

        # Then: exported row matches
        assert actual_exported_row == expected_exported_row
