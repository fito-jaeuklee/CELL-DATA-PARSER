from newcell.messages.start_message import StartMessage 


start_message_bytes = b'\xb9\x08LCXBA5\x03\x00\x1b\x00\xff\xff\xff\xffPolar OH1 8714ED23\x00\x00\x00\x00\x00\x00\x00\x00R\xbd\x00\x00d;\x00\x00\xe0\xbc\xc9\xb3\x8c\xc0Xq\xc2\xbf\xfcU\x1c=\x00\x00\x9b?\x00\x00\x9c?\x00\x00\x96?'

expected_start_message = StartMessage(
    cell_serial_number=2233,
    product_id=b"LC",
    version_id=b"XB",
    product_version=b"A5",
    firmware_major_version=3,
    firmware_minor_version=0,
    gnss_fix_time=27,
    used_nand_block_size=65535,
    used_nand_page_size=65535,
    ble_id=b"Polar OH1 8714ED23\x00\x00\x00\x00\x00\x00",
    imu_cal_accx=-0.05126953125,
    imu_cal_accy=0.00347900390625,
    imu_cal_accz=-0.02734375,
    imu_cal_gyrx=-4.396946430206299,
    imu_cal_gyry=-1.5190839767456055,
    imu_cal_gyrz=0.038167938590049744,
    imu_cal_magx=1.2109375,
    imu_cal_magy=1.21875,
    imu_cal_magz=1.171875,
)

class TestEndMessage:
    def test_bytes_message_parsed_correctly(self):
        # When: StartMessage.create is called on raw start message
        actual_start_message = StartMessage.create(start_message_bytes)

        # Then: parsed message should eqaul to expected
        assert actual_start_message == expected_start_message

    def test_imu_accel_calibration(self):
        # When: call imu_accel_calibration property
        actual_imu_accel_calibration = expected_start_message.imu_accel_calibration

        # Then: result tuple values matches
        assert actual_imu_accel_calibration == (
            -0.05126953125, 0.00347900390625, -0.02734375,
        )

    def test_imu_gyro_calibration(self):
        # When: call imu_gyro_calibration property
        actual_imu_gyro_calibration = expected_start_message.imu_gyro_calibration

        # Then: result tuple values matches
        assert actual_imu_gyro_calibration == (
            -4.396946430206299, -1.5190839767456055, 0.038167938590049744,
        )

    def test_imu_magnet_calibration(self):
        # When: call imu_magnet_calibration property
        actual_imu_magnet_calibration = expected_start_message.imu_magnet_calibration

        # Then: result tuple values matches
        assert actual_imu_magnet_calibration == (
            1.2109375, 1.21875, 1.171875,
        )

    def test_ble_hr_id(self):
        # When: call ble_hr_id property
        actual_ble_hr_id = expected_start_message.ble_hr_id

        # Then: result ble id matches (stripped)
        assert actual_ble_hr_id == "Polar OH1 8714ED23"

    def test_firmware_version(self):
        # When: call firmware_version property
        actual_firmware_version = expected_start_message.firmware_version

        # Then: result firmware version values matches
        assert actual_firmware_version == "v3.0"

    def test_device_number(self):
        # When: call device_number property
        actual_device_number = expected_start_message.device_number

        # Then: result tuple values matches
        assert actual_device_number == 2233

    def test_device_version(self):
        # When: call device_version property
        actual_device_version = expected_start_message.device_version

        # Then: result device version values matches
        assert actual_device_version == "5A"

    def test_device_model(self):
        # When: call device_model property
        actual_device_model = expected_start_message.device_model

        # Then: result device model values matches
        assert actual_device_model == "CLBX"
