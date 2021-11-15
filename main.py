from tkinter import filedialog
import struct
import csv
import re
import os
import data_verification as dv


# GPS 4 + 45 + 1 = 50
# IMU = 30
# BS = 30
# gps / imu / bs verification
# time verification

class Parser:
    def __init__(self):
        self.gps_data = []
        self.imu_data = []
        self.bs_data = []
        self.crc_flag = False
        self.crc_failed_cnt = 0

    @staticmethod
    def crc_check(check_data, crc_value):
        crc = 0
        for i in range(0, len(check_data)):
            crc ^= check_data[i]

        # print("check result / crc value", crc, crc_value)

        if crc == crc_value:
            return True
        else:
            return False

    @staticmethod
    def int_to_bytes(x: int) -> bytes:
        return x.to_bytes((x.bit_length() + 7) // 8, 'big')

    @staticmethod
    def gps_to_lat_lon_conv(gps_x, gps_y):
        # print(gps_x, gps_y)
        gps_x = float(gps_x * 100)
        gps_y = float(gps_y * 100)

        d_x = int(gps_x / 100)
        m_x = (gps_x - (d_x * 100)) / 60

        lat = d_x + m_x

        d_y = int(gps_y / 100)
        m_y = (gps_y - (d_y * 100)) / 60

        lon = d_y + m_y
        # print("12314", lat, lon)

        return lat, lon

    def convert_ms(self, hmsf):
        hours = hmsf[0:2]
        minutes = hmsf[2:4]
        seconds = hmsf[4:6]
        milliseconds = hmsf[6:8]

        hours, minutes, seconds, milliseconds = map(int, (hours, minutes, seconds, milliseconds))

        return (hours * 60 * 60 + minutes * 60 + seconds) * 100 + milliseconds

    def gps_parser(self, gps_data):
        gps_one_list = []
        # date
        # print(gps_data)
        year = struct.unpack('H', gps_data[:2])
        # print(gps_data[:2])
        # print("Unpack data = ", year)
        # print(self.int_to_bytes(gps_data[2]), self.int_to_bytes(gps_data[3]))
        month = struct.unpack('B', self.int_to_bytes(gps_data[2]))
        day = struct.unpack('B', self.int_to_bytes(gps_data[3]))

        yymmdd = str(year[0])[2:] + str(month[0]).zfill(2) + str(day[0]).zfill(2)
        # print(yymmdd)
        gps_one_list.append(yymmdd)

        # UTC
        utc = struct.unpack('BBBB', gps_data[4:8])
        utc_hms = str(utc[0]).zfill(2) + str(utc[1]).zfill(2) + str(utc[2]).zfill(2) + str(utc[3]).zfill(2)
        # print(utc_hms)
        gps_one_list.append(utc_hms)

        # Lat & Long
        lat = struct.unpack('i', gps_data[8:12])
        lon = struct.unpack('i', gps_data[12:16])
        lat_conv, lon_conv = self.gps_to_lat_lon_conv(lat[0] * (10 ** -5), lon[0] * (10 ** -5))
        # print(lat_conv)
        # print(lon_conv)
        set_lat_lon = [round(lat_conv * 10 ** -2, 7), round(lon_conv * 10 ** -2, 7)]
        gps_one_list.append(set_lat_lon)
        # height
        # print(struct.unpack('I', gps_data[16:20]))
        height = struct.unpack('I', gps_data[16:20])
        gps_one_list.append(height[0])
        # hAcc
        # print(struct.unpack('H', gps_data[20:22]))
        hAcc = struct.unpack('H', gps_data[20:22])
        gps_one_list.append(hAcc[0])
        # vAcc
        # print(struct.unpack('H', gps_data[22:24]))
        vAcc = struct.unpack('H', gps_data[22:24])
        gps_one_list.append(vAcc[0])

        # Hspeed
        # print(struct.unpack('i', gps_data[24:28]))
        Hspeed = struct.unpack('i', gps_data[24:28])
        gps_one_list.append(Hspeed[0])
        # Corse angle
        # print(struct.unpack('h', gps_data[28:30]))
        corse_ang = struct.unpack('h', gps_data[28:30])
        gps_one_list.append(corse_ang[0])
        # Vspeed
        # print(struct.unpack('i', gps_data[30:34]))
        Vspeed = struct.unpack('i', gps_data[30:34])
        gps_one_list.append(Vspeed[0])
        # HDOP
        # print(struct.unpack('H', gps_data[34:36]))
        HDOP = struct.unpack('H', gps_data[34:36])
        gps_one_list.append(HDOP[0])
        # VDOP
        # print(struct.unpack('H', gps_data[36:38]))
        VDOP = struct.unpack('H', gps_data[36:38])
        gps_one_list.append(VDOP[0])
        # TDOP
        # print(struct.unpack('H', gps_data[38:40]))
        TDOP = struct.unpack('H', gps_data[38:40])
        gps_one_list.append(TDOP[0])
        # N of sat
        # print(struct.unpack('B', gps_data[40:41]))
        satnum = struct.unpack('B', gps_data[40:41])
        gps_one_list.append(satnum[0])
        # N of sat 03
        # print(struct.unpack('B', gps_data[41:42]))
        satnum03 = struct.unpack('B', gps_data[41:42])
        gps_one_list.append(satnum03[0])
        # Aver C/N0 03
        # print(struct.unpack('H', gps_data[42:44]))
        cno = struct.unpack('H', gps_data[42:44])
        gps_one_list.append(cno[0])

        # print(gps_one_list)
        self.gps_data.append(gps_one_list)

    # Fixmode
    #     # print(struct.unpack('B', gps_data[44:45]))

    def imu_parser(self, imu_data):
        imu_one_list = []
        # UTC
        # # print(struct.unpack('BBBB', imu_data[0:4]))
        utc = struct.unpack('BBBB', imu_data[0:4])
        utc_hms = str(utc[0]).zfill(2) + str(utc[1]).zfill(2) + str(utc[2]).zfill(2) + str(utc[3]).zfill(2)
        # print(utc_hms)

        # ACC X/Y/Z
        # print("acc X= ", struct.unpack('>h', imu_data[4:6]))
        # print("acc Y= ", struct.unpack('>h', imu_data[6:8]))
        # print("acc Z= ", struct.unpack('>h', imu_data[8:10]))
        accel = [struct.unpack('>h', imu_data[4:6])[0], struct.unpack('>h', imu_data[6:8])[0],
                 struct.unpack('>h', imu_data[8:10])[0]]
        # GYRO X/Y/Z
        # print("gyro X= ", struct.unpack('>h', imu_data[10:12]))
        # print("gyro Y= ", struct.unpack('>h', imu_data[12:14]))
        # print("gyro Z= ", struct.unpack('>h', imu_data[14:16]))
        gyro = [struct.unpack('>h', imu_data[10:12])[0], struct.unpack('>h', imu_data[12:14])[0],
                struct.unpack('>h', imu_data[14:16])[0]]
        # MAG X/Y/Z little endian
        # print("mag X= ", struct.unpack('<h', imu_data[16:18]))
        # print("mag Y= ", struct.unpack('<h', imu_data[18:20]))
        # print("mag Z= ", struct.unpack('<h', imu_data[20:22]))
        mag = [struct.unpack('<h', imu_data[16:18])[0], struct.unpack('<h', imu_data[18:20])[0],
               struct.unpack('<h', imu_data[20:22])[0]]

        imu_one_list.append(utc_hms)
        imu_one_list.append(accel)
        imu_one_list.append(gyro)
        imu_one_list.append(mag)

        self.imu_data.append(imu_one_list)
        # print("imu = ", len(self.imu_data))

    def bs_parser(self, bs_data):
        bs_one_data = []

        # date
        # print(bs_data)
        year = struct.unpack('H', bs_data[:2])
        # print(bs_data[:2])
        # print("Unpack data = ", bs_data)
        # print(self.int_to_bytes(bs_data[2]), self.int_to_bytes(bs_data[3]))
        month = struct.unpack('B', self.int_to_bytes(bs_data[2]))
        day = struct.unpack('B', self.int_to_bytes(bs_data[3]))

        yymmdd = str(year[0])[2:] + str(month[0]).zfill(2) + str(day[0]).zfill(2)
        # print(yymmdd)
        bs_one_data.append(yymmdd)

        # UTC
        # print(struct.unpack('BBBB', bs_data[4:8]))
        utc = struct.unpack('BBBB', bs_data[4:8])
        utc_hms = str(utc[0]).zfill(2) + str(utc[1]).zfill(2) + str(utc[2]).zfill(2) + str(utc[3]).zfill(2)
        # print(utc_hms)
        bs_one_data.append(utc_hms)
        # Operation time
        # print(struct.unpack('I', bs_data[8:12]))
        bs_one_data.append(struct.unpack('I', bs_data[8:12])[0])
        # HR
        # print(struct.unpack('H', bs_data[12:14]))
        bs_one_data.append(struct.unpack('H', bs_data[12:14])[0])
        # VBAT
        # print(struct.unpack('H', bs_data[14:16]))
        bs_one_data.append(struct.unpack('H', bs_data[14:16])[0])
        # Cell temperature
        # print(struct.unpack('H', bs_data[16:18]))
        bs_one_data.append(struct.unpack('H', bs_data[16:18])[0])
        # Reserve 1
        # print(struct.unpack('H', bs_data[18:20]))
        bs_one_data.append(struct.unpack('H', bs_data[18:20])[0])
        # Reserve 2
        # print(struct.unpack('H', bs_data[20:22]))
        bs_one_data.append(struct.unpack('H', bs_data[20:22])[0])
        # Reserve 3
        # print(struct.unpack('H', bs_data[22:24]))
        bs_one_data.append(struct.unpack('H', bs_data[20:22])[0])

        self.bs_data.append(bs_one_data)

    def time_verification_main(self, gps_utc, imu_utc, bs_utc):
        # print("Check UTC discontinuous !")
        # print("GPS UTC check")
        for i in range(0, len(gps_utc) - 1):
            if (self.convert_ms(gps_utc[i + 1]) - self.convert_ms(gps_utc[i])) != 10:
                print(float(gps_utc[i+1]), float(gps_utc[i]))
                print("GPS UTC error data is discontinuous")
        # print("IMU UTC check")
        for i in range(0, len(imu_utc) - 1):
            if (self.convert_ms(imu_utc[i+1]) - self.convert_ms(imu_utc[i])) != 1:
                print(float(imu_utc[i + 1]), float(imu_utc[i]))
                print("IMU UTC error data is discontinuous")
        # print("BS UTC check")
        for i in range(0, len(bs_utc) - 2):
            if (self.convert_ms(bs_utc[i + 2]) - self.convert_ms(bs_utc[i + 1])) != 100:
                print(float(bs_utc[i + 1]), float(bs_utc[i]))
                print("BS UTC error data is discontinuous")

    def verification_main(self, gps_data, imu_data, bs_data):
        gps_lla_list = []

        for z in range(0, len(gps_data)):
            # Only pass lla data from original data
            gps_lla_list.append(gps_data[z][2])

        x, y = dv.lla_to_ecef_list(gps_lla_list)
        dv.ECEF_2D_plotting_only_cell(x, y)

        dv.imu_3axis_plot(imu_data)

        # dv.bs_vbat_plot(self.bs_data)

    def main_parser(self, fpath):
        cell_info = {"hw_ver": "", "fw_ver": "", "serial": ""}
        imu_calibration_bias = []
        gps_lla_list = []
        imu_axis_plot_list = []

        # start_id = 11
        # end_id = 15
        #
        # hr_data = []
        # date_data = []
        line_cnt = 0
        with open(fpath, 'rb') as f:
            barr = f.read()
            # print('Total bytes = ', len(barr))
            # # print(barr[:1000])
            barr = barr.split(b'COACH')

            # print(barr[0])
            # print(barr[3].hex())
            start_head_line = barr[0][6:].decode()
            start_head_line = start_head_line.split(',')
            cell_info["hw_ver"] = start_head_line[2]
            cell_info["fw_ver"] = start_head_line[3]
            cell_info["serial"] = start_head_line[1]
            imu_calibration_bias = start_head_line[4:12]
            imu_calibration_bias.append(start_head_line[12][:-1])

            # print(imu_calibration_bias)
            # print(cell_info)

            # CRC check if True, parsing process execute
            # print("Start line by line parser")
            for i in range(1, len(barr)):
                # print(barr[i])
                # print(barr[i].hex(' '))
                crc_data = barr[i][:-3]
                crc_val = barr[i][-3]
                # print("crc data / crc val = ", type(crc_data), crc_data, type(crc_val), crc_val, self.int_to_bytes(crc_val))
                self.crc_flag = self.crc_check(crc_data, crc_val)

                if self.crc_flag:
                    # print("crc pass")
                    # print("data type =", barr[i][0])
                    data_type = barr[i][0]
                    if data_type == 1:
                        # print("GPS parsing")
                        self.gps_parser(barr[i][2:-3])
                    elif data_type == 2:
                        # print("IMU parsing")
                        self.imu_parser(barr[i][2:-3])
                    elif data_type == 3:
                        # print("BS parsing")
                        self.bs_parser(barr[i][2:-3])
                    else:
                        print("Not found data type.")

                else:
                    print("crc fail")
                    # print(barr[i])
                    self.crc_failed_cnt += 1

                # print("")
                # print(line_cnt)
                line_cnt += 1

            print("crc fail = ", self.crc_failed_cnt)

            for z in range(0, len(self.gps_data)):
                print(self.gps_data[z])
            for a in range(0, len(self.imu_data)):
                print(self.imu_data[a])
            for b in range(0, len(self.bs_data)):
                print(self.bs_data[b])

            gps_utc = ([i[1] for i in self.gps_data])
            imu_utc = ([i[0] for i in self.imu_data])
            bs_utc = ([i[1] for i in self.bs_data])

            self.time_verification_main(gps_utc, imu_utc, bs_utc)
            self.verification_main(self.gps_data, self.imu_data, self.bs_data)

cell_parse = Parser()
cell_parse.main_parser("/Users/jaeuklee/Downloads/CLBX_imu_data_test_234524534.gp")
