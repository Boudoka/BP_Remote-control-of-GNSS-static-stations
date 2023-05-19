import os
import re
import serial
import logging
import pynmea2
import threading
import mysql.connector
from mysql.connector import Error
from sshtunnel import SSHTunnelForwarder
import time
import math
import sys
from ftplib import FTP
from rinex_conv import Convert2RinexAndSync

login = {
    'user': 'gnssegnosedas',
    'password': 'J****$*****4**',
    'host': '147.32.131.147',  # connection to ftp pi@k155rasp6.cvut.cz 147.32.131.147
    'database': 'gnssegnosedas',
   # 'charset': 'utf8mb3'
    }

try:
    connection = mysql.connector.connect(**login)
    if connection.is_connected():
        db_Info = connection.get_server_info()  # connection to database
        print("Connected to MySQL Server version ", db_Info)
        cursor = connection.cursor(prepared=True)  # prepared = True - means that query is parameterized, increases speed
        cursor.execute("select database();")
        record = cursor.fetchone()
        print("You're connected to database: ", record)

except Error as e:
    print("Error while connecting to MySQL", e)
    sys.exit()
print("CONNECTED ______________________________")

id_station = 1

logger = logging.getLogger(__name__)


class SerialNmeaRead(threading.Thread):
    global i_DB
    global id_station
    global gga
    global gsa
    global gst
    global zda

    '''
    The class with the method that reads the serial port in the backgroud.
    '''
    
    def __init__(self, directory: str, com_port: str, baudrate: int = 38400, ftp_acess: str = None, erase: bool = False, compress: bool = False):
        super().__init__()
        self._stop_event = threading.Event()
        self.directory = directory
        self.serial_object = serial.Serial(com_port, int(baudrate))
        self.ftp_acess = ftp_acess
        self.file_name = ""
        self.erase = erase
        self.compress = compress
   
    def define_file_name(self, ZDA_file_name: str):

        logging_dir = os.path.join("LOGS", self.directory)
        if not os.path.exists(logging_dir):
            try:
                os.makedirs(logging_dir)
            except OSError:
                logger.exception(
                    f"Creation of the directory {logging_dir} failed")
            else:
                logger.info(
                    f"Successfully created the directory {logging_dir}")

        actual_file_name = os.path.join(
            logging_dir, ZDA_file_name)

        actual_file_str = f"Actual logging file is {actual_file_name} ."

        if self.file_name == "":
            self.file_name = actual_file_name

            logger.info(actual_file_str)
            print(actual_file_str)

        elif self.file_name != actual_file_name:
            logger.info(actual_file_str)
            print(actual_file_str)

            old_file_name = self.file_name
            # update new name
            self.file_name = actual_file_name
            # convert *.ubx log to RINEX and synchronize data
            Convert2RinexAndSync(
                old_file_name, self.directory, self.ftp_acess, self.erase, [self.file_name], self.compress).start()
   
    def get_ZDA_timestamp(self, serial_data: str):

        match = re.search("\$GNZDA.*\*..", serial_data)

        if match:
            ZDA_message = serial_data[match.start():match.end()]
            ZDA_parse = pynmea2.parse(ZDA_message)

            # if develop True, files will be separated after 10 minutes
            BOOL_DEVELOP = False

            if BOOL_DEVELOP:
                ZDA_file_name = f"{ZDA_parse.year}_{ZDA_parse.month}_{ZDA_parse.day}_{str(ZDA_parse.timestamp)[0:2]}_{str(ZDA_parse.timestamp)[3]}0_00.ubx"
            else:
                ZDA_file_name = f"{ZDA_parse.year}_{ZDA_parse.month}_{ZDA_parse.day}_{str(ZDA_parse.timestamp)[0:2]}_00_00.ubx"

            self.define_file_name(ZDA_file_name)
    
    def get_GGA_timestamp(self, serial_data: str):

        match = re.search("\$GNGGA.*\*..", serial_data)

        if match:
            GGA_message = serial_data[match.start():match.end()]
            GGA_parse = pynmea2.parse(GGA_message)
            self.define_file_name(GGA_parse.timestamp)
    
    def run(self):
        i_DB = 0    
        
        
        '''
        The method that actually gets data from the port
        '''
        while not self.stopped():
            try:
                serial_data = self.serial_object.readline()
                
                self.get_ZDA_timestamp(
                    serial_data.decode("ascii", errors="replace"))

                if self.file_name != "":
                    # open file as append-binary
                    with open(self.file_name, "ab") as f:
                        f.write(serial_data)
                                          
                              
                def endcycle():  # functions that runs at the end of one loop
                    print("zacina endcycle")
                    #global i_DB
                    #i_DB = 0
                    #print(i_DB)
                    
               #     print("bezime")
                    if gga.lat_dir == "S":  # negative coordinates
                        lat = gga.lat * (-1)                                                                                           
#                     lat = gga.lat
#                     print(lat)
                    else:
                        lat = gga.lat
                        
                    DD = int(float(lat)/100)
                    SS = float(lat) - DD * 100 
                    lat = DD + SS/60
#                     lat = round(lat,8)
                    lat = "{:.8f}".format(lat)

                    
                    if gga.lon_dir == "W":
                        lon = gga.lon * (-1)
                    else:
                        lon = gga.lon
                    
                    DD = int(float(lon)/100)
                    SS = float(lon) - DD * 100 
                    lon = DD + SS/60
#                     lon = round(lon,8)
                    lon = "{:.9f}".format(lon)
                    
                        # all values are %s because query is parameterized
                    #print(lon)
                    main_table = ("INSERT INTO GNSS_static_station_control (ID_station, time, lat, lon, GPS_quality, SV_in_use, lat_error, lon_error, HDOP, PDOP, VDOP)"
                                  "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)")
                    main_tuple = (id_station, None, lat, lon, gga.gps_qual, gga.num_sats, gst.data[5], gst.data[6], gsa.data[15], gsa.data[14], gsa.data[16],)
                    
                    print(id_station, lat, lon, gga.gps_qual, gga.num_sats, gst.data[5], gst.data[6], gsa.data[15], gsa.data[14], gsa.data[16])
                    try:
                        
                        cursor.execute(main_table, main_tuple)
                        print("zapis")
                        connection.commit()
                        print("konci endcycle")
                    except Exception as err:
                        print(err)
#     
#     
                    
           # for t in range(121):  # replace with timer, import time
                #line = ser.readline()
                #print("nas port")
            
                line = serial_data                                                   
                #print("bezi\n")                    
                line = line.decode()                
#                 print(line)
                #line = line.decode(encoding= 'unicode_escape')
                #line = line.decode('unicode_escape', 'ignore')
                #print(line)
                line = line[:-2]                
#                     len(line) 
                #print(len(line))
                #if len(line) > 3:    
#                     if line[0] == "$":
                    #print(line[3:6])
#                     if line[3:5] == "TXT":  # GNTXT
#                 print(i)
#                 print(line)
                #print(i_DB)   # !!!!!!!!!!!!!!!!NIC TO NEVYPISE POKUD JE TENTO PRIKAZ PLATNY!!!!!!!!!!!!
              #  print("jsemtady")
                #ii_DB = i_DB
                #print(ii_DB)
                if line[3:6] == "GGA":  # GGA                       
                        
                   # print("GGA:",line, "IDDB",i_DB)
                    #print("bude iDB")
                    #print(i_DB)
                    print("GGA:",line)
                    
                    #gga = pynmea2.parse(line)
                    if i_DB == 1:  # second gga ends line in database and starts new one
                       
                        gga = pynmea2.parse(line)
                     #   print(gga.lat)
                        i_DB = i_DB + 1
                     #   print(i_DB) 
                    else:
                        gga = pynmea2.parse(line)
                        i_DB = i_DB + 1
                   
                elif line[3:6] == "GSA":  # GSA 
                    print(line)
                    gsa = pynmea2.parse(line)
                   # print(gsa.data[15])
                elif line[3:6] == "GST":  # GST
                    print(line)
                    gst = pynmea2.parse(line)
                    #print(gst)
                #elif line[4] == "L":  # GLL
                #    print(line)
                #    gll = pynmea2.parse(line)
                elif line[3:6] == "ZDA":  # ZDA
                    print(line)
                    zda = pynmea2.parse(line)
                   # print(zda)
#                     else:pass
                    #else:pass #print("ha")
                
                    
                else:pass
#                     #print("t=",t)
                endcycle()
                time.sleep(60)
                #except Exception:
                
                #    logger.exception(f"Some error in data: {serial_data}")
                
            except Exception:
              logger.exception(f"Some error in data: {serial_data}")
              
            # print("zkouska\n")


    def stop(self):
        self._stop_event.set()
        self.serial_object.close()

    def stopped(self):
        return self._stop_event.is_set()
