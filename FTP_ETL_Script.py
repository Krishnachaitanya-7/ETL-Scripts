#!/usr/bin/env python
# coding: utf-8

"""
FTP File Monitor - Template Script
Author: Krishna Chaitanya Myneni
Description: 
  Connects to an FTP server, checks latest modified files in dated plant directories,
  extracts timestamp and metadata, logs the information to a SQL Server table and local text log.
  This is a **template** for educational/demonstration purposes only.
"""

from ftplib import FTP, error_perm
from datetime import datetime, timedelta
import pyodbc
import os

# FTP credentials (placeholders)
ftp_server = 'ftp.example.com'          #Enter your FTP server 
ftp_user = 'your_ftp_username'          #Enter your FTP server username
ftp_password = 'your_ftp_password'      #Enter your FTP password

# Directory structure
base_directory = '/base/directory/path/'        #Enter your base directory where the plants directory is stored
plant_list_path = '/path/to/plant_names.txt'    #Enter the plant_name file stored in the above directory

# Read plant directories
with open(plant_list_path, 'r') as file:
    plant_directories = file.read().splitlines()

# Get current date components
current_date = datetime.now()
year = current_date.strftime('%Y')
month = current_date.strftime('%m')
day = current_date.strftime('%d')

# Helper to convert UTC to IST
def convert_to_ist(utc_time):
    return utc_time + timedelta(hours=5, minutes=30)

# Connect to FTP
ftp = FTP(ftp_server)
ftp.login(user=ftp_user, passwd=ftp_password)

# Connect to SQL Server (generic config)
cnxn = pyodbc.connect(
    "Driver={SQL Server};"          #Enter your SQL server
    "Server=your_sql_server;"       #Enter your SQL server port number
    "Database=your_database;"       #Enter your SQL server database   
    "UID=your_username;"            #Enter your SQL server user ID
    "PWD=your_password;"            #Enter your SQL server password
)
cursor = cnxn.cursor()

# Process each plant directory
for sub_dir in plant_directories:
    dynamic_path = sub_dir.format(year=year, month=month, day=day)
    full_path = os.path.join(base_directory, dynamic_path)

    try:
        print(f"Accessing FTP directory: {full_path}")
        ftp.cwd(full_path)
        files = ftp.nlst()

        if not files:
            raise error_perm("No files found")

        # Find most recently modified file
        latest_file = max(files, key=lambda f: ftp.sendcmd(f'MDTM {f}').split()[1])
        mod_time_str = ftp.sendcmd(f'MDTM {latest_file}').split()[1]
        mod_time = datetime.strptime(mod_time_str, "%Y%m%d%H%M%S")
        mod_time_ist = convert_to_ist(mod_time)

        # Extract metadata from filename
        parts = latest_file.split('_')
        plant_name = parts[0]
        timestamp_str = parts[2].replace('.csv', '')
        timestamp = datetime.strptime(timestamp_str, "%Y%m%d%H%M").strftime('%Y-%m-%d %H:%M')

        # Insert into SQL table
        cursor.execute('''
            INSERT INTO ftp_monitor_log (filename, ftp_mod_time, file_timestamp, plant)             
            VALUES (?, ?, ?, ?)
        ''', (latest_file, mod_time_ist.strftime('%Y-%m-%d %H:%M:%S'), timestamp, plant_name))      #Inserts data retrieved in the table created in SQL server
        cnxn.commit()

        # Append to local log file
        with open('ftp_monitor_log.txt', 'a') as log:       #Creates a log file for the data ingested into the table
            log.write(f"{plant_name} | {latest_file} | {mod_time_ist} | {timestamp}\n")

        print(f"{plant_name}: Logged {latest_file}")

    except error_perm as e:
        print(f"FTP Error in {full_path}: {e}")
    except Exception as e:
        print(f"Unexpected error in {full_path}: {e}")

# Cleanup
ftp.quit()
cursor.close()
cnxn.close()
