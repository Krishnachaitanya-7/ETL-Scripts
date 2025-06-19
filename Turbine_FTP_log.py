#!/usr/bin/env python
# coding: utf-8

# In[1]:


from ftplib import FTP, error_perm
from datetime import datetime, timedelta
import pyodbc
import os

# FTP server credentials
ftp_server = 'your_ftp_server'       #Enter your FTP server 
ftp_user = 'your_ftp_username'       #Enter your FTP server username
ftp_password = 'your_ftp_password'   #Enter your FTP password

# Base directory
base_directory = 'Enter_your_base_directory'        #Enter your base directory where the plants directory is stored

# Directory where the FTP log files and subdirectory list are stored
log_directory = 'Enter_plants_directory_file_path'      #Enter the plants name file directory 

# Read subdirectories from plant_names.txt
with open(os.path.join(log_directory, 'plant_names.txt'), 'r') as file:     #Enter the plant_name file stored in the above directory
    subdirectories = file.read().splitlines()

# Function to get modification time of a file
def get_mod_time(ftp, filename):
    return ftp.sendcmd(f'MDTM {filename}').split()[1]

# Convert UTC time to IST
def convert_to_ist(utc_time):
    ist_time = utc_time + timedelta(hours=5, minutes=30)
    return ist_time

# Connect to the FTP server
ftp = FTP(ftp_server)
ftp.login(user=ftp_user, passwd=ftp_password)

# SQL Server connection
cnxn = pyodbc.connect(
    "Driver={SQL Server};"
    "Server=ENTER_YOUR_SERVER;"     #Enter your SQL server
    "PORT=ENTER_YOUR_PORT_NUMBER;"  #Enter your SQL server port number
    "Database=ENTER_YOUR_DATABASE;" #Enter your SQL server database   
    "UID=ENTER_YOUR_USERID;"        #Enter your SQL server user ID
    "PWD=ENTER_YOUT_PASSWORD;"      #Enter your SQL server password
)

# Cursor establishment
cursor = cnxn.cursor()

# Get current date
current_date = datetime.now()
year = current_date.strftime('%Y')
month = current_date.strftime('%m')
day = current_date.strftime('%d')

# Process each subdirectory and construct the full path
for sub_dir in subdirectories:
    directory = sub_dir.format(year=year, month=month, day=day)
    full_path = f"{base_directory}{directory}"
    try:
        print(f"Processing directory: {full_path}")
        ftp.cwd(full_path)

        # List files in the directory
        files = ftp.nlst()
        if not files:
            raise error_perm("No files in directory")

        # Find the latest file
        latest_file = None
        latest_time = None

        for file in files:
            mod_time_str = get_mod_time(ftp, file)
            mod_time = datetime.strptime(mod_time_str, "%Y%m%d%H%M%S")
            if latest_time is None or mod_time > latest_time:
                latest_time = mod_time
                latest_file = file

        # Extract the timestamp and plant name from the latest file's name
        if latest_file:
            # Assuming the format is PLANTNAME_ABT_YYYYMMDDHHMM.csv
            parts = latest_file.split('_')
            plant_name = parts[0]
            timestamp_str = parts[2].replace('.csv', '')
            latest_timestamp = datetime.strptime(timestamp_str, "%Y%m%d%H%M").strftime('%Y-%m-%d %H:%M')
        else:
            plant_name = None
            latest_timestamp = None

        # Convert modification time to IST
        if latest_time:
            latest_time_ist = convert_to_ist(latest_time)
        else:
            latest_time_ist = None

        # Log the latest timestamp, plant name, and Export (Mwh) into the SQL Server
        cursor.execute('''
            INSERT INTO YOUR_TABLE (latest_file, modification_time, latest_filetime, plant_name)       
            VALUES (?, ?, ?, ?)                                                                                 
        ''', (latest_file, latest_time_ist.strftime('%Y-%m-%d %H:%M:%S'), latest_timestamp, plant_name))        #Inserts data retrieved in the table created in SQL server
        cnxn.commit()

        # Log the latest timestamp, plant name, and Export (Mwh) into the log file
        with open(os.path.join(log_directory, 'turbine_ftp_log.txt'), 'a') as log:          #Creates a log file for the data ingested into the table
            log.write(f'Directory: {full_path}, Latest file: {latest_file}, Modification time (UTC): {latest_time}, Modification time (IST): {latest_time_ist}, Timestamp: {latest_timestamp}, Plant name: {plant_name}\n')

        # Print the latest file, its timestamp, plant name, and Export (Mwh)
        print(f'Latest file: {latest_file}')
        print(f'Modification time (UTC): {latest_time}')
        print(f'Modification time (IST): {latest_time_ist}')
        print(f'Timestamp: {latest_timestamp}')
        print(f'Plant name: {plant_name}')
    
    except error_perm as e:
        error_msg = f"Error processing directory {full_path}: {e}"
        with open(os.path.join(log_directory, 'turbine_ftp_log.txt'), 'a') as log:
            log.write(f'{error_msg}\n')
        print(error_msg)
    except Exception as e:
        error_msg = f"Unexpected error processing directory {full_path}: {e}"
        with open(os.path.join(log_directory, 'turbine_ftp_log.txt'), 'a') as log:
            log.write(f'{error_msg}\n')
        print(error_msg)

# Close the FTP connection
ftp.quit()

# Close the database connection
cursor.close()
cnxn.close()


# In[ ]:




