

A lightweight Python-based ETL pipeline that connects to an FTP server, checks for the latest files in plant directories, extracts timestamps and metadata, and logs the information to a SQL database and text file.

> This is a **generalized version** of a production system and is meant for **educational or demonstration** purposes only.

---

## Features

- Extracts metadata from FTP directories
- Transforms timestamps and file structure
- Loads data into a SQL Server table and local log
- Converts UTC timestamps to IST
- Logs file activity per directory
- Reads dynamic subdirectories from an external list

---

## 🏗️ ETL Flow


