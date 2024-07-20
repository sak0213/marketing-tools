Repo for holding all ELT code for campaigns.

Process for each platform:
1. Query DB for missing data then build series of Jobs
2. (Optional) Check status of Job if asynchronous. If failed, resubmit.
3. Retreieve query data from API and send to DB for staging
4. SQL Procedures parse and insert
5. Procedures clear staging queues and build relevant production tables for reporting

General setup for each platform:
 - SQL
     Schema & Table creation
     Triggers
     Procedures
 - creds.py - DB & API credentials
 - config.py - DB & API settings

 Added Some popular scripts for analysis
