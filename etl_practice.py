import pandas as pd
import pyodbc
import os
from datetime import date, datetime
import sys
import math
import shutil
from requests.api import post

execution_start_obj = datetime.now()
execution_start = execution_start_obj.strftime('%Y-%m-%d %H:%M:%S')



# ====================  DEFINE PARAMETERS ==========================
#dir_path = "C:/Users/user-sql/file_etl/" # full path to files
etl_history = "etl_log"
path = dir_path+'import/' 
sql_path = dir_path+'sql/' # directly to hold sql scripts
archive_path = path+'archive/' # old import files will be stored here

# each of the files in the list below will be executed in the SQL database
sql_filenames = ['daily_1.sql',
                'daily_2.sql',
                'daily_3.sql']


# these strings are pull from the database
# format the string as a SQL query
campaign_sql_query = """SELECT [Person ID], [Email Address], [CRM ID]
        ,[Campaign Info]
        FROM etl_campaign;"""


# the dictionary below includes the data map names that will be used when importing to platform B.
# the key must be in the file name that is exported to csv. The program will match the data map based on the key value
data_maps = {'bio':'etl_bio',
            'campaign':'etl_campaign'}


# This secton stores the tokens and urls for each Bulk API service
prod_token = 'abcdeghifkj'
prod_url = 'https://us.platformB.com/vvvbbb'
reference_name = 'imp_platformB' # name used in file imports



# sql server database connections
sqldatabase = pyodbc.connect('Driver={SQL Server};'
                        'Server=sqldatabase;'
                        'Database=platformA;'
                        'Trusted_Connection=yes;')
cursor = sqldatabase.cursor()

sqltestbase = pyodbc.connect('Driver={SQL Server};'
                        'Server=sqltestbase;'
                        'Database=platformA;'
                        'Trusted_Connection=yes;')
sqltestbase_cursor = sqltestbase.cursor()

# this function will be called when error messages arise and are posted
def post_error_message(database_cursor, table, start_date, message):
    database_cursor.execute(f"""
    INSERT INTO dbo.{table} (error_details, execution_start)
    VALUES ('{message}','{start_date}');
    """)
    database_cursor.commit()
    database_cursor.close()


# get todays date
today = date.today().strftime('%Y%m%d')

# ============= EXECUTE VALIDATION CHECKS TO CONFIRM THE PROCESS SHOULD CONTINUE =====================

# get the last successful run date and format as date object
last_run = pd.read_sql_query(f"SELECT TOP 1 CONVERT(DATE,execution_start) last_run_date FROM {etl_history} WHERE error_details IS NULL ORDER BY execution_start DESC;", sqldev10)
last_run_date = last_run.iloc[0]['last_run_date'] # extract date values
last_run_date = datetime.strptime(last_run_date, "%Y-%m-%d").date() # convert to date object with only date

# post error message if the etl process already ran successfully today
if last_run_date == date.today():
    error_message = f"""The etl process has already run successfully today.
    To re-run this process, delete the row for today from {etl_history} table in sqldev10 or insert an message into the error details column.
    """
    post_error_message(sqltestbase_cursor, etl_history, execution_start, error_message)
    raise Exception (error_message)


# move any old files in folder to the archive directory
for entry in os.listdir(path):
    if os.path.isfile(os.path.join(path, entry)):
        try:
            shutil.move(path+entry,archive_path+entry)
        except OSError as e:
            print(f"Error {entry}:{e.strerror}")
