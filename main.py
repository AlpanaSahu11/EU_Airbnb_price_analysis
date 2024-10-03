import pandas as pd
import numpy as np
from dotenv import load_dotenv
import os
import mysql.connector
from sqlalchemy import create_engine
from mysql.connector import Error
from oauth2client.service_account import ServiceAccountCredentials
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from io import StringIO

load_dotenv()

scope = ["https://spreadsheets.google.com/feeds",'https://www.googleapis.com/auth/drive']
cred_filename = os.getenv("CRED_FILENAME")

# Authenticate and create the PyDrive client
def authenticate_gdrive(scope, cred_file):
    cred = ServiceAccountCredentials.from_json_keyfile_name(cred_file, scope)
    gauth = GoogleAuth()
    gauth.credentials = cred
    drive = GoogleDrive(gauth)
    return drive

# Function to read CSV file content from Google Drive into a Pandas DataFrame
def read_csv_from_drive(folder_id, drive):
    # List all files in the folder
    file_list = drive.ListFile({'q': f"'{folder_id}' in parents and trashed=false"}).GetList()
    # print(file_list)
    
    final_df = pd.DataFrame()
    for file in file_list:
        # Get the file's download URL
        test_file = drive.CreateFile({'id': file['id']})
        file_content = test_file.GetContentString()  # Get the file content as string, npt downloaded to local, rather stored as a local variable object
        df = pd.read_csv(StringIO(file_content))  # Read the content into a Pandas DataFrame
        df["City"] = file["title"].split("_")[0]
        df["day_flag"] = file["title"].split("_")[1].split(".")[0]
        
        print(f"Successfully read the file {file["title"]}")
        
        if final_df.empty:
            final_df = df
        else:
            final_df = pd.concat([final_df, df])
    
    return final_df

def create_connection(host_name, user_name, user_password):
    """ Create a database connection to the MySQL server """
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            password=user_password
        )
        print("Connection to MySQL DB successful")
    except Error as e:
        print(f"The error '{e}' occurred")
    return connection

def create_database(connection, db_name):
    """ Create a new database """
    cursor = connection.cursor()
    try:
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
        print(f"Database '{db_name}' created successfully")
    except Error as e:
        print(f"The error '{e}' occurred")

def create_table(connection, db_name):
    """ Create a new table in the specified database """
    cursor = connection.cursor()
    cursor.execute(f"USE {db_name}")  # Switch to the newly created database
    create_table_query = """
    CREATE TABLE IF NOT EXISTS airbnb_eu (
        id INT AUTO_INCREMENT PRIMARY KEY,
        realsum DOUBLE,
        room_type VARCHAR(20),
        room_shared BOOLEAN,
        room_private BOOLEAN,
        person_capacity INTEGER,
        host_is_superhost BOOLEAN,
        multi BOOLEAN,
        biz BOOLEAN,
        cleanliness_rating NUMERIC,
        guest_satisfaction_overall NUMERIC,
        bedrooms NUMERIC,
        dist NUMERIC,
        metro_dist NUMERIC,
        lng NUMERIC,
        lat NUMERIC,
        city VARCHAR(100) NOT NULL,
        day_flag VARCHAR(100) NOT NULL
    )
    """
    try:
        cursor.execute(create_table_query)
        print("Table 'airbnb_eu' created successfully")
    except Error as e:
        print(f"The error '{e}' occurred")


# Main execution
folder_id = os.getenv("FOLDER_ID")  # Add the folder ID where the CSV files are stored
    
# Authenticate and get the Google Drive client
drive = authenticate_gdrive(scope, cred_filename)

output_df = read_csv_from_drive(folder_id, drive)

output_df = output_df.drop(columns=["Unnamed: 0"], axis=1)

# Print the first few rows of the DataFrame
print(f"Final data: \n{output_df.head()}")

# Connection parameters
host = os.getenv("HOST")  # Change if you're connecting remotely
user = os.getenv("USER")  # Your MySQL username
password = os.getenv("PASSWORD")  # Your MySQL password
database = os.getenv("DATABASE")  # The database name you want to create

# Create connection
conn = create_connection(host, user, password)

if conn:
    # Create a new database
    create_database(conn, database)

    # Create a new table in the database
    create_table(conn, database)
    
    # Close the connection
    conn.close()

try:
    # Create the SQLAlchemy engine
    engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}/{database}")

    # Insert DataFrame into MySQL table
    output_df.to_sql('airbnb_eu', con=engine, if_exists='replace', index=False)

    print("Successfully wrote the data to SQL table")
except Error as e:
    print(f"Error while writing data to Sql\n{e}")

print("End")