import mysql.connector as conn
from mysql.connector.connection import MySQLConnection
from mysql.connector.cursor import MySQLCursor
import sys
import os
from dotenv import load_dotenv
from src.scraper.exception import CustomException
from src.scraper.logger import logging
import pandas as pd


def configure():
    load_dotenv()


configure()
host = os.getenv("database_host_name")
user = os.getenv("database_user_name")
password = os.getenv("database_user_password")
database = os.getenv("database_name")


def create_database(host:str ,user:str, password:str) -> bool:
    """
    This function connects to a MySQL server using the provided credentials,
    and then attempts to create a database named 'linkedin'. If the database
    already exists, no changes are made.

    Args:
        host (str): The hostname of the MySQL server.
        user (str): The username used to authenticate with the MySQL server.
        password (str): The password used to authenticate with the MySQL server.
    """
    try:
        mydb = conn.connect(host=host, user=user, password=password)
        cursor = mydb.cursor()
        cursor.execute("CREATE DATABASE IF NOT EXISTS webscraper")
        logging.info("Database created successfully")
        return True
    except Exception as e:
        logging.error(f"An error occurred while creating database: {e}")
        raise CustomException(e, sys)


def connect_to_mysql_database(host:str, user:str, password:str, database:str) -> MySQLConnection:
    """
    Connects to MySQL database using provided credentials.
    If the connection is successful, it returns the MySQLConnection object.

    Args:
        host (str): The hostname or IP address of the MySQL server.
        user (str): The username used to authenticate with the MySQL server.
        password (str): The password used to authenticate with the MySQL server.
        database (str): The name of the database to connect to.

    Returns:
        MySQLConnection: A connection object to interact with the MySQL database.
    """
    try:
        mydb = conn.connect(host=host, user=user, password=password, database=database)
        logging.info("Connected to MySQL successfully!")
        return mydb
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise CustomException(e,sys)
    

def create_cursor_object(mydb: MySQLConnection) -> MySQLCursor:
    """
    Creates a cursor object from a MySQLConnection object.
    The cursor object allows for the execution of SQL queries

    Args:
        mydb (MySQLConnection): The MySQLConnection object to obtain the cursor from.

    Returns:
        MySQLCursor: A cursor object for interacting with the MySQL database.
    """
    try:
        cursor = mydb.cursor()
        logging.info("Cursor object obtained successfully!")
        return cursor
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise CustomException(e,sys)


def create_tables(host: str, user: str, password: str, database: str) -> bool:
    """
    Creates tables in the specified MySQL database if they do not already exist.

    Args:
        host (str): The hostname or IP address of the MySQL server.
        user (str): The username used to authenticate with the MySQL server.
        password (str): The password used to authenticate with the MySQL server.
        database (str): The name of the database to connect to.
    Returns:
        bool: True if the tables were created successfully.   
    """
    try:
        mydb = connect_to_mysql_database(host, user, password, database)
        cursor = create_cursor_object(mydb)
        table_queries = [
            """
            CREATE TABLE IF NOT EXISTS LinkedinJobData(
                ID INT AUTO_INCREMENT PRIMARY KEY,
                Date DATE,
                Time TIME,
                JobTitle VARCHAR(255),
                CompanyName VARCHAR(255),
                JobLocation VARCHAR(255),
                JobURL VARCHAR(255)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS LinkedinUniqueJobData(
                ID INT AUTO_INCREMENT PRIMARY KEY,
                Date DATE,
                Time TIME,
                JobTitle VARCHAR(255),
                CompanyName VARCHAR(255),
                JobLocation VARCHAR(255),
                JobURL VARCHAR(255)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS DiceJobData(
                ID INT AUTO_INCREMENT PRIMARY KEY,
                Date DATE,
                Time TIME,
                CompanyName VARCHAR(255),
                JobTitile VARCHAR(255),
                JobLocation VARCHAR(255),
                CompanyURL VARCHAR(255)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS DiceUniqueJobData(
                ID INT AUTO_INCREMENT PRIMARY KEY,
                Date DATE,
                Time TIME,
                CompanyName VARCHAR(255),
                JobTitile VARCHAR(255),
                JobLocation VARCHAR(255),
                CompanyURL VARCHAR(255)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS NaukriJobData(
                ID INT AUTO_INCREMENT PRIMARY KEY,
                Date DATE,
                Time TIME,
                JobTitle VARCHAR(255),
                CompanyName VARCHAR(255),
                JobLocation VARCHAR(255),
                JobURL VARCHAR(255)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS NaukriUniqueJobData(
                ID INT AUTO_INCREMENT PRIMARY KEY,
                Date DATE,
                Time TIME,
                JobTitle VARCHAR(255),
                CompanyName VARCHAR(255),
                JobLocation VARCHAR(255),
                JobURL VARCHAR(255)
            )
            """,
        ]

        # Execute table creation queries
        for query in table_queries:
            cursor.execute(query)

        logging.info("Tables and columns created successfully")
        return True
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise CustomException(e, sys)


if __name__ == "__main__":
    create_database(host, user, password)
    create_tables(host, user, password, database)
