import os
import mysql.connector
from src.main.utility.logging_config import logger


def get_mysql_connection():
    """
    Creates and returns a MySQL connection using environment variables.

    Required environment variables:
    - MYSQL_HOST
    - MYSQL_USER
    - MYSQL_PASSWORD
    - MYSQL_DATABASE
    """

    try:
        connection = mysql.connector.connect(
            host=os.getenv("MYSQL_HOST", "localhost"),
            user=os.getenv("MYSQL_USER"),
            password=os.getenv("MYSQL_PASSWORD"),
            database=os.getenv("MYSQL_DATABASE"),
        )

        logger.info("MySQL connection established successfully")
        return connection

    except mysql.connector.Error as err:
        logger.error("Failed to connect to MySQL: %s", err)
        raise

















# connection = mysql.connector.connect(
#     host="localhost",
#     user="root",
#     password="password",
#     database="manish"
# )
#
# # Check if the connection is successful
# if connection.is_connected():
#     print("Connected to MySQL database")
#
# cursor = connection.cursor()
#
# # Execute a SQL query
# query = "SELECT * FROM manish.testing"
# cursor.execute(query)
#
# # Fetch and print the results
# for row in cursor.fetchall():
#     print(row)
#
# # Close the cursor
# cursor.close()
#
# connection.close()
