import os
import pandas as pd
import mysql.connector as mysql
from mysql.connector import Error


def create_connection():
    '''Create and return connection to MySQL database.'''
    try:
        mydb = mysql.connect(
            host="localhost",
            user="root",
            password="MySQLCommunityPassword",
            database="movieCollections" 
        )
        if mydb.is_connected():
            print("Connection successful")
            return mydb
    except Error as err:
        print('Error: {err}')
        return None



def create_new_table(cursor):
    '''Create 'movies' table if it doesn't exist'''
    create_table_query = """
    CREATE TABLE IF NOT EXISTS movies(
        id INT AUTO_INCREMENT PRIMARY KEY, 
        title VARCHAR(255),
        release year INT, 
        ...
    )
    """
    cursor.execute(create_table_query)



def insert_movies_from_csv(file_path, cursor, mydb):
    '''
    Read in the csv files
    Args: 
        file_path (str): Path to CSV file
        cursor (MySQLCursor)L MySQL cursor object
        db_connection (MySQLConnection): MySQL database connection object
    '''
    try:
        df = pd.read_csv(file_path)
        for _, row in df.iterrows():
            sql = """
                INSERT INTO movies (title, release_year, ....)
                VALUES (%s, %f ,....
            """
            val = (row['title'], row['release_year', ...])
            cursor.execute(sql, val)
            print(f"Data from {file_path} inserted successfully.")
    except Error as err:
        print(f"Error inserting data from {file_path}: {err}")


def main():
    '''Main function to process and insert movie data from CSV files'''
    # Establish database connection
    connection = create_connection()
    if connection is None:
        return
    # create database if it doesn't exist
    cursor = connection.cursor()
    cursor.execute("CREATE DATABASE IF NOT EXISTS movieCollections")
    cursor.execute("USE movieCollections")
    # create new table in database
    create_new_table(cursor)

    # Directory containing csv files
    folder_path='../data'

    # read in file for year ranges
    for year in range(2010, 2024):
        file_path = os.path.join(folder_path, f"{year}Movies.csv")

        if os.path.isfile(file_path):
            insert_movies_from_csv(file_path)
        else:
            print(f"File not found: {file_path}")

    print("Data insertion complete. Proceeding to analysis...")

    # close cursor and database connection
    cursor.close()
    connection.close()
    print("Database connection closed")



if __name__ == '__main__':
    main()