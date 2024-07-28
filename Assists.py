import requests
import pandas as pd
from bs4 import BeautifulSoup
import json
import mysql.connector
from mysql.connector import Error

def connect_to_database():
    try:
        connection = mysql.connector.connect(
            ... 
        )
        if connection.is_connected():
            print("Database connection established.")
        return connection
    except Error as e:
        print(f"Error connecting to MySQL database: {e}")
        return None

def delete_existing_data(cursor):
    try:
        cursor.execute("DELETE FROM top_assisters")
        print("Existing data deleted successfully.")
    except Error as e:
        print(f"Error deleting existing data: {e}")

def insert_top_assisters(cursor, data):
    insert_query = """
        INSERT INTO top_assisters (`rank`, player, club, assists)
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        player = VALUES(player),
        club = VALUES(club),
        assists = VALUES(assists);
    """
    for row in data:
        cursor.execute(insert_query, (row['Rank'], row['Player'], row['Club'], row['Stat']))
    print("New data inserted successfully.")

def get_top_assisters():
    url = "https://www.premierleague.com/stats/top/players/goal_assist" 
    response = requests.get(url)
    if response.status_code == 200:
        tables = pd.read_html(response.text)
        if tables:
            df = tables[0]
            df = df[['Rank', 'Player', 'Club', 'Nationality', 'Stat']]
            return df.to_dict(orient='records')
        else:
            raise ValueError("No tables found on the page")
    else:
        raise Exception("Failed to retrieve data, status code:", response.status_code)

if __name__ == "__main__":
    db_conn = connect_to_database()
    if db_conn is not None:
        with db_conn.cursor() as cursor:
            delete_existing_data(cursor)
            try:
                top_assisters_data = get_top_assisters()
                insert_top_assisters(cursor, top_assisters_data)
                db_conn.commit() 
            except Exception as e:
                print("An error occurred:", e)
                db_conn.rollback()
        db_conn.close() 
