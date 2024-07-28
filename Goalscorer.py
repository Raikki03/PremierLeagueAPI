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
        cursor.execute("DELETE FROM top_scorers")
        print("Existing data deleted successfully.")
    except Error as e:
        print(f"Error deleting existing data: {e}")

def insert_top_goal_scorers(cursor, data):
    insert_query = """
        INSERT INTO top_scorers (`rank`, player, club, goals)
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        player = VALUES(player),
        club = VALUES(club),
        goals = VALUES(goals);
    """
    for row in data:
        cursor.execute(insert_query, (row['Rank'], row['Player'], row['Club'], row['Stat']))
    print("New data inserted successfully.")

def get_top_goal_scorers():
    url = "https://www.premierleague.com/stats/top/players/goals?se=578"
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
                top_scorers_data = get_top_goal_scorers()
                insert_top_goal_scorers(cursor, top_scorers_data)
                db_conn.commit() 
            except Exception as e:
                print("An error occurred:", e)
                db_conn.rollback()  
        db_conn.close() 
