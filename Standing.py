import requests
import pandas as pd
from bs4 import BeautifulSoup
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
        cursor.execute("DELETE FROM team_table")
        print("Existing data deleted successfully.")
    except Error as e:
        print(f"Error deleting existing data: {e}")

def insert_premier_league_standings(cursor, data):
    insert_query = """
        INSERT INTO team_table (`Rk`, `Squad`, `MP`, `W`, `D`, `L`, `GD`, `Pts`)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
    """
    for _, row in data.iterrows():
        cursor.execute(insert_query, (row['Rk'], row['Squad'], row['MP'], row['W'], row['D'], row['L'], row['GD'], row['Pts']))
    print("New Premier League standings inserted successfully.")

def get_premier_league_table():
    url = "https://fbref.com/en/comps/9/Premier-League-Stats"
    response = requests.get(url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
        table = soup.select_one("table.stats_table")
        df = pd.read_html(str(table))[0]
        df = df[['Rk', 'Squad', 'MP', 'W', 'D', 'L', 'Pts', 'GD']]
        return df
    else:
        raise Exception(f"Failed to retrieve data with status code: {response.status_code}")

if __name__ == "__main__":
    db_conn = connect_to_database()
    if db_conn is not None:
        with db_conn.cursor() as cursor:
            delete_existing_data(cursor)
            try:
                premier_league_data = get_premier_league_table()
                insert_premier_league_standings(cursor, premier_league_data)
                db_conn.commit() 
            except Exception as e:
                print("An error occurred:", e)
                db_conn.rollback()
        db_conn.close()
