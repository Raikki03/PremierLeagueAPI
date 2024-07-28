import requests
import pandas as pd
from bs4 import BeautifulSoup
import mysql.connector
import numpy as np
import sys

if sys.stdout.encoding != 'utf-8':
    sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf-8', buffering=1)

url_team_ids = [
    '18bb7c10', '8602292d', '4ba7cbea', 'cd051869', 'd07537b9', '943e8050',
    'cff3d9bb', '47c64c55', 'd3fd31cc', 'fd962109', '822bd0ba', 'e297cd13',
    'b8fd03ef', '19538871', 'b2b47a98', 'e4a775cb', '1df6b87e', '361ca564',
    '7c21e445', '8cec06e1'
]  


db_config = {
    ...
}

def create_db_connection():
    try:
        connection = mysql.connector.connect(**db_config)
        print("Database connection established.")
        return connection
    except mysql.connector.Error as error:
        print(f"Failed to connect to database: {error}")
        return None

def clear_table_data(connection, table_name):
    cursor = connection.cursor()
    cursor.execute(f"DELETE FROM {table_name};")
    connection.commit()
    cursor.close()
    print(f"All data cleared from {table_name}.")

def insert_team_data(connection, team_data, team_id):
    cursor = connection.cursor()
    
    insert_query = """
    INSERT INTO player_stats (
        player_name, nation, position, age, matches_played, starts, minutes_played,
        minutes_per_90, goals, assists, goals_plus_assists, goals_non_penalty,
        penalty_kicks_made, penalty_kicks_attempted, yellow_cards, red_cards,
        expected_goals, non_penalty_expected_goals, expected_assists_goals, npxg_plus_xa,
        progressive_carries, progressive_passes, progressive_passes_received,
        goals_per_90, assists_per_90, goals_plus_assists_per_90,
        goals_non_penalty_per_90, goals_plus_assists_non_penalty_per_90,
        expected_goals_per_90, expected_assists_goals_per_90,
        expected_goals_plus_assists_per_90, non_penalty_expected_goals_per_90,
        npxg_plus_xa_per_90, team_id
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """
    for record in team_data:
        values = tuple(record.get(column) for column in [
            'player_name', 'nation', 'position', 'age', 'matches_played', 'starts', 'minutes_played',
            'minutes_per_90', 'goals', 'assists', 'goals_plus_assists', 'goals_non_penalty',
            'penalty_kicks_made', 'penalty_kicks_attempted', 'yellow_cards', 'red_cards',
            'expected_goals', 'non_penalty_expected_goals', 'expected_assists_goals', 'npxg_plus_xa',
            'progressive_carries', 'progressive_passes', 'progressive_passes_received',
            'goals_per_90', 'assists_per_90', 'goals_plus_assists_per_90',
            'goals_non_penalty_per_90', 'goals_plus_assists_non_penalty_per_90',
            'expected_goals_per_90', 'expected_assists_goals_per_90',
            'expected_goals_plus_assists_per_90', 'non_penalty_expected_goals_per_90',
            'npxg_plus_xa_per_90'
        ]) + (team_id,)
        print(f"Expected {insert_query.count('%s')} parameters; got {len(values)} values.")
        
        if insert_query.count('%s') != len(values):
            print("Mismatch in the number of SQL parameters and provided values!")
            continue 

        cursor.execute(insert_query, values)
    
    connection.commit()
    cursor.close()

def scrape_team_stats(team_id):
    url = f"https://fbref.com/en/squads/{team_id}/Stats"
    response = requests.get(url)
     
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')
        for over_header in soup.find_all("tr", class_="over_header"):
            over_header.decompose()
        table = soup.find('table')
        if table:
            df = pd.read_html(str(table))[0]
            print("DataFrame before transformation:")
            print(df.head())  
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(1)
            transformed_data = transform_data(df)
            print("Dictionary of records after transformation:")
            print(transformed_data[:5])  
            return transformed_data  
        else:
            print("No table found on the page.")
            return []
    else:
        print(f"Failed to retrieve data for URL: {url}")
        return []


def transform_data(df):
    
    rename_map = {
        'Player': 'player_name',
        'Nation': 'nation',
        'Pos': 'position',
        'Age': 'age',
        'MP': 'matches_played',
        'Starts': 'starts',
        'Min': 'minutes_played',
        '90s': 'minutes_per_90',
        'Gls': 'goals',
        'Ast': 'assists',
        'G+A': 'goals_plus_assists',
        'G-PK': 'goals_non_penalty',
        'PK': 'penalty_kicks_made',
        'PKatt': 'penalty_kicks_attempted',
        'CrdY': 'yellow_cards',
        'CrdR': 'red_cards',
        'xG': 'expected_goals',
        'npxG': 'non_penalty_expected_goals',
        'xAG': 'expected_assists_goals',
        'npxG+xAG': 'npxg_plus_xa',
        'PrgC': 'progressive_carries',
        'PrgP': 'progressive_passes',
        'PrgR': 'progressive_passes_received',
        'Gls.1': 'goals_per_90',
        'Ast.1': 'assists_per_90',
        'G+A.1': 'goals_plus_assists_per_90',
        'G-PK.1': 'goals_non_penalty_per_90',
        'G+A-PK': 'goals_plus_assists_non_penalty_per_90',
        'xG.1': 'expected_goals_per_90',
        'xAG.1': 'expected_assists_goals_per_90',
        'xG+xAG': 'expected_goals_plus_assists_per_90',
        'npxG.1': 'non_penalty_expected_goals_per_90',
        'npxG+xAG.1': 'npxg_plus_xa_per_90'
    }

    df.rename(columns=rename_map, inplace=True)
    df.replace({np.nan: None}, inplace=True)  
    df = df.where(pd.notnull(df), None)
    df = df[(df['player_name'] != 'Squad Total') & (df['player_name'] != 'Opponent Total') & (df['player_name'].notna())]

    df['nation'] = df['nation'].str.extract(r'([A-Z]+)', expand=False)
  
    return df.to_dict('records')

def main():
    db_conn = create_db_connection()
    if db_conn:
        clear_table_data(db_conn, "player_stats")

        team_ids_mapping = {url_id: db_id for db_id, url_id in enumerate(url_team_ids, start=1)}

        for url_team_id in url_team_ids:
            print(f"Processing team with ID: {url_team_id}...")
            team_data = scrape_team_stats(url_team_id)

            if team_data:
                db_team_id = team_ids_mapping[url_team_id]

                insert_team_data(db_conn, team_data, db_team_id)
                print(f"Data inserted successfully for team ID: {url_team_id}.")
            else:
                print(f"No data to insert for team ID: {url_team_id}.")

        db_conn.close()
    else:
        print("Failed to establish database connection.")

if __name__ == "__main__":
    main()
