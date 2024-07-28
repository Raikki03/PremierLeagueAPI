import json
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import mysql.connector
from mysql.connector import Error
from datetime import timedelta


def connect_to_database():
    try:
        connection = mysql.connector.connect(
            ...
        )
        return connection
    except Error as e:
        print(f"Error connecting to MySQL database: {e}")
        return None

def last_fixture(db_connection):
    cursor = db_connection.cursor()
    query = """
    SELECT match_date FROM fixtures
    ORDER BY match_date DESC
    LIMIT 1
    """
    cursor.execute(query)
    result = cursor.fetchone()
    cursor.close()
    return result[0] if result else None




def scrape_officials(match_report_url):
    try:
        response = requests.get(match_report_url, timeout=5)  
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            officials_container = soup.find(lambda tag: tag.name == "div" and "Officials" in tag.text)
            if officials_container:
                officials = {}
                for official in officials_container.find_all('span', style="display:inline-block"):
                    official_text = official.get_text(strip=True)
                    if 'Referee' in official_text:
                        officials["on_field"] = official_text.replace(" (Referee)", "")
                    elif 'VAR' in official_text:
                        officials["var"] = official_text.replace(" (VAR)", "")
                return officials
    except requests.RequestException:
        pass
    return {"on_field": "Error", "var": "Error"}  

def fixtures(url, last_date):
    response = requests.get(url)
    if response.status_code != 200:
        print(f"Failed to fetch the page. Status code: {response.status_code}")
        return []

    soup = BeautifulSoup(response.content, 'html.parser')
    fixtures_data = []

    rows = soup.find_all('tr')
    for row in rows:
        date_element = row.find('td', {'data-stat': 'date'})
        if date_element:
            match_date_str = date_element.text.strip()
            if match_date_str:
                match_date = datetime.strptime(match_date_str, '%Y-%m-%d').date()
                if last_date and match_date <= last_date:
                    continue  

                extract(row, fixtures_data, match_date_str)
    return fixtures_data

def extract(row, fixtures_data, match_date_str):
    gameweek_element = row.find('th', {'data-stat': 'gameweek'})
    home_team_element = row.find('td', {'data-stat': 'home_team'})
    away_team_element = row.find('td', {'data-stat': 'away_team'})
    score_element = row.find('td', {'data-stat': 'score'})
    match_report_link_tag = row.find('td', {'data-stat': 'match_report'}).find('a') if row.find('td', {'data-stat': 'match_report'}) else None

    if all([gameweek_element, home_team_element, away_team_element, score_element]) and match_report_link_tag:
        match_report_link = 'https://fbref.com' + match_report_link_tag['href']
        officials = scrape_officials(match_report_link)

        fixture_data = {
            "match_date": match_date_str,
            "matchweek": int(gameweek_element.text.strip()),
            "home_team": home_team_element.text.strip(),
            "away_team": away_team_element.text.strip(),
            "score": score_element.text.strip(),
            "officials": officials
        }
        fixtures_data.append(fixture_data)

def insert_fixture(db_connection, fixture_data):
    cursor = db_connection.cursor()

    cursor.execute("""
        INSERT INTO fixtures (match_date, matchweek, home_team, away_team, score, VAR, Onfield)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (fixture_data['match_date'], fixture_data['matchweek'], fixture_data['home_team'], fixture_data['away_team'], fixture_data['score'], fixture_data['officials'].get('var', 'Error'), fixture_data['officials'].get('on_field', 'Error')))
    db_connection.commit()

    if 'Error' in [fixture_data['officials'].get('var'), fixture_data['officials'].get('on_field')]:
        cursor.execute("""
            DELETE FROM fixtures
            WHERE home_team = %s AND away_team = %s AND match_date = %s AND (VAR = 'Error' OR Onfield = 'Error')
            """, (fixture_data['home_team'], fixture_data['away_team'], fixture_data['match_date']))
        db_connection.commit()

    cursor.close()



def main():
    db_connection = connect_to_database()
    if not db_connection:
        return

    last_fixture_date = last_fixture(db_connection)
    if last_fixture_date is not None:
        start_date = last_fixture_date + timedelta(days=1)
    else:
        start_date = None

    fixtures_data = fixtures('https://fbref.com/en/comps/9/schedule/Premier-League-Scores-and-Fixtures', start_date)
    for fixture_data in fixtures_data:
        insert_fixture(db_connection, fixture_data)

    db_connection.close()
    print(f"Started scraping from date: {start_date}")
    print("Fixture data update complete.")




if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        error_response = {"error": str(e)}
        print(json.dumps(error_response))
