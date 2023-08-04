import pandas as pd
import os
import sqlite3
import re
import requests
from datetime import datetime, timedelta

# environment variables from api.boardgameatlas.com
atlas_client_id = ''

# environment variables from Twitch
client_id = ''
client_secret = ''

grant_type = 'client_credentials'

# Make a POST request to the Twitch API token endpoint
payload = {
    'client_id': client_id,
    'client_secret': client_secret,
    'grant_type': grant_type,
}

twitch_url = 'https://id.twitch.tv/oauth2/token'

def get_access_token():
    response = requests.post(twitch_url, data=payload)

    if response.status_code == 200:
        access_token = response.json()['access_token']
        return access_token
    else:
        print('Error:', response.status_code, response.text)

url = 'https://api.igdb.com/v4/games'

headers = {
        'Accept': 'application/json',
        'Client-ID': client_id,
        'Authorization': 'Bearer ' + get_access_token()}


# find CSV files directory
dataset_dir = 'csv'
location = os.listdir(dataset_dir)

# Isolate only the CSV files
csv_files = []

for file in location:
    if file.endswith('.csv'):
        csv_files.append(file)

data_path = os.getcwd()+'/'+dataset_dir+'/'
df = {}

offset = 0

for file in csv_files:
    try:
        df[file] = pd.read_csv(f'{data_path}{file}')
    except UnicodeDecodeError:
        df[file] = pd.read_csv(f'{data_path}{file}', encoding='ISO-8859-1')

for file in csv_files:
    data_frame = df[file]
    # Clean table names
    # lower case letters
    # remove all white spaces and $
    # replace -, /, \\, with _
    clean_tbl_name = file.lower().replace(" ", "_").replace("?", "") \
                        .replace("-", "_").replace(r"/","_").replace("\\", "_").replace("%", "") \
                        .replace(")", "").replace(r"(", "").replace("$","")
    
    #remove .csv from file extension
    tbl_name = '{0}'.format(clean_tbl_name.split('.')[0]).capitalize()

    # Clean header names
    # lower case letters
    # remove all white spaces and $
    # replace -, /, \\, with _
    def add_underscore_before_first_cap_or_num(input_string):
        outputstring = re.sub(r'(?=[A-Z0-9])', '_', input_string, 1)
        return outputstring
    
    data_frame.columns = [re.sub(r'(?<!^)(?=[A-Z0-9])', '_', x, 1).lower().replace(" ", "_").replace("?", "") \
                        .replace("-", "_").replace(r"/","_").replace("\\", "_").replace("%", "") \
                        .replace(")", "").replace(r"(", "").replace("$","") for x in data_frame.columns]
    
    data_frame.columns = [add_underscore_before_first_cap_or_num(x).replace("__", "_") for x in data_frame.columns]

    # Connect to the SQLite database
    # Can use a different connector to connect to SQL dbs like psycopg2
    # If locally find database.db file relative to this seedDB.py file
    conn = sqlite3.connect('../../GuildGamingFork/server/instance/database.db') # update this
    cursor = conn.cursor()

    if tbl_name == "Articles":
        data_frame.columns = [x.replace('writer', 'writer_id') for x in data_frame.columns]

        articles_table = ['id', 'title', 'description', 'body', 'published', 'thumbnail', 'date_published', 'date_updated', 'featured', 'writer_id']
        insert_sql = 'insert into ' + tbl_name + ' (' + ','.join(articles_table) + ') VALUES (' + ','.join(['?'] * len(articles_table))+ ')'
          
        featured = 0

        for row in data_frame.itertuples(index=False):
            cursor.execute("SELECT id FROM Venues ORDER BY id DESC LIMIT 1")
            table_result = cursor.fetchone() 

            thumbnail = ''

            if isinstance(row.thumbnail, str):
                thumbnail = row.thumbnail
            else:
                thumbnail = ''

            values = (row.id, row.title, row.description, row.body, row.published, thumbnail, row.date_published, row.date_updated, featured ,row.writer_id)

            cursor.execute(insert_sql, values)

        print(f'Successfuly pushed data to {tbl_name} table')
        conn.commit()
        conn.close()

    elif tbl_name == "Events":
        data_frame.columns = [x.replace('game_cover', 'featured_game_id').replace('datetime', 'start_time').replace('online_eventurl', 'online_event_url') for x in data_frame.columns]

        events_table = ['id', 'title', 'thumbnail', 'icon', 'game_type', 'frequency', 'registration_fee', 'pot', 'address_line_1', 'address_line_2', 'city', 'state', 'zip', 'online_event_url', 'status', 'description', 'created_time', 'start_time', 'end_time', 'latitude', 'longitude', 'archived', 'stream_link', 'featured', 'venue_id', 'featured_game_id']
        insert_event_sql = 'insert into ' + tbl_name + ' (' + ','.join(events_table) + ') VALUES (' + ','.join(['?'] * len(events_table))+ ')'

        found_id = 0

        for row in data_frame.itertuples(index=False):
            games_columns = ['id', 'title', 'cover_img_src', 'game_type', 'referenceId']
            tbl_columns = ['id int', 'title varchar', 'cover_img_src varchar', 'game_type varchar', 'referenceId varchar']

            cursor.execute("SELECT id FROM Games ORDER BY id DESC LIMIT 1")
            table_result = cursor.fetchone() 
            
            if table_result != None:
                game_id = table_result[0] + 1
            else: 
                game_id = 1

            thumbnail = ''
            
            if row.game_type == "Board Game":
                title = row.game
                board_game_url = f'https://api.boardgameatlas.com/api/search?name={title}&offset={offset}&limit=3&order_by=rank&ascending=false&client_id={atlas_client_id}'
                
                board_game_req = requests.get(board_game_url)
                board_game_response = board_game_req.json()
                
                try:
                    if board_game_response['games'] and len(board_game_response['games']) > 0:
                        b_game = board_game_response['games'][0]
                        reference_id = b_game['id']

                        cursor.execute("SELECT * FROM Games WHERE referenceId=?", (reference_id,))
                        reference_result = cursor.fetchone()

                        if reference_result:
                            found_id = reference_result[0]
                            thumbnail = reference_result[2]
                        else:
                            board_game = {
                                'reference_id': b_game['id'],
                                'cover_img_src': b_game['thumb_url'],
                                'game_type': "board_game",
                                'title': b_game['name']
                            }
                            values = [game_id, board_game['title'], board_game['cover_img_src'], board_game['game_type'],board_game['reference_id']]
                            thumbnail =  board_game['cover_img_src']

                            found_id = game_id

                            insert_sql = 'insert into ' + 'Games' + ' (' + ','.join(games_columns) + ') VALUES (' + ','.join(['?'] * len(games_columns))+ ')'
                            cursor.execute(insert_sql, values)
                            conn.commit()
                except KeyError:
                    pass
            else:
                game = row.game
                data = f'fields name, cover.url, screenshots.url; search "{game}"; where themes != (42); limit 3; offset {offset};'
                res = requests.post(url, headers=headers, data=data)
                full_response = res.json()

                try:
                    game_response = full_response[0]
                    reference_id = game_response['id']

                    cursor.execute("SELECT * FROM Games WHERE referenceId=?", (reference_id,))
                    reference_result = cursor.fetchone()

                    cover_url = ''
                    if 'cover' in game_response:
                        cover_url = game_response['cover']['url'].replace(
                                    't_thumb', 't_cover_big')
                    
                    screenshot_url = ''
                    if 'screenshots' in game_response and len(game_response['screenshots']) > 0:
                        screenshot_url = game_response['screenshots'][0]['url'].replace(
                            't_thumb', 't_original')

                    if reference_result:
                        found_id = reference_result[0]
                        thumbnail = reference_result[2]
                    else:
                        result = {
                                    'reference_id': game_response['id'],
                                    'title': game_response['name'],
                                    'cover_img_src': cover_url,
                                    'screenshot_img_src': screenshot_url,
                                    'game_type': 'video_game'
                                }
                        
                        values = [game_id, result['title'], cover_url, result['game_type'],result['reference_id']]
                        thumbnail = cover_url

                        found_id = game_id
                        
                        insert_sql = 'insert into ' + 'Games' + ' (' + ','.join(games_columns) + ') VALUES (' + ','.join(['?'] * len(games_columns))+ ')'
                        cursor.execute(insert_sql, values)
                        conn.commit()
                except KeyError:
                    pass

            stream_link = ''
            date = row.start_time
            date_time = datetime.strptime(date, '%Y-%m-%d %H:%M:%S')
            end_time = date_time + timedelta(hours=8)
            featured = 0
            
            values = (row.id, row.title, thumbnail, row.icon, row.game_type, row.frequency, row.registration_fee, row.pot, row.address_line_1, row.address_line_2, row.city, row.state, row.zip, row.online_event_url, row.status, row.description, row.start_time, row.start_time, end_time, row.latitude, row.longitude, row.archived, stream_link, featured, row.venue_id, found_id)

            cursor.execute(insert_event_sql, values)

        print(f'Successfuly pushed data to {tbl_name} table')
        conn.commit()
        conn.close()
    
    elif tbl_name == "Posts":
        data_frame.columns = [x.replace('post_date', 'date_published').replace('user_id', 'author_id') for x in data_frame.columns]
        
        posts_table = ['id',  'body', 'date_published', 'author_id']
        insert_sql = 'insert into ' + tbl_name + ' (' + ','.join(posts_table) + ') VALUES (' + ','.join(['?'] * len(posts_table))+ ')'

        for row in data_frame.itertuples(index=False):
            cursor.execute("SELECT id FROM Posts ORDER BY id DESC LIMIT 1")
            table_result = cursor.fetchone() 

            values = (row.id, row.body, row.date_published, row.author_id)

            cursor.execute(insert_sql, values)
    
        print(f'Successfuly pushed data to {tbl_name} table')
        conn.commit()
        conn.close()
        
    elif tbl_name == "Venues":
        data_frame.columns = [x.replace('owners', 'owners_id').replace('managers', 'managers_id').replace('bio', 'description') for x in data_frame.columns]

        venue_table = ['id', 'name', 'description', 'address_line_1', 'address_line_2', 'venue_type', 'city', 'icon', 'state', 'zip', 'latitude', 'longitude', 'thumbnail', 'event_space', 'logo', 'cap', 'venue_url', 'featured', 'phone_number', 'subregion_id']
        insert_sql = 'insert into ' + tbl_name + ' (' + ','.join(venue_table) + ') VALUES (' + ','.join(['?'] * len(venue_table))+ ')'

        for row in data_frame.itertuples(index=False):
            cursor.execute("SELECT id FROM Venues ORDER BY id DESC LIMIT 1")
            table_result = cursor.fetchone() 

            venue_id = 0
            featured = 0
            phone_number = ''
            subregion_id = 1
            description = ''

            if isinstance(row.description, str):
                description = row.description
            else:
                description = ''

            if table_result != None:
                venue_id = table_result[0] + 1
            else: 
                venue_id = 1

            values = (row.id, row.name, description, row.address_line_1, row.address_line_2, row.venue_type, row.city, row.icon, row.state, row.zip, row.latitude, row.longitude, row.thumbnail, row.event_space, row.logo, row.cap, row.venue_url, featured, phone_number, subregion_id)

            cursor.execute(insert_sql, values)

        print(f'Successfuly pushed data to {tbl_name} table')
        conn.commit()
        conn.close()