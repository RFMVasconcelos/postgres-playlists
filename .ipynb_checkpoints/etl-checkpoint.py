import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    - Opens and reads song files from the /data/song_data folder
    
    - Extracts & transforms song data, and loads it into the songs table 
    
    - Extracts & transforms artist data, and loads it into the artist table 
    """
    
    # open song file
    df = pd.read_json(filepath, typ='series')

    # insert song record
    song_data = df[["song_id", "title", "artist_id", "year", "duration"]].values.tolist()
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[["artist_id", "artist_name", "location", "latitude", "longitude"]].values.tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    
    """
    - Opens and reads log files from the /data/log_data folder
    
    - Filters logs by NextSong event
    
    - Converts timestamp to datetime and extracts time measures
    
    - Loads time data into the time table 
    
    - Extracts & transforms user data, and loads it into the users table 
    
    - Combines data extracted from log_data with existing song and artists table
    
    - Loads data into songplays table
    """
    
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df.loc[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    df['ts'] = t
    
    # mid-step
    timestamp = t.dt.time
    hour = t.dt.hour
    day = t.dt.day
    weekofyear = t.dt.weekofyear
    month = t.dt.month
    year = t.dt.year
    weekday = t.dt.weekday
    
    # insert time data records
    time_data = pd.concat([timestamp, hour, day, weekofyear, month, year, weekday], axis=1).values.tolist()
    column_labels = ('timestamp', 'hour', 'day', 'weekOfYear', 'month', 'year', 'weekday')
    time_df = pd.DataFrame.from_records(time_data, columns=column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[["userId", "firstName", "lastName", "gender", "level"]]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = [index, row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent]
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    
    """
    - gets all '*.json' files from path directory
    - gets total number of files
    - iterates over files in directory and processes them using given function 'func'
    
    """
    
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    
    """
    - Connects to postgresql db using psycopg2
    - Creates a cursor 
    - Calls in the `process_data` function to process song data via `process_song_file` function recursively
    - Calls in the `process_data` function to process log data via `process_log_file` function recursively
    """
    
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()