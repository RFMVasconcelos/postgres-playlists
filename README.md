# Sparkify DB creation + ETL Pipeline

This repository contains the necessary scripts to create a PostgreSQL DB for a music streaming app (Sparkify), with a performant architecture, as well as the scripts to extract data from 2 sets of files and load it to the DB.

## 0. Context

In order to enable performant queries to be done to the database, a star-schema composed of 5 tables was used: users, songs, artists, time, songplays. 

The startup has put up systems that collect 2 types of data:

- Songs 
- Activity Logs

Through the scripts developed in this repo, data is extracted from 2 groups of files and loaded into the 5 tables of the DB. 

With the data cleanly placed into the 5 tables, the analytics team can now easily create dashboards which focus on the different areas of the business:

1. Acquiring and retaining users
2. Increase the songs catalog
3. Acquire more artists
4. Engage users daily

which correspond to 4 independent tables, while minimizing JOINS. The time table is used when time granularity is needed in the query. 

Advanced queries & usecases like recommending songs to a user, based on listening history can be performed with easily with the `songplays` table.

## 1. Running

In order to create the tables run:

```
python create-tables.py
```

To load the data from the songs and activity logs run:

```
python etl.py
```

## 2. Directory Structure

* /data - folder containing all the data
    * /data/log_data - Activity Logs
    * /data/song_data - Activity Logs
* create_tables.py - script to create DB + tables
* etl.py - script to extract data from /data/log_data and /data/song_data and load it to the DB
* sql_queries.py - helper script with all SQL queries
* etl.ipynb - Jupyter notebook with etl process - for development and converted into etl.py
* test.ipynb - Jupyter notebook for testing
* Readme.md
