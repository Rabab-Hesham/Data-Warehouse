import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')


# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events
(artist VARCHAR, 
auth VARCHAR, 
firstName VARCHAR,
gender VARCHAR, 
itemInSession INTEGER, 
lastName VARCHAR, 
length FLOAT, 
level VARCHAR,
location VARCHAR,
method VARCHAR,
page VARCHAR,
registration BIGINT, 
sessionId INTEGER, 
song VARCHAR, 
status INTEGER, 
ts TIMESTAMP, 
userAgent VARCHAR,
userId INTEGER)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs
(num_songs int,
artist_id VARCHAR,
artist_latitude float,
artist_longitude float,
artist_location VARCHAR,
artist_name VARCHAR,
song_id VARCHAR,
title VARCHAR,
duration float,
year int)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY, 
start_time TIMESTAMP NOT NULL, 
user_id int NOT NULL, 
level VARCHAR,
song_id VARCHAR,
artist_id VARCHAR,
session_id VARCHAR,
location VARCHAR,
user_agent VARCHAR)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
user_id int PRIMARY KEY, 
first_name VARCHAR,
last_name VARCHAR,
gender VARCHAR,
level VARCHAR,
ts TIMESTAMP)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs 
(song_id VARCHAR PRIMARY KEY, 
title VARCHAR,
artist_id VARCHAR NOT NULL,
year int,
duration float NOT NULL)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists 
(artist_id VARCHAR PRIMARY KEY,
name VARCHAR NOT NULL,
location VARCHAR, 
latitude float,
longitude float)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time 
(start_time timestamp PRIMARY KEY,
hour int,\
day int,\
week int,\
month int,\
year int,\
weekday VARCHAR)
""")

# STAGING TABLES

staging_events_copy = ("""copy staging_events
FROM {}
CREDENTIALS 'aws_iam_role={}'
region 'us-west-2'
compupdate off
timeformat as 'epochmillisecs'
JSON {}
""").format(config.get ('S3', 'LOG_DATA'),
            config.get ('IAM_ROLE', 'ARN'),
            config.get ('S3','LOG_JSONPATH'))


staging_songs_copy = ("""
COPY staging_songs
FROM {}
FORMAT JSON AS 'auto'
iam_role '{}'
""").format(config.get('S3', 'SONG_DATA'),
            config.get('IAM_ROLE', 'ARN'))


# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays 
(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT 
se.ts AS start_time, 
se.userid, 
se.level, 
ss.song_id, 
ss.artist_id, 
se.sessionid, 
se.location, 
se.useragent 

FROM staging_events se 
JOIN staging_songs ss 
ON se.artist = ss.artist_name 
where se.page = 'NextSong' 
""")

user_table_insert = ("""
INSERT INTO users 
(user_id, first_name, last_name, gender, level, ts)
SELECT DISTINCT userid, 
                firstname, 
                lastname, 
                gender, 
                level, 
                MAX(ts) OVER(PARTITION BY userid, firstname, lastname, gender, level) AS max_ts_in_level
FROM staging_events 
WHERE userid IN (SELECT DISTINCT userid
                     FROM staging_events 
                    GROUP BY userid, firstname, lastname, gender
                   HAVING COUNT(DISTINCT level) > 1
                  ) 
 ORDER BY userid, firstname, lastname, gender, ts
""")

song_table_insert = ("""
INSERT INTO songs 
(song_id, title, artist_id, year, duration)
SELECT DISTINCT(song_id),
title,
artist_id, 
year, 
duration 
FROM staging_songs
WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT DISTINCT(artist_id), 
artist_name, 
artist_location, 
artist_latitude AS latitude, 
artist_longitude AS longitude 
FROM staging_songs 
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT distinct (so.start_time), 
extract(hour from so.start_time) AS hour, 
extract(day from so.start_time) AS day, 
extract(week from so.start_time) AS week,
extract(month from so.start_time) AS month, 
extract(year from so.start_time) AS year, 
extract(dayofweek from so.start_time) AS weekday 
FROM songplays so 
""")


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
