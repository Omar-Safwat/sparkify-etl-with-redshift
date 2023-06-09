import configparser


# CONFIG
config = configparser.ConfigParser()
config.read("dwh.cfg")

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events CASCADE;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs CASCADE;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays CASCADE;"
user_table_drop = "DROP TABLE IF EXISTS users CASCADE;"
song_table_drop = "DROP TABLE IF EXISTS songs CASCADE;"
artist_table_drop = "DROP TABLE IF EXISTS artists CASCADE;"
time_table_drop = "DROP TABLE IF EXISTS time CASCADE;"

# CREATE TABLES
staging_events_table_create = """
CREATE TABLE IF NOT EXISTS staging_events (
    artist VARCHAR(255) DISTKEY,
    auth VARCHAR(50),
    firstName VARCHAR(255),
    gender CHAR(1),
    iteminSession SMALLINT,
    lastName VARCHAR(255),
    length REAL,
    level VARCHAR(4),
    location VARCHAR(255),
    method VARCHAR(10),
    page VARCHAR(100),
    registration NUMERIC,
    sessionid INTEGER,
    song VARCHAR(255),
    status SMALLINT,
    ts DOUBLE PRECISION,
    userAgent TEXT,
    userid VARCHAR(100)
);
"""
staging_songs_table_create = """
CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs SMALLINT,
    artist_id VARCHAR(100),
    artist_latitude REAL,
    artist_longitude REAL,
    artist_location VARCHAR(255),
    artist_name VARCHAR(255) DISTKEY,
    song_id VARCHAR(100) PRIMARY KEY,
    title VARCHAR(255),
    duration REAL,
    year SMALLINT
);
"""
songplay_table_create = """
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id INTEGER IDENTITY(0, 1) PRIMARY KEY,
    start_time timestamp NOT NULL SORTKEY,
    user_id VARCHAR(100) NOT NULL,
    level VARCHAR(4),
    song_id VARCHAR(100) NOT NULL,
    artist_id VARCHAR(100) NOT NULL DISTKEY,
    session_id INTEGER,
    location VARCHAR(255),
    user_agent TEXT
);
"""
user_table_create = """
CREATE TABLE IF NOT EXISTS users (
    user_id VARCHAR(100) PRIMARY KEY,
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    gender VARCHAR(1),
    level VARCHAR(4) SORTKEY
)
DISTSTYLE ALL;
"""
song_table_create = """
CREATE TABLE IF NOT EXISTS songs(
    song_id VARCHAR(100) PRIMARY KEY,
    title VARCHAR(255),
    artist_id VARCHAR(100),
    year SMALLINT SORTKEY,
    duration REAL
)
DISTSTYLE ALL;
"""
artist_table_create = """
CREATE TABLE IF NOT EXISTS artists(
    artist_id VARCHAR(100) PRIMARY KEY SORTKEY,
    name VARCHAR(255),
    location VARCHAR(255),
    latitude REAL,
    longitude REAL
)
DISTSTYLE ALL;
"""
time_table_create = """
CREATE TABLE IF NOT EXISTS time(
    start_time TIMESTAMP PRIMARY KEY SORTKEY,
    hour SMALLINT,
    day SMALLINT,
    week SMALLINT,
    month SMALLINT,
    year SMALLINT,
    weekday VARCHAR(10)
)
DISTSTYLE ALL;
"""
# STAGING TABLES

staging_events_copy = (
    """COPY staging_events FROM {}
    CREDENTIALS 'aws_iam_role={}'
    REGION 'us-west-2'
    json {}
    """
).format(config.get("S3", "LOG_DATA"), config.get("IAM_ROLE", "ARN"), config.get("S3", "LOG_JSONPATH"))

staging_songs_copy = (
    """COPY staging_songs FROM {}
    CREDENTIALS 'aws_iam_role={}'
    REGION 'us-west-2'
    JSON 'auto'
    """
).format(config.get("S3", "SONG_DATA"), config.get("IAM_ROLE", "ARN"))

# FINAL TABLES

songplay_table_insert = """
INSERT INTO songplays (
    start_time,
    user_id,
    level,
    song_id,
    artist_id,
    session_id,
    location,
    user_agent)
SELECT
    TIMESTAMP 'epoch' + se.ts/1000 * interval '1 second' AS start_time,
    se.userid AS user_id,
    se.level AS level,
    ss.song_id AS song_id,
    ss.artist_id AS artist_id,
    se.sessionid AS session_id,
    se.location AS location,
    se.userAgent AS user_agent
FROM staging_events se
JOIN staging_songs ss ON (se.song = ss.title AND se.artist = ss.artist_name AND se.length = ss.duration)
WHERE se.page = 'NextSong'
"""
user_table_insert = """
INSERT INTO users (
    user_id,
    first_name,
    last_name,
    gender,
    level)
SELECT
    userid AS user_id,
    firstName AS first_name,
    lastName AS last_name,
    gender,
    level
FROM staging_events
WHERE page = 'NextSong'
"""
song_table_insert = """
INSERT INTO songs (
    song_id,
    title,
    artist_id,
    year,
    duration)
SELECT
    song_id,
    title,
    artist_id,
    year,
    duration
FROM staging_songs
"""
artist_table_insert = """
INSERT INTO artists (
    artist_id,
    name,
    location,
    latitude,
    longitude)
SELECT
    artist_id,
    artist_name AS name,
    artist_location AS location,
    artist_latitude AS latitude,
    artist_longitude AS longitude
FROM staging_songs
WHERE artist_id IS NOT NULL
"""
time_table_insert = """
INSERT INTO time (
    start_time,
    hour,
    day,
    week,
    month,
    year,
    weekday)
SELECT
    start_time,
    EXTRACT(hour FROM start_time) AS hour,
    EXTRACT(day FROM start_time) AS day,
    EXTRACT(week FROM start_time) AS week,
    EXTRACT(month FROM start_time) AS month,
    EXTRACT(year FROM start_time) AS year,
    EXTRACT(weekday FROM start_time) AS weekday
FROM songplays
"""
# QUERY LISTS

create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    songplay_table_create,
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create,
]
drop_table_queries = [
    staging_events_table_drop,
    staging_songs_table_drop,
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop,
]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [
    songplay_table_insert,
    user_table_insert,
    song_table_insert,
    artist_table_insert,
    time_table_insert,
]
