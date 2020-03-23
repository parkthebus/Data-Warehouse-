import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS events_staging;"
staging_songs_table_drop = "DROP TABLE IF EXISTS songs_staging;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"


# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS events_staging (
        artist        VARCHAR,
        auth          VARCHAR,
        firstName     VARCHAR,
        gender        CHAR(1),
        itemInSession INT,
        lastName      VARCHAR,
        length        NUMERIC,
        level         VARCHAR,
        location      VARCHAR,
        method        VARCHAR,
        page          VARCHAR,
        registration  NUMERIC,
        sessionId     INT,
        song          VARCHAR,
        status        INT,
        ts            NUMERIC,
        userAgent     VARCHAR,
        userId        INT
    )
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs_staging (
        num_songs        INT,
        artist_id        VARCHAR,
        artist_latitude  FLOAT,
        artist_longitude FLOAT,
        artist_location  VARCHAR,
        artist_name      VARCHAR,
        song_id          VARCHAR,
        title            VARCHAR,
        duration         FLOAT,
        year             INT
    )
""")

### Fact Table
# songplays - records in event data associated with song plays i.e. records with page NextSong 
#           songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id     INT IDENTITY(1,1) PRIMARY KEY,
    start_time      TIMESTAMP NOT NULL,
    user_id         INTEGER NOT NULL,
    level           VARCHAR NOT NULL,
    song_id         VARCHAR NOT NULL,
    artist_id       VARCHAR NOT NULL,
    session_id      INTEGER NOT NULL,
    location        VARCHAR NOT NULL,
    user_agent      VARCHAR NOT NULL
)
""")

### Dimension Tables
# users - users in the app user_id, first_name, last_name, gender, level
# songs - songs in music database song_id, title, artist_id, year, duration
# artists - artists in music database artist_id, name, location, lattitude, longitude
# time - timestamps of records in songplays broken down into specific units start_time, hour, day, week, month, year, weekday
user_table_create = ("""
CREATE TABLE IF NOT EXISTS users(
    user_id    INT NOT NULL PRIMARY KEY,
    first_name VARCHAR NOT NULL,
    last_name  VARCHAR NOT NULL,
    gender     VARCHAR NOT NULL,
    level      VARCHAR NOT NULL
)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs(
    song_id      VARCHAR NOT NULL PRIMARY KEY,
    title        VARCHAR NOT NULL,
    artist_id    VARCHAR NOT NULL,
    year         INTEGER NOT NULL,
    duration     NUMERIC NOT NULL
)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists(
    artist_id VARCHAR NOT NULL PRIMARY KEY,
    name      VARCHAR NOT NULL,
    location  VARCHAR,
    lattitude NUMERIC,
    longitude NUMERIC
)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time(
    start_time TIMESTAMP NOT NULL PRIMARY KEY,
    hour       INT NOT NULL,
    day        INT NOT NULL,
    week       INT NOT NULL,
    month      INT NOT NULL,
    year       INT NOT NULL,
    weekday    INT NOT NULL
)
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY events_staging FROM {}
    CREDENTIALS 'aws_iam_role={}'
    FORMAT AS json {}
""").format(config.get('S3','LOG_DATA'), config.get('IAM_ROLE','ARN'), config.get('S3','LOG_JSONPATH'))

staging_songs_copy = ("""
    COPY songs_staging FROM {}
    CREDENTIALS 'aws_iam_role={}'
    json 'auto'
""").format(config.get('S3','SONG_DATA'), config.get('IAM_ROLE','ARN'))

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (
        start_time, user_id, level, song_id, 
        artist_id, session_id, location, user_agent
    )
    SELECT DISTINCT
        TIMESTAMP 'epoch' + es.ts / 1000 * interval '1 second' AS start_time,
        es.userID AS user_id,
        es.level,
        ss.song_id,
        ss.artist_id,
        es.sessionId  assession_id,
        es.location,
        es.userAgent AS user_agent
    FROM events_staging AS es
    JOIN songs_staging AS ss ON es.song=ss.title AND es.artist=ss.artist_name
    WHERE es.page='NextSong'
""")

user_table_insert = ("""
    INSERT INTO users
    SELECT DISTINCT
        es.userId, es.firstName, es.lastName, es.gender, es.level
    FROM events_staging AS es
    JOIN (
        SELECT DISTINCT
            MAX(ts) AS ts, userId
        FROM events_staging
        WHERE page='NextSong'
        GROUP BY userId
    ) AS ei ON es.userId=ei.userId AND es.ts=ei.ts
""")

song_table_insert = ("""
    INSERT INTO songs
    SELECT DISTINCT
        song_id, title, artist_id,
        year, duration
    FROM songs_staging
""")

artist_table_insert = ("""
    INSERT INTO artists
    SELECT DISTINCT
        artist_id,
        artist_name AS name,
        artist_location AS location,
        artist_latitude AS latitude,
        artist_longitude AS longitude
    FROM songs_staging
""")

time_table_insert = ("""
    INSERT INTO time
    SELECT DISTINCT
        es.start_time,
        EXTRACT(hour FROM es.start_time) AS hour,
        EXTRACT(day FROM es.start_time) AS day,
        EXTRACT(week FROM es.start_time) AS week,
        EXTRACT(month FROM es.start_time) AS month,
        EXTRACT(year FROM es.start_time) AS year,
        EXTRACT(weekday FROM es.start_time) AS weekday
    FROM (
        SELECT DISTINCT
            TIMESTAMP 'epoch' + ts / 1000 * interval '1 second' AS start_time
        FROM songplays
        WHERE page='NextSong'
    ) AS es
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
