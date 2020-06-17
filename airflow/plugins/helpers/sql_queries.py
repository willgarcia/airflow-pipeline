class SqlQueries:

    songplay_table_insert = ("""
INSERT INTO factsongplay (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT
    '1970-01-01'::date + events.ts / 1000 * interval '1 second' as start_time,
    events.userId as user_id,
    events.level,
songs.song_id,
songs.artist_id,
    events.sessionId as session_id,
    events.location,
    events.user_agent
FROM staging_events events
JOIN staging_songs songs ON
    events.song = songs.title and events.artist = songs.artist_name
WHERE
    events.page = 'NextSong'
"""
)

    user_table_truncate = "TRUNCATE TABLE dimuser"   
    user_table_insert = ("""
INSERT INTO dimuser (user_id, first_name, last_name, gender, level)  
SELECT DISTINCT 
    userId,
    firstName,
    lastName,
    gender, 
    level
FROM staging_events
WHERE 
    page = 'NextSong'
    AND userId NOT IN (SELECT DISTINCT user_id FROM dimuser)
""")

    song_table_truncate = "TRUNCATE TABLE dimsong"   
    song_table_insert = ("""
INSERT INTO dimsong (song_id, title, artist_id, year, duration)
SELECT DISTINCT
    song_id,
    title,
    artist_id,
    year,
    duration
FROM staging_songs
WHERE 
    song_id NOT IN (SELECT DISTINCT song_id FROM dimsong)
""")

    artist_table_truncate = "TRUNCATE TABLE dimartist"   
    artist_table_insert = ("""
INSERT INTO dimartist (artist_id, name, location, latitude, longitude)
SELECT DISTINCT
    artist_id,
    artist_name as name,
    artist_location as location,
    artist_latitude as latitude,
    artist_longitude as longitude
FROM staging_songs
WHERE 
    artist_id NOT IN (SELECT DISTINCT artist_id FROM dimartist);
""")

    time_table_truncate = "TRUNCATE TABLE dimtime"   
    time_table_insert = ("""
INSERT INTO dimtime (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT
    start_time,
    EXTRACT(hour FROM start_time) as hour,
    EXTRACT(day FROM start_time) as day,
    EXTRACT(week FROM start_time) as week,
    EXTRACT(month FROM start_time) as month,
    EXTRACT(year FROM start_time) as year,
    EXTRACT(weekday FROM start_time) as weekday
FROM (
    SELECT DISTINCT '1970-01-01'::date + ts / 1000 * interval '1 second' as start_time
    FROM staging_events
    WHERE page = 'NextSong'
)
WHERE 
    start_time NOT IN (SELECT DISTINCT start_time FROM dimtime);
""")