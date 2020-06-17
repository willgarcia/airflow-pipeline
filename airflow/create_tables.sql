CREATE TABLE staging_events (
    artist VARCHAR(255),
    auth VARCHAR(40),
    firstName VARCHAR(255),
    gender VARCHAR(1),
    itemInSession SMALLINT, 
    lastName VARCHAR(255),
    length  DECIMAL(10, 5),
    level VARCHAR(40),
    location VARCHAR(255),
    method VARCHAR(20),
    page VARCHAR(40),
    registration VARCHAR(40),
    sessionId INT,
    song VARCHAR(255),
    status SMALLINT,
    ts BIGINT,
    user_agent  VARCHAR(255),
    userId INTEGER 
);

CREATE TABLE staging_songs (
    artist_id VARCHAR(100),
    artist_latitude DECIMAL(9,6),
    artist_longitude DECIMAL(9,6),
    artist_location VARCHAR(255),
    artist_name VARCHAR(255),
    duration DOUBLE PRECISION,
    num_songs SMALLINT,
    song_id VARCHAR(255),
    title  VARCHAR(255),
    year SMALLINT 
)

CREATE TABLE factsongplay (
    songplay_id INT IDENTITY(0,1),
    start_time TIMESTAMP NOT NULL,
    user_id VARCHAR(100) NOT NULL,
    level VARCHAR(10),
    song_id VARCHAR(100) NOT NULL,
    artist_id VARCHAR(100) NOT NULL,
    session_id VARCHAR(100) NOT NULL,
    location VARCHAR(200),
    user_agent  VARCHAR(200)
);

CREATE TABLE dimsong (
    song_id VARCHAR(100) PRIMARY KEY,
    title  VARCHAR(255),
    artist_id VARCHAR(100) NOT NULL,
    year SMALLINT,
    duration SMALLINT 
);

CREATE TABLE dimuser (
    user_id VARCHAR(100) PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    gender VARCHAR(1),
    level VARCHAR(10) 
);

CREATE TABLE dimartist (
    artist_id VARCHAR(100) PRIMARY KEY,
    name VARCHAR(100),
    location VARCHAR(200),
    latitude DECIMAL(9,6),
    longitude DECIMAL(9,6) 
);

CREATE TABLE dimtime (
    start_time TIMESTAMP PRIMARY KEY,
    hour SMALLINT,
    day SMALLINT,
    week SMALLINT,
    month SMALLINT,
    year SMALLINT,
    weekday SMALLINT 
);
