CREATE DATABASE spotify_db;
USE spotify_db;

CREATE OR REPLACE STORAGE INTEGRATION s3_init
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = S3
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::975050103735:role/spotify-spark-snowflake-role'
    STORAGE_ALLOWED_LOCATIONS = ('s3://spotify-etl-project-shubham')
    COMMENT = "Creating connection to S3"

DESC integration s3_init;


// Create file format object
CREATE OR REPLACE FILE FORMAT csv_file_format
    type=csv
    field_delimiter = ','
    skip_header = 1
    null_if = ('NULL','null')
    empty_field_as_null = TRUE;

CREATE OR REPLACE STAGE spotify_stage
    URL = 's3://spotify-etl-project-shubham/transformed_data/'
    STORAGE_INTEGRATION = s3_init
    FILE_FORMAT = csv_file_format;


LIST @spotify_stage/songs;

CREATE OR REPLACE TABLE tbl_album(
    album_id STRING,
    name STRING,
    release_date DATE,
    total_tracks INT,
    url STRING
);

CREATE OR REPLACE TABLE tbl_artists(
    artist_id STRING,
    name STRING,
    url STRING
);

CREATE OR REPLACE TABLE tbl_songs(
    song_id STRING,
    song_name STRING,
    duration_ms INT,
    url STRING,
    popularity INT,
    song_added DATE,
    album_id STRING,
    artist_id STRING
);

-- create snowpipe
CREATE OR REPLACE SCHEMA pipe;


CREATE OR REPLACE pipe spotify_db.pipe.tbl_songs_pipe
auto_ingest = TRUE
AS 
COPY INTO spotify_db.public.tbl_songs
FROM @spotify_db.public.spotify_stage/songs/ ;


CREATE OR REPLACE pipe spotify_db.pipe.tbl_artists_pipe
auto_ingest = TRUE
AS 
COPY INTO spotify_db.public.tbl_artists
FROM @spotify_db.public.spotify_stage/artist/ ; 


CREATE OR REPLACE pipe spotify_db.pipe.tbl_album_pipe
auto_ingest = TRUE
AS 
COPY INTO spotify_db.public.tbl_album
FROM @spotify_db.public.spotify_stage/album/ ;


DESC pipe pipe.tbl_songs_pipe;
DESC pipe pipe.tbl_album_pipe;
DESC pipe pipe.tbl_artists_pipe;



DESC pipe


SELECT COUNT(*) FROM tbl_songs;


SELECT SYSTEM$PIPE_STATUS('pipe.tbl_songs_pipe')


SELECT SYSTEM$PIPE_STATUS('pipe.tbl_album_pipe')

