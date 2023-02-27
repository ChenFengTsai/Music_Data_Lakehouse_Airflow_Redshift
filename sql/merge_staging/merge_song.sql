BEGIN TRANSACTION;

DELETE FROM spotify_table.track;

INSERT INTO spotify_table.track
SELECT
    uri,
    song,
    popularity,
    artist,
    energy,
    acousticness,
    speechiness,
    tempo,
    duration,
    release_date,
    album
FROM spotify_table.track_staging;

END TRANSACTION;