BEGIN TRANSACTION;

DELETE FROM spotify_table.album;

INSERT INTO spotify_table.album
SELECT
    album_id,
    name,
    popularity,
    number_tracks
FROM spotify_table.album_staging;

END TRANSACTION;