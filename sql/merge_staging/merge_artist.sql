BEGIN TRANSACTION;

DELETE FROM spotify_table.artist;

INSERT INTO spotify_table.artist
SELECT
    artist_id,
    name,
    followers,
    popularity,
    genre
FROM spotify_table.artist_staging;

END TRANSACTION;