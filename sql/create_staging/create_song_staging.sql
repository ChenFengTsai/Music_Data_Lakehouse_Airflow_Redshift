CREATE TABLE IF NOT EXISTS spotify_table.track
(
    uri integer NOT NULL DISTKEY SORTKEY,
    song varchar(30),
    popularity float(2,4),
    artist varchar(30),
    energy float(2,4),
    acousticness float(2,4),
    speechiness float(2,4),
    tempo float(10,4),
    duration char(10,4),
    release_date varchar(30),
    album varchar(30)
);