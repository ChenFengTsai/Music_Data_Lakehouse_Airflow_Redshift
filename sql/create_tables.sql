BEGIN SQL;

DROP TABLE IF EXISTS spotify_table.track;
CREATE TABLE spotify_table.track
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
    release_date varchar(30)
    album varchar(30)
);


DROP TABLE IF EXISTS spotify_table.artist;
CREATE TABLE spotify_table.artist
(
    artist_id integer NOT NULL DISTKEY SORTKEY,
    name varchar(100),
    followers integer,
    popularity float(2,4),
    genre varchar(30)
);

DROP TABLE IF EXISTS spotify_table.album;
CREATE TABLE spotify_table.album
(
    album_id integer NOT NULL DISTKEY SORTKEY,
    name varchar(100),
    popularity float(2,4),
    number_tracks integer
);

END SQL;
