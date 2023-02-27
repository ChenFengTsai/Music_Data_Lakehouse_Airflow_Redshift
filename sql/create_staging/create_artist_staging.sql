CREATE TABLE IF NOT EXISTS spotify_table.artist_staging
(
    artist_id integer NOT NULL DISTKEY SORTKEY,
    name varchar(100),
    followers integer,
    popularity float(2,4),
    genre varchar(30),
);