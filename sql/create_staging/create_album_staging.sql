CREATE TABLE IF NOT EXISTS spotify_table.album_staging
(
    album_id integer NOT NULL DISTKEY SORTKEY,
    name varchar(100),
    popularity float(2,4),
    number_tracks integer,
);
