query=  """
        SELECT *
        FROM spotify_table.track AS s
        LEFT JOIN spotify_table.artist AS art ON art.name = s.artist
        LEFT JOIN spotify_table.album AS alb ON alb.name = s.album;
        """
        
UNLOAD (query)
TO '{{ params.s3_unload_path }}'
IAM_ROLE '{{ params.redshift_unload_iam_role }}'
PARQUET PARTITION BY (catgroup, catname) CLEANPATH;