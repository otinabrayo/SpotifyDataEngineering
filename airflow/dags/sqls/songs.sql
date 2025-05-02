TRUNCATE TABLE UKSLEEK.AIRTRAIN.spotify_songs;

INSERT INTO UKSLEEK.AIRTRAIN.spotify_songs
SELECT
    t.$1:name::STRING AS name,
    ARRAY_TO_STRING(t.$1:artists, ', ') AS artists,
    t.$1:album::STRING AS album,
    t.$1:album_id::STRING AS album_id,
    t.$1:duration_ms::NUMBER AS duration_ms,
    t.$1:external_url::STRING AS external_url,
    t.$1:artist_ids::ARRAY AS artist_ids,
    TO_DATE(
        CASE
            WHEN LENGTH(t.$1:release_date::STRING) = 7 THEN t.$1:release_date::STRING || '-01'
            ELSE t.$1:release_date::STRING
        END
    ) AS release_date,
    t.$1:track_number::NUMBER AS track_number,
    t.$1:disc_number::NUMBER AS disc_number,
    t.$1:explicit::BOOLEAN AS explicit,
    t.$1:popularity::NUMBER AS popularity,
    t.$1:uri::STRING AS uri
FROM @my_s3_stage/spotify_songs/ (FILE_FORMAT => 'my_json_format') t
WHERE name IS NOT NULL;
