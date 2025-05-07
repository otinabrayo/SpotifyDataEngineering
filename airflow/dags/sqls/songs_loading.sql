DROP PROCEDURE IF EXISTS UKSLEEK.AIRTRAIN.load_spotify_songs();
CREATE OR REPLACE PROCEDURE UKSLEEK.AIRTRAIN.load_spotify_songs()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    v_result STRING;
    v_loaded_count NUMBER;
    v_updated_count NUMBER;
BEGIN
    BEGIN TRANSACTION;

    -- Create table if it doesn't exist
    CREATE TABLE IF NOT EXISTS UKSLEEK.AIRTRAIN.spotify_songs (
        name STRING,
        artists STRING,
        album STRING,
        album_id STRING,
        duration_ms NUMBER,
        external_url STRING,
        artist_ids STRING,
        release_date DATE,
        track_number NUMBER,
        disc_number NUMBER,
        explicit BOOLEAN,
        popularity NUMBER,
        uri STRING,
        updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    );

    -- Create temporary table for new data
    CREATE OR REPLACE TEMPORARY TABLE UKSLEEK.AIRTRAIN.temp_spotify_songs AS
    SELECT
        t.$1:name::STRING AS name,
        ARRAY_TO_STRING(ARRAY_AGG(t.$1:artists), ', ') AS artists,
        t.$1:album::STRING AS album,
        t.$1:album_id::STRING AS album_id,
        t.$1:duration_ms::NUMBER AS duration_ms,
        t.$1:external_url::STRING AS external_url,
        ARRAY_TO_STRING(ARRAY_AGG(t.$1:artist_ids), ', ') AS artists_id,
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
    WHERE name IS NOT NULL
    GROUP BY
        t.$1:name,
        t.$1:album,
        t.$1:album_id,
        t.$1:duration_ms,
        t.$1:external_url,
        t.$1:release_date,
        t.$1:track_number,
        t.$1:disc_number,
        t.$1:explicit,
        t.$1:popularity,
        t.$1:uri;

    -- Merge operation to update existing records and insert new ones
    MERGE INTO UKSLEEK.AIRTRAIN.spotify_songs target
    USING UKSLEEK.AIRTRAIN.temp_spotify_songs source
    ON target.uri = source.uri
    WHEN MATCHED AND (
        target.name != source.name OR
        target.artists != source.artists OR
        target.album != source.album OR
        target.album_id != source.album_id OR
        target.duration_ms != source.duration_ms OR
        target.external_url != source.external_url OR
        target.artist_ids != source.artists_id OR
        target.release_date != source.release_date OR
        target.track_number != source.track_number OR
        target.disc_number != source.disc_number OR
        target.explicit != source.explicit OR
        target.popularity != source.popularity
    ) THEN UPDATE SET
        target.name = source.name,
        target.artists = source.artists,
        target.album = source.album,
        target.album_id = source.album_id,
        target.duration_ms = source.duration_ms,
        target.external_url = source.external_url,
        target.artist_ids = source.artists_id,
        target.release_date = source.release_date,
        target.track_number = source.track_number,
        target.disc_number = source.disc_number,
        target.explicit = source.explicit,
        target.popularity = source.popularity,
        target.updated_at = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (
        name, artists, album, album_id, duration_ms, external_url,
        artist_ids, release_date, track_number, disc_number,
        explicit, popularity, uri, updated_at
    ) VALUES (
        source.name, source.artists, source.album, source.album_id,
        source.duration_ms, source.external_url, source.artists_id,
        source.release_date, source.track_number, source.disc_number,
        source.explicit, source.popularity, source.uri, CURRENT_TIMESTAMP()
    );

    -- Get counts for reporting
    SELECT COUNT(*) INTO v_loaded_count FROM UKSLEEK.AIRTRAIN.temp_spotify_songs;
    SELECT COUNT(*) INTO v_updated_count FROM UKSLEEK.AIRTRAIN.spotify_songs
    WHERE updated_at >= DATEADD(second, -10, CURRENT_TIMESTAMP());

    COMMIT;
    v_result := 'Procedure completed successfully. Loaded: ' || v_loaded_count ||
                ' records. Updated: ' || v_updated_count || ' records.';
    RETURN v_result;

EXCEPTION
    WHEN OTHER THEN
        ROLLBACK;
        RAISE;
END;
$$;

CALL UKSLEEK.AIRTRAIN.load_spotify_songs();
