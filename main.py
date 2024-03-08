import csv
import json
import os
import sqlite3
from sqlite3 import Connection, Cursor
from typing import List, Tuple


DB_FILENAME = "stream_data.db"
TABLE_NAME = "stream_data"


def create_database() -> Tuple[Connection, Cursor]:
    conn = sqlite3.connect(DB_FILENAME)
    cursor = conn.cursor()

    cursor.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            timestamp TEXT,
            username TEXT,
            platform TEXT,
            ms_played INTEGER,
            conn_country TEXT,
            ip_addr_decrypted TEXT,
            user_agent_decrypted TEXT,
            track_name TEXT,
            artist_name TEXT,
            album_name TEXT,
            spotify_track_uri TEXT,
            episode_name TEXT,
            episode_show_name TEXT,
            spotify_episode_uri TEXT,
            reason_start TEXT,
            reason_end TEXT,
            shuffle INTEGER,
            skipped INTEGER,
            offline INTEGER,
            offline_timestamp TEXT,
            incognito_mode INTEGER
        )
    """)

    return conn, cursor


def insert_data(conn: Connection, cursor: Cursor, spotify_data_dir: str) -> None:
    data_file_prefix = "Streaming_History_Audio_"

    for file in os.listdir(spotify_data_dir):
        if file.startswith(data_file_prefix) and file.endswith(".json"):
            print(file)
            with open(os.path.join(spotify_data_dir, file), "r", encoding="utf-8") as json_file:
                data = json.load(json_file)
                for stream in data:
                    cursor.execute(
                        f"INSERT INTO {TABLE_NAME} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        tuple(stream.values())
                    )

    conn.commit()


def analyse_data(cursor: Cursor, working_dir: str):
    stream_condition = "spotify_track_uri <> '' AND ms_played >= 30000"

    headers = ["track_name", "artist_name", "album_name", "total_play_count", "total_listen_time_ms"]
    track_play_counts = get_track_play_counts(cursor, stream_condition)
    write_to_csv(working_dir, "track_counts", track_play_counts, headers)

    headers_by_year = ["year", "track_name", "artist_name", "album_name", "total_play_count", "total_listen_time_ms"]
    track_play_counts_by_year = get_track_play_counts_by_year(cursor, stream_condition)
    write_to_csv(working_dir, "track_counts_yearly", track_play_counts_by_year, headers_by_year)

    headers_artist = ["artist_name", "total_play_count", "total_listen_time_ms"]
    artist_play_counts = get_artist_play_counts(cursor, stream_condition)
    write_to_csv(working_dir, "artist_counts", artist_play_counts, headers_artist)

    headers_artist_by_year = ["year", "artist_name", "total_play_count", "total_listen_time_ms"]
    artist_play_counts_by_year = get_artist_play_counts_by_year(cursor, stream_condition)
    write_to_csv(working_dir, "artist_counts_yearly", artist_play_counts_by_year, headers_artist_by_year)


def get_track_play_counts(cursor: Cursor, stream_condition: str) -> List[Tuple]:
    cursor.execute(
        f"""
        SELECT
            track_name,
            artist_name,
            album_name,
            COUNT(*) as total_play_count,
            SUM(ms_played) as total_listen_time_ms
        FROM
            {TABLE_NAME}
        WHERE
            {stream_condition}
        GROUP BY
            track_name,
            artist_name
        ORDER BY
            total_play_count DESC
        """
    )

    return cursor.fetchall()


def get_track_play_counts_by_year(cursor: Cursor, stream_condition: str) -> List[Tuple]:
    cursor.execute(
        f"""
        SELECT
            STRFTIME('%Y', "timestamp") as "year",
            track_name,
            artist_name,
            album_name,
            COUNT(*) as total_play_count,
            SUM(ms_played) as total_listen_time_ms
        FROM
            {TABLE_NAME}
        WHERE
            {stream_condition}
        GROUP BY
            track_name,
            artist_name,
            "year"
        ORDER BY
            total_play_count DESC
        """
    )

    return cursor.fetchall()


def get_artist_play_counts(cursor: Cursor, stream_condition: str) -> List[Tuple]:
    cursor.execute(
        f"""
        SELECT
            artist_name,
            COUNT(*) as total_play_count,
            SUM(ms_played) as total_listen_time_ms
        FROM
            {TABLE_NAME}
        WHERE
            {stream_condition}
        GROUP BY
            artist_name
        ORDER BY
            total_play_count DESC
        """
    )

    return cursor.fetchall()


def get_artist_play_counts_by_year(cursor: Cursor, stream_condition: str) -> List[Tuple]:
    cursor.execute(
        f"""
        SELECT
            STRFTIME('%Y', "timestamp") as "year",
            artist_name,
            COUNT(*) as total_play_count,
            SUM(ms_played) as total_listen_time_ms
        FROM
            {TABLE_NAME}
        WHERE
            {stream_condition}
        GROUP BY
            artist_name,
            "year"
        ORDER BY
            total_play_count DESC
        """
    )

    return cursor.fetchall()


def write_to_csv(working_dir: str, file_name: str, data: List[Tuple], header_names: List[str]) -> None:
    output_dir = os.path.join("output", file_name)
    with open(os.path.join(working_dir, f"{output_dir}.csv"), "w", newline="", encoding="utf-8") as output_file:
        writer = csv.writer(output_file)
        writer.writerow(header_names)
        writer.writerows(data)


def main():
    working_dir = os.path.dirname(os.path.realpath(__file__))
    spotify_data_dir = os.path.join(working_dir, "data")

    conn, cursor = create_database()
    insert_data(conn, cursor, spotify_data_dir)

    analyse_data(cursor, working_dir)


if __name__ == "__main__":
    main()
