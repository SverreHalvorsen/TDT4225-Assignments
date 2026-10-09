import csv
import json
import os
import pandas as pd

from collections import Counter

from haversine import haversine

from DbConnector import DbConnector
from tabulate import tabulate

LINE_SEPERATOR = "-" * 50

# Written by Clean.ipynb; TIMESTAMP is already converted to Porto local time there
CLEAN_DATA_PATH = "porto/clean.parquet"
TRIPS_LOAD_PATH = os.path.abspath("porto/trips_load.csv")
POINTS_LOAD_PATH = os.path.abspath("porto/points_load.csv")

CITY_HALL_LON, CITY_HALL_LAT = -8.62911, 41.15794
RADIUS_M = 100
# Bounding box slightly larger than 100 m: 1° lat ≈ 111.3 km, 1° lon ≈ 83.8 km at Porto
LAT_DELTA = 0.0010
LON_DELTA = 0.0013

TAXI_FIELDS = {
    "id": "INT NOT NULL PRIMARY KEY"
}

TRIP_FIELDS = {
    "id": "BIGINT NOT NULL PRIMARY KEY",
    "taxi_id": "INT NOT NULL",
    "call_type": "CHAR(1) NOT NULL",
    "origin_call": "INT",
    "origin_stand": "INT",
    "start_time": "DATETIME NOT NULL",
    "number_of_points": "INT NOT NULL",
    "distance_km": "DOUBLE NOT NULL",
}

TRIP_CONSTRAINTS = [
    "FOREIGN KEY (taxi_id) REFERENCES taxi(id) ON DELETE CASCADE",
    "INDEX idx_taxi_id (taxi_id, start_time)"
]

TRIP_POINT_FIELDS = {
    "trip_id": "BIGINT NOT NULL",
    "sequence_number": "INT NOT NULL",
    "latitude": "DOUBLE NOT NULL",
    "longitude": "DOUBLE NOT NULL",
}

TRIP_POINT_CONSTRAINTS = [
    "PRIMARY KEY (trip_id, sequence_number)",
    "FOREIGN KEY (trip_id) REFERENCES trip(id) ON DELETE CASCADE",
]

# Added after the bulk load: building the index once is much faster than updating it per row
LAT_LON_INDEX = "INDEX lat_lon (latitude, longitude)"


class DatabaseManager:

    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def create_table(self, table_name: str, fields: dict, constraints: list = None):
        fields_query = ", ".join([f"{field} {field_type}" for field, field_type in fields.items()])
        if constraints:
            fields_query += ", " + ", ".join(constraints)

        query = f"CREATE TABLE IF NOT EXISTS {table_name} ({fields_query})"

        self.cursor.execute(query)
        self.db_connection.commit()

    def insert_data(self, table_name: str, columns, rows: list, batch_size: int = 10_000):
        cols = ", ".join(columns)
        placeholders = ", ".join(["%s"] * len(columns))
        query = f"INSERT IGNORE INTO {table_name} ({cols}) VALUES ({placeholders})"
        for i in range(0, len(rows), batch_size):
            self.cursor.executemany(query, rows[i:i + batch_size])
            self.db_connection.commit()

    def load_csv(self, table_name: str, columns, path: str):
        cols = ", ".join(columns)
        query = f"""
            LOAD DATA LOCAL INFILE '{path}' INTO TABLE {table_name}
            FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
            LINES TERMINATED BY '\\n'
            ({cols})
        """
        self.cursor.execute(query)
        self.db_connection.commit()

    def fetch_data(self, table_name: str):
        query = "SELECT * FROM %s"
        self.cursor.execute(query % table_name)
        rows = self.cursor.fetchall()
        print("Data from table %s, raw format:" % table_name)
        print(rows)
        # Using tabulate to show the table in a nice way
        print("Data from table %s, tabulated:" % table_name)
        print(tabulate(rows, headers=self.cursor.column_names))
        return rows

    def run(self, sql: str, params: tuple = None):
        self.cursor.execute(sql, params or ())
        return self.cursor.column_names, self.cursor.fetchall()

    def show(self, headers, rows):
        print(tabulate(rows, headers=headers))

    def drop_table(self, table_name):
        query = "DROP TABLE %s"
        self.cursor.execute(query % table_name)

    def show_tables(self):
        self.cursor.execute("SHOW TABLES")
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))
        print()


def main():
    program = None
    try:
        program = DatabaseManager()
        initiate_tables(program)

        _, rows = program.run("SELECT COUNT(*) FROM trip")
        if rows[0][0] == 0:
            df = pd.read_parquet(CLEAN_DATA_PATH)
            seed_data(program, df)
        else:
            print(f"Skipping seeding, trip table already has {rows[0][0]:,} rows")

        complete_tasks(program)

        # drop_tables(program)

        # program.insert_data(table_name="Person")
        # _ = program.fetch_data(table_name="Person")
        # program.drop_table(table_name="Person")
        # Check that the table is dropped
        # program.show_tables()
    except Exception as e:
        print("ERROR: Failed to use database:", e)
        raise
    finally:
        if program:
            program.connection.close_connection()


def initiate_tables(program: DatabaseManager):
    program.create_table("taxi", TAXI_FIELDS)
    program.create_table("trip", TRIP_FIELDS, TRIP_CONSTRAINTS)
    program.create_table("trip_point", TRIP_POINT_FIELDS, TRIP_POINT_CONSTRAINTS)
    print("Tables created successfully:")
    program.show_tables()


def seed_data(program: DatabaseManager, df: pd.DataFrame):
    if os.path.exists(TRIPS_LOAD_PATH) and os.path.exists(POINTS_LOAD_PATH):
        print("Reusing existing load files (delete them if the cleaned data has changed)")
    else:
        write_load_files(df)

    program.insert_data("taxi", TAXI_FIELDS, [(int(taxi_id),) for taxi_id in df["TAXI_ID"].unique()])

    # Data is consistent by construction (every point is written for a trip in the same load)
    program.cursor.execute("SET foreign_key_checks = 0")
    program.cursor.execute("SET unique_checks = 0")
    print("Loading trips...")
    program.load_csv("trip", TRIP_FIELDS, TRIPS_LOAD_PATH)
    print("Loading trip points...")
    program.load_csv("trip_point", TRIP_POINT_FIELDS, POINTS_LOAD_PATH)
    print("Building lat_lon index...")
    program.cursor.execute(f"ALTER TABLE trip_point ADD {LAT_LON_INDEX}")
    program.cursor.execute("SET foreign_key_checks = 1")
    program.cursor.execute("SET unique_checks = 1")

    print("Data seeded successfully:")


def write_load_files(df: pd.DataFrame):
    # MySQL stores rows sorted by primary key, so inserting in TRIP_ID order only appends to the end
    df = df.sort_values("TRIP_ID")

    # Write to .tmp first, so a crash halfway never leaves a partial file that looks complete
    trips_tmp, points_tmp = TRIPS_LOAD_PATH + ".tmp", POINTS_LOAD_PATH + ".tmp"
    print(f"Writing {len(df):,} trips to load files...")
    with open(trips_tmp, "w", newline="") as trips_file, open(points_tmp, "w", newline="") as points_file:
        trip_writer = csv.writer(trips_file, lineterminator="\n")
        point_writer = csv.writer(points_file, lineterminator="\n")
        for row in df.itertuples(index=False):
            latlon = [(lat, lon) for lon, lat in json.loads(row.POLYLINE)]
            distance = sum(haversine(a, b) for a, b in zip(latlon, latlon[1:]))
            # \N is NULL in LOAD DATA; column order must match TRIP_FIELDS
            trip_writer.writerow([row.TRIP_ID, row.TAXI_ID, row.CALL_TYPE,
                                  r"\N" if pd.isna(row.ORIGIN_CALL) else row.ORIGIN_CALL,
                                  r"\N" if pd.isna(row.ORIGIN_STAND) else row.ORIGIN_STAND,
                                  row.TIMESTAMP, len(latlon), distance])
            point_writer.writerows((row.TRIP_ID, i, lat, lon) for i, (lat, lon) in enumerate(latlon))

    os.replace(trips_tmp, TRIPS_LOAD_PATH)
    os.replace(points_tmp, POINTS_LOAD_PATH)


def drop_tables(program: DatabaseManager):
    program.drop_table("trip_point")
    program.drop_table("trip")
    program.drop_table("taxi")
    print("Tables dropped successfully:")
    program.show_tables()


def complete_tasks(program: DatabaseManager):
    print("Starting answering the questions...")
    print(LINE_SEPERATOR)

    tasks = [task_one, task_two, task_three, task_four, task_five, task_six, task_seven, task_eight, task_nine,
             task_ten]

    for task in tasks:
        task(program)
        print(LINE_SEPERATOR)


def task_one(program: DatabaseManager):
    print("Task 1: Number of taxis, trips and GPS points.")
    print()
    headers, rows = program.run("""
                                SELECT (SELECT COUNT(*) FROM taxi)       AS taxis,
                                       (SELECT COUNT(*) FROM trip)       AS trips,
                                       (SELECT COUNT(*) FROM trip_point) AS gps_points
                                """)
    program.show(headers, rows)
    print()


def task_two(program: DatabaseManager):
    print("Task 2: Average number of trips per taxi.")
    print()
    headers, rows = program.run("""
                                SELECT (SELECT COUNT(*) FROM trip) / NULLIF((SELECT COUNT(*) FROM taxi), 0)
                                           AS avg_trips_per_taxi
                                """)
    program.show(headers, rows)
    print()


def task_three(program: DatabaseManager):
    print("Task 3: The 20 taxis with the most trips.")
    print()
    headers, rows = program.run("""
                                SELECT taxi_id, COUNT(*) AS number_of_trips
                                FROM trip
                                GROUP BY taxi_id
                                ORDER BY number_of_trips DESC
                                    LIMIT %s
                                """, (20,))
    program.show(headers, rows)
    print()


def task_four(program: DatabaseManager):
    print("Task 4a: Most used call type per taxi.")
    print()
    headers, rows = program.run("""
                                SELECT taxi_id, call_type, call_type_count
                                FROM (SELECT taxi_id,
                                             call_type,
                                             COUNT(*) AS call_type_count,
                                             RANK() OVER (PARTITION BY taxi_id ORDER BY COUNT(*) DESC) AS rnk
                                     FROM trip
                                     GROUP BY taxi_id, call_type) AS ranked
                                WHERE rnk = 1
                                ORDER BY taxi_id, call_type
                                """)

    program.show(headers, rows[:20])
    if len(rows) > 20:
        print(f"{len(rows) - 20} more rows not shown...")
    summary = Counter(calltype for _, calltype, _ in rows)
    program.show(["most_common_call_type", "number_of_taxis"], sorted(summary.items()))
    taxis = len({taxi_id for taxi_id, _, _ in rows})
    if len(rows) > taxis:
        print(f"{len(rows) - taxis} taxi(s) tied between call types and appear more than once.")

    print()
    print(
        "Task 4b: Average trip duration and distance, and share of trips in each 6-hour time band grouped by call type.")

    headers, rows = program.run("""
                                SELECT call_type,
                                       AVG(GREATEST(number_of_points - 1, 0)) * 15 / 60                              AS avg_trip_duration_minutes,
                                       AVG(distance_km)                                                                AS avg_trip_distance_km,
                                       SUM(CASE WHEN HOUR (start_time) BETWEEN 0 AND 5 THEN 1 ELSE 0 END) / COUNT(*)   AS share_0_6,
                                       SUM(CASE WHEN HOUR (start_time) BETWEEN 6 AND 11 THEN 1 ELSE 0 END) / COUNT(*)  AS share_6_12,
                                       SUM(CASE WHEN HOUR (start_time) BETWEEN 12 AND 17 THEN 1 ELSE 0 END) / COUNT(*) AS share_12_18,
                                       SUM(CASE WHEN HOUR (start_time) BETWEEN 18 AND 24 THEN 1 ELSE 0 END) / COUNT(*) AS share_18_24
                                FROM trip
                                GROUP BY call_type
                                ORDER BY call_type
                                """)

    program.show(headers, rows)


def task_five(program: DatabaseManager):
    print("Task 5: Taxis with most total hours driven, and distance driven.")
    print()
    headers, rows = program.run("""
                                SELECT taxi_id,
                                       SUM(GREATEST(number_of_points - 1, 0) * 15 / (60 * 60)) AS total_hours_driven,
                                       SUM(distance_km)                                        AS total_distance_driven
                                FROM trip
                                GROUP BY taxi_id
                                ORDER BY total_hours_driven DESC, total_distance_driven DESC
                                """)
    program.show(headers, rows[:20])
    if len(rows) > 20:
        print(f"{len(rows) - 20} more rows not shown...")


def task_six(program: DatabaseManager):
    print(f"Task 6: Trips that passed within {RADIUS_M} m of Porto City Hall ({CITY_HALL_LON}, {CITY_HALL_LAT})")
    print()
    headers, rows = program.run(f"""
                                SELECT DISTINCT trip_id
                                FROM trip_point
                                WHERE latitude BETWEEN {CITY_HALL_LAT - LAT_DELTA} AND {CITY_HALL_LAT + LAT_DELTA}
                                  AND longitude BETWEEN {CITY_HALL_LON - LON_DELTA} AND {CITY_HALL_LON + LON_DELTA}
                                  AND ST_Distance_Sphere(POINT(longitude, latitude), POINT({CITY_HALL_LON}, {CITY_HALL_LAT})) <= {RADIUS_M}
                                ORDER BY trip_id
                                """)
    print(f"Number of trips: {len(rows)}")
    program.show(headers, rows[:20])
    if len(rows) > 20:
        print(f"{len(rows) - 20} more rows not shown...")


def task_seven(program: DatabaseManager):
    print("Task 7: Identify invalid trips (fewer than 3 GPS points)")
    print()
    headers, rows = program.run("""
                                SELECT COUNT(*) as number_of_invalid_trips
                                FROM trip
                                WHERE number_of_points < 3
                                """)
    program.show(headers, rows)


def task_eight(program: DatabaseManager):
    print("Task 8: Identify trips starting on one day and ending on the next")
    print()

    headers, rows = program.run("""
                                SELECT id                                                                             AS trip_id,
                                       taxi_id,
                                       start_time,
                                       DATE_ADD(start_time, INTERVAL (GREATEST(number_of_points - 1, 0)) * 15 SECOND) AS end_time
                                FROM trip
                                WHERE DATEDIFF(DATE_ADD(start_time, INTERVAL (GREATEST(number_of_points - 1, 0)) * 15 SECOND), start_time) = 1
                                ORDER BY start_time
                                """)
    print(f"Number of trips: {len(rows)}")
    program.show(headers, rows[:20])
    if len(rows) > 20:
        print(f"{len(rows) - 20} more rows not shown...")


def task_nine(program: DatabaseManager):
    print("Task 9: Trips with start and end points less than 50m apart")
    print()
    headers, rows = program.run("""
                                SELECT t.id                                                         AS trip_id,
                                       t.taxi_id,
                                       t.start_time,
                                       ST_Distance_Sphere(POINT(tp_start.longitude, tp_start.latitude),
                                                          POINT(tp_end.longitude, tp_end.latitude)) AS start_end_distance_m
                                FROM trip t
                                         JOIN trip_point tp_start
                                              ON t.id = tp_start.trip_id AND tp_start.sequence_number
                                                  = 0
                                         JOIN trip_point tp_end ON t.id = tp_end.trip_id AND
                                                                   tp_end.sequence_number = t.number_of_points - 1
                                WHERE t.number_of_points >= 2
                                  AND ST_Distance_Sphere(POINT(tp_start.longitude, tp_start.latitude),
                                                         POINT(tp_end.longitude, tp_end.latitude)) < 50
                                ORDER BY t.start_time
                                """)
    print(f"Number of trips: {len(rows)}")
    program.show(headers, rows[:20])
    if len(rows) > 20:
        print(f"{len(rows) - 20} more rows not shown...")


def task_ten(program: DatabaseManager):
    print("Task 10: Top 20 taxis with highest average idle time between consecutive trips")
    print()
    headers, rows = program.run("""
                                SELECT taxi_id, COUNT(*) AS number_of_gaps, AVG(idle_seconds) / 60 AS avg_idle_minutes
                                FROM (SELECT taxi_id,
                                             TIMESTAMPDIFF(
                                                 SECOND,
                                                     LAG(DATE_ADD(start_time, INTERVAL GREATEST(number_of_points - 1, 0) * 15 SECOND)) OVER (PARTITION BY taxi_id ORDER BY start_time),
                                                     start_time)
                                                 AS idle_seconds
                                      FROM trip) as gaps
                                WHERE idle_seconds IS NOT NULL
                                  AND idle_seconds >= 0
                                GROUP BY taxi_id
                                ORDER BY avg_idle_minutes DESC LIMIT 20
                                """)
    program.show(headers, rows)


if __name__ == '__main__':
    main()
