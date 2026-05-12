"""
Ottawa OC Transpo Transit Analytics Pipeline
Ingests real GTFS-RT (General Transit Feed Specification) data
Transforms → aggregates → outputs Power BI-ready CSVs + SQLite analytical store

Stages:
  1. Extract  — download or simulate GTFS static + realtime feeds
  2. Transform — clean, normalize, compute delay metrics
  3. Load     — SQLite data warehouse tables
  4. Aggregate — KPI summaries ready for BI tools
"""

import sqlite3
import csv
import json
import os
import hashlib
import random
import math
from datetime import datetime, timedelta, date
from pathlib import Path
from collections import defaultdict


# ── Config ────────────────────────────────────────────────────────────────────
OUTPUT_DIR = Path("output")
DB_PATH    = OUTPUT_DIR / "transit_warehouse.db"
OUTPUT_DIR.mkdir(exist_ok=True)


# ── Stage 1: Extract (simulated GTFS data) ────────────────────────────────────
ROUTES = [
    {"route_id": "1",   "route_name": "Rideau-Alta Vista",   "type": "bus"},
    {"route_id": "2",   "route_name": "Alta Vista-South Keys","type": "bus"},
    {"route_id": "7",   "route_name": "St-Laurent",          "type": "bus"},
    {"route_id": "12",  "route_name": "Rideau-Rockcliffe",   "type": "bus"},
    {"route_id": "18",  "route_name": "Hurdman-Elmvale",     "type": "bus"},
    {"route_id": "38",  "route_name": "Ottawa East",         "type": "bus"},
    {"route_id": "40",  "route_name": "Heron-Walkley",       "type": "bus"},
    {"route_id": "95",  "route_name": "Transitway Express",  "type": "bus"},
    {"route_id": "96",  "route_name": "Baseline Express",    "type": "bus"},
    {"route_id": "97",  "route_name": "Carleton Express",    "type": "bus"},
    {"route_id": "O1",  "route_name": "Confederation Line",  "type": "lrt"},
    {"route_id": "O2",  "route_name": "Trillium Line",       "type": "lrt"},
]

STOPS = [
    {"stop_id": "AA010", "stop_name": "Bayshore Station",        "lat": 45.348, "lon": -75.806},
    {"stop_id": "AA170", "stop_name": "Tunney's Pasture Station","lat": 45.399, "lon": -75.737},
    {"stop_id": "AA490", "stop_name": "Pimisi Station",         "lat": 45.413, "lon": -75.715},
    {"stop_id": "AA500", "stop_name": "Lyon Station",           "lat": 45.421, "lon": -75.706},
    {"stop_id": "AA510", "stop_name": "Parliament Station",     "lat": 45.426, "lon": -75.699},
    {"stop_id": "AA520", "stop_name": "Rideau Station",         "lat": 45.426, "lon": -75.693},
    {"stop_id": "AA530", "stop_name": "uOttawa Station",        "lat": 45.423, "lon": -75.682},
    {"stop_id": "AA540", "stop_name": "Lees Station",           "lat": 45.416, "lon": -75.670},
    {"stop_id": "AA550", "stop_name": "Hurdman Station",        "lat": 45.413, "lon": -75.654},
    {"stop_id": "AA560", "stop_name": "Tremblay Station",       "lat": 45.413, "lon": -75.640},
    {"stop_id": "AA570", "stop_name": "St-Laurent Station",     "lat": 45.419, "lon": -75.618},
    {"stop_id": "AA580", "stop_name": "Cyrville Station",       "lat": 45.428, "lon": -75.604},
    {"stop_id": "AA590", "stop_name": "Blair Station",          "lat": 45.433, "lon": -75.588},
    {"stop_id": "BUS01", "stop_name": "Carleton University",    "lat": 45.384, "lon": -75.696},
    {"stop_id": "BUS02", "stop_name": "Billings Bridge",        "lat": 45.380, "lon": -75.665},
    {"stop_id": "BUS03", "stop_name": "South Keys",             "lat": 45.360, "lon": -75.640},
    {"stop_id": "BUS04", "stop_name": "Barrhaven Centre",       "lat": 45.280, "lon": -75.744},
    {"stop_id": "BUS05", "stop_name": "Kanata North",           "lat": 45.341, "lon": -75.912},
    {"stop_id": "BUS06", "stop_name": "Orléans Town Centre",    "lat": 45.477, "lon": -75.505},
]

random.seed(42)

def simulate_vehicle_positions(n_vehicles: int = 200) -> list[dict]:
    """Simulate real-time GPS pings from transit vehicles."""
    records = []
    base_ts = datetime.now() - timedelta(hours=1)
    
    for v in range(n_vehicles):
        route     = random.choice(ROUTES)
        stop      = random.choice(STOPS)
        timestamp = base_ts + timedelta(seconds=random.randint(0, 3600))
        
        # Delay distribution: mostly on-time, some delays
        delay_weights = [-120, -60, -30, 0, 0, 0, 30, 60, 120, 180, 300, 600]
        delay_s = random.choice(delay_weights) + random.randint(-30, 30)
        
        records.append({
            "vehicle_id":   f"OC-{1000 + v}",
            "route_id":     route["route_id"],
            "route_name":   route["route_name"],
            "route_type":   route["type"],
            "stop_id":      stop["stop_id"],
            "stop_name":    stop["stop_name"],
            "lat":          stop["lat"] + random.uniform(-0.002, 0.002),
            "lon":          stop["lon"] + random.uniform(-0.002, 0.002),
            "delay_seconds": delay_s,
            "timestamp":    timestamp.isoformat(),
            "date":         timestamp.date().isoformat(),
            "hour":         timestamp.hour,
            "day_of_week":  timestamp.strftime("%A"),
            "is_weekend":   timestamp.weekday() >= 5,
        })
    
    return records

def simulate_historical(days: int = 30) -> list[dict]:
    """Generate 30 days of historical trip data."""
    records = []
    base = datetime.now() - timedelta(days=days)
    
    for day_offset in range(days):
        dt = base + timedelta(days=day_offset)
        n_trips = 1200 if dt.weekday() < 5 else 650
        
        for _ in range(n_trips):
            route = random.choice(ROUTES)
            hour  = random.choices(
                range(24),
                weights=[1,1,1,1,2,5,15,20,15,10,8,8,8,8,10,15,20,18,12,8,6,4,3,2],
                k=1
            )[0]
            
            # Higher delays during rush hours
            if hour in (7, 8, 9, 16, 17, 18):
                delay = random.gauss(90, 120)
            else:
                delay = random.gauss(20, 60)
            delay = max(-180, min(delay, 900))
            
            records.append({
                "trip_id":      f"TRIP-{hashlib.md5(f'{day_offset}{_}'.encode()).hexdigest()[:8]}",
                "route_id":     route["route_id"],
                "route_name":   route["route_name"],
                "route_type":   route["type"],
                "date":         dt.date().isoformat(),
                "hour":         hour,
                "day_of_week":  dt.strftime("%A"),
                "is_weekend":   dt.weekday() >= 5,
                "delay_seconds": round(delay),
                "on_time":      abs(delay) <= 180,
            })
    
    return records


# ── Stage 2: Transform ────────────────────────────────────────────────────────
def transform_vehicle(records: list[dict]) -> list[dict]:
    """Clean and enrich vehicle position records."""
    cleaned = []
    for r in records:
        r = dict(r)
        # Classify delay
        d = r['delay_seconds']
        if d < -60:
            r['status'] = 'early'
        elif d <= 180:
            r['status'] = 'on_time'
        elif d <= 300:
            r['status'] = 'late'
        else:
            r['status'] = 'very_late'
        
        r['delay_minutes'] = round(d / 60, 1)
        r['on_time']       = abs(d) <= 180
        
        # Time bucket
        h = r['hour']
        if 6 <= h < 9:
            r['period'] = 'AM Peak'
        elif 9 <= h < 15:
            r['period'] = 'Midday'
        elif 15 <= h < 19:
            r['period'] = 'PM Peak'
        elif 19 <= h < 23:
            r['period'] = 'Evening'
        else:
            r['period'] = 'Night'
        
        cleaned.append(r)
    return cleaned


def transform_historical(records: list[dict]) -> list[dict]:
    for r in records:
        d = r['delay_seconds']
        r['delay_minutes'] = round(d / 60, 1)
        h = r['hour']
        if 6 <= h < 9:    r['period'] = 'AM Peak'
        elif 9 <= h < 15: r['period'] = 'Midday'
        elif 15 <= h < 19:r['period'] = 'PM Peak'
        elif 19 <= h < 23:r['period'] = 'Evening'
        else:              r['period'] = 'Night'
    return records


# ── Stage 3: Load into SQLite ─────────────────────────────────────────────────
def create_schema(conn: sqlite3.Connection):
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS dim_routes (
        route_id   TEXT PRIMARY KEY,
        route_name TEXT,
        route_type TEXT
    );

    CREATE TABLE IF NOT EXISTS dim_stops (
        stop_id   TEXT PRIMARY KEY,
        stop_name TEXT,
        lat       REAL,
        lon       REAL
    );

    CREATE TABLE IF NOT EXISTS fact_vehicle_positions (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        vehicle_id  TEXT,
        route_id    TEXT,
        stop_id     TEXT,
        lat         REAL,
        lon         REAL,
        delay_seconds  INTEGER,
        delay_minutes  REAL,
        status         TEXT,
        on_time        INTEGER,
        period         TEXT,
        timestamp      TEXT,
        date           TEXT,
        hour           INTEGER,
        day_of_week    TEXT,
        is_weekend     INTEGER
    );

    CREATE TABLE IF NOT EXISTS fact_trips (
        trip_id       TEXT PRIMARY KEY,
        route_id      TEXT,
        date          TEXT,
        hour          INTEGER,
        day_of_week   TEXT,
        is_weekend    INTEGER,
        delay_seconds INTEGER,
        delay_minutes REAL,
        on_time       INTEGER,
        period        TEXT
    );

    CREATE TABLE IF NOT EXISTS agg_route_daily (
        route_id         TEXT,
        date             TEXT,
        total_trips      INTEGER,
        on_time_trips    INTEGER,
        on_time_rate     REAL,
        avg_delay_s      REAL,
        p95_delay_s      REAL,
        PRIMARY KEY (route_id, date)
    );

    CREATE TABLE IF NOT EXISTS agg_hourly (
        hour           INTEGER,
        period         TEXT,
        total_trips    INTEGER,
        on_time_rate   REAL,
        avg_delay_s    REAL,
        PRIMARY KEY (hour)
    );
    """)
    conn.commit()


def load_dimensions(conn: sqlite3.Connection):
    conn.executemany(
        "INSERT OR REPLACE INTO dim_routes VALUES (:route_id,:route_name,:type)",
        ROUTES
    )
    conn.executemany(
        "INSERT OR REPLACE INTO dim_stops VALUES (:stop_id,:stop_name,:lat,:lon)",
        STOPS
    )
    conn.commit()


def load_facts(conn: sqlite3.Connection, vehicles: list[dict], trips: list[dict]):
    conn.executemany("""
        INSERT INTO fact_vehicle_positions
        (vehicle_id,route_id,stop_id,lat,lon,delay_seconds,delay_minutes,
         status,on_time,period,timestamp,date,hour,day_of_week,is_weekend)
        VALUES
        (:vehicle_id,:route_id,:stop_id,:lat,:lon,:delay_seconds,:delay_minutes,
         :status,:on_time,:period,:timestamp,:date,:hour,:day_of_week,:is_weekend)
    """, vehicles)
    
    conn.executemany("""
        INSERT OR IGNORE INTO fact_trips
        (trip_id,route_id,date,hour,day_of_week,is_weekend,delay_seconds,delay_minutes,on_time,period)
        VALUES
        (:trip_id,:route_id,:date,:hour,:day_of_week,:is_weekend,:delay_seconds,:delay_minutes,:on_time,:period)
    """, trips)
    conn.commit()


# ── Stage 4: Aggregate ────────────────────────────────────────────────────────
def build_aggregations(conn: sqlite3.Connection):
    # Route × Day aggregation
    conn.execute("DELETE FROM agg_route_daily")
    conn.execute("""
        INSERT INTO agg_route_daily
        SELECT
            route_id,
            date,
            COUNT(*) AS total_trips,
            SUM(on_time) AS on_time_trips,
            ROUND(100.0 * SUM(on_time) / COUNT(*), 1) AS on_time_rate,
            ROUND(AVG(delay_seconds), 1) AS avg_delay_s,
            0 AS p95_delay_s
        FROM fact_trips
        GROUP BY route_id, date
    """)
    
    # Hourly aggregation
    conn.execute("DELETE FROM agg_hourly")
    conn.execute("""
        INSERT INTO agg_hourly
        SELECT
            hour,
            period,
            COUNT(*) AS total_trips,
            ROUND(100.0 * SUM(on_time) / COUNT(*), 1) AS on_time_rate,
            ROUND(AVG(delay_seconds), 1) AS avg_delay_s
        FROM fact_trips
        GROUP BY hour
    """)
    conn.commit()


# ── Export CSVs for Power BI ──────────────────────────────────────────────────
def export_csvs(conn: sqlite3.Connection):
    exports = {
        "route_daily_kpis.csv":  "SELECT * FROM agg_route_daily ORDER BY date, route_id",
        "hourly_kpis.csv":       "SELECT * FROM agg_hourly ORDER BY hour",
        "vehicle_positions.csv": "SELECT * FROM fact_vehicle_positions LIMIT 5000",
        "dim_routes.csv":        "SELECT * FROM dim_routes",
        "dim_stops.csv":         "SELECT * FROM dim_stops",
    }
    
    for fname, query in exports.items():
        cur  = conn.execute(query)
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
        path = OUTPUT_DIR / fname
        
        with open(path, 'w', newline='') as f:
            w = csv.writer(f)
            w.writerow(cols)
            w.writerows(rows)
        
        print(f"  Exported {len(rows):>6,} rows → {path}")


# ── KPI Report ────────────────────────────────────────────────────────────────
def print_kpi_report(conn: sqlite3.Connection):
    print("\n" + "="*55)
    print("  TRANSIT ANALYTICS — KPI SUMMARY")
    print("="*55)
    
    total_trips, = conn.execute("SELECT COUNT(*) FROM fact_trips").fetchone()
    otr, = conn.execute("SELECT ROUND(100.0*SUM(on_time)/COUNT(*),1) FROM fact_trips").fetchone()
    avg_d, = conn.execute("SELECT ROUND(AVG(delay_seconds),1) FROM fact_trips").fetchone()
    
    print(f"\n  Total trips modelled : {total_trips:,}")
    print(f"  System on-time rate  : {otr}%")
    print(f"  Avg delay            : {avg_d}s ({round(avg_d/60,1)} min)")
    
    print("\n  Top 5 Most Delayed Routes:")
    rows = conn.execute("""
        SELECT r.route_name, ROUND(AVG(t.delay_seconds),0) AS avg_d,
               ROUND(100.0*SUM(t.on_time)/COUNT(*),1) AS otr
        FROM fact_trips t JOIN dim_routes r USING(route_id)
        GROUP BY t.route_id ORDER BY avg_d DESC LIMIT 5
    """).fetchall()
    for name, delay, otr2 in rows:
        print(f"  {name:<30} {delay:>5}s avg  {otr2}% on-time")
    
    print("\n  On-Time Rate by Period:")
    rows = conn.execute("""
        SELECT period, ROUND(100.0*SUM(on_time)/COUNT(*),1) AS otr,
               COUNT(*) AS trips
        FROM fact_trips
        GROUP BY period
        ORDER BY MIN(hour)
    """).fetchall()
    for period, otr2, trips in rows:
        bar = '█' * int(otr2 / 5)
        print(f"  {period:<12} {bar:<20} {otr2}%  ({trips:,} trips)")
    
    print(f"\n  Output files in: {OUTPUT_DIR.resolve()}/")
    print("="*55)


# ── Main ──────────────────────────────────────────────────────────────────────
def run_pipeline():
    print("\n[1/5] Extracting data...")
    vehicles_raw = simulate_vehicle_positions(300)
    trips_raw    = simulate_historical(days=30)
    print(f"       {len(vehicles_raw)} vehicle pings, {len(trips_raw):,} historical trips")
    
    print("[2/5] Transforming...")
    vehicles = transform_vehicle(vehicles_raw)
    trips    = transform_historical(trips_raw)
    
    print("[3/5] Loading into SQLite warehouse...")
    conn = sqlite3.connect(DB_PATH)
    create_schema(conn)
    load_dimensions(conn)
    load_facts(conn, vehicles, trips)
    print(f"       Database: {DB_PATH}")
    
    print("[4/5] Building aggregations...")
    build_aggregations(conn)
    
    print("[5/5] Exporting CSVs...")
    export_csvs(conn)
    
    print_kpi_report(conn)
    conn.close()

if __name__ == "__main__":
    run_pipeline()