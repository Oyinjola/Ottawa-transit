"""
Transit Analytics API — serves pipeline output
"""
from flask import Flask, jsonify, request
from flask_cors import CORS
import sqlite3
from pathlib import Path

app = Flask(__name__)
CORS(app)

DB = Path("output/transit_warehouse.db")


def get_conn():
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    return conn


@app.route("/api/kpis")
def kpis():
    conn = get_conn()
    total, = conn.execute("SELECT COUNT(*) FROM fact_trips").fetchone()
    otr,   = conn.execute("SELECT ROUND(100.0*SUM(on_time)/COUNT(*),1) FROM fact_trips").fetchone()
    avg_d, = conn.execute("SELECT ROUND(AVG(delay_seconds),1) FROM fact_trips").fetchone()
    routes,= conn.execute("SELECT COUNT(DISTINCT route_id) FROM dim_routes").fetchone()
    conn.close()
    return jsonify({"total_trips": total, "on_time_rate": otr,
                    "avg_delay_s": avg_d, "total_routes": routes})


@app.route("/api/hourly")
def hourly():
    conn = get_conn()
    rows = conn.execute("SELECT * FROM agg_hourly ORDER BY hour").fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


@app.route("/api/routes")
def routes():
    conn = get_conn()
    rows = conn.execute("""
        SELECT r.route_id, r.route_name, r.route_type,
               ROUND(100.0*SUM(t.on_time)/COUNT(*),1) AS on_time_rate,
               ROUND(AVG(t.delay_seconds),0) AS avg_delay_s,
               COUNT(*) AS total_trips
        FROM dim_routes r JOIN fact_trips t USING(route_id)
        GROUP BY r.route_id
        ORDER BY avg_delay_s DESC
    """).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


@app.route("/api/trend")
def trend():
    conn = get_conn()
    rows = conn.execute("""
        SELECT date,
               ROUND(100.0*SUM(on_time)/COUNT(*),1) AS on_time_rate,
               ROUND(AVG(delay_seconds),1) AS avg_delay_s,
               COUNT(*) AS trips
        FROM fact_trips GROUP BY date ORDER BY date
    """).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


@app.route("/api/vehicles")
def vehicles():
    conn = get_conn()
    rows = conn.execute("""
        SELECT 
            v.vehicle_id,
            v.route_id,
            r.route_name,
            s.stop_name,
            v.lat,
            v.lon,
            v.delay_minutes,
            v.delay_seconds,
            v.status,
            v.period,
            v.timestamp
        FROM fact_vehicle_positions v
        JOIN dim_routes r ON v.route_id = r.route_id
        JOIN dim_stops s ON v.stop_id = s.stop_id
        ORDER BY ABS(v.delay_seconds) DESC
        LIMIT 100
    """).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])
    return jsonify([dict(r) for r in rows])


if __name__ == "__main__":
    if not DB.exists():
        print("Run pipeline.py first to generate the database.")
    else:
        app.run(debug=True, port=5052)