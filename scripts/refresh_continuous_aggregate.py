import os
from datetime import datetime, timedelta
import psycopg2

def refresh_materialized_view(cur, view, start, end, interval):
    while start < end:
        between = start + interval
        print(f"{view}: {start} - {between}")
        cur.execute(f"CALL refresh_continuous_aggregate('{view}', '{start}', '{between}');")
        start = between

def refresh(start, end):
    user = os.environ["POSTGRES_USER"]
    password = os.environ["POSTGRES_PASSWORD"]
    dbname = os.environ["POSTGRES_DB"]
    host = "timescaledb"
    port = 5432

    conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()

    start = datetime.strptime(start, '%Y-%m-%d')
    end = datetime.strptime(end, '%Y-%m-%d')
    
    refresh_materialized_view(cur, 'statistics_5m', start, end, timedelta(hours=1))
    refresh_materialized_view(cur, 'ranking_statistics_1h', start, end, timedelta(days=1))
    refresh_materialized_view(cur, 'ranking_statistics_1d', start, end, timedelta(days=1))
    refresh_materialized_view(cur, 'quality_statistics_1h', start, end, timedelta(days=1))
    refresh_materialized_view(cur, 'quality_statistics_1d', start, end, timedelta(days=1))

if __name__ == '__main__':
    refresh('2023-03-01', '2023-03-22')
