import os
import subprocess
import sys
import time
from datetime import datetime, timedelta
import psycopg #PostgreSQL client

# PostgreSQL connection info
DB_NAME = "mydb"
DB_USER = "postgres"
DB_PASSWORD = "postgrespw"
DB_HOST = "localhost"
DB_PORT = 5433


# -------------------------
# Periodically compute average over last 10 minutes
# -------------------------
def fetch_avg_temp():
    """Fetch average temperature for the last 10 minutes."""
    ten_minutes_ago = datetime.now() - timedelta(minutes=10)
    ## Fetch the data from the choosen source (to be implemented)
        
    # Connect to PostgreSQL and fetch data
    with psycopg.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    ) as conn:
        with conn.cursor() as cur:
            #Get temperature readings from the last 10 minutes
            cur.execute("""
                SELECT AVG(temperature) 
                FROM temperature_readings
                WHERE recorded_at >= %s
            """, (ten_minutes_ago,))
            avg_temp = cur.fetchone()[0] # AVG returns None if no rows
            return avg_temp


try:
    print("Starting the temperature consumer...")
    while True:
        avg_temp = fetch_avg_temp()
        if avg_temp is not None:
            print(f"{datetime.now()} - Average temperature last 10 minutes: {avg_temp:.2f} °C")
        else:
            print(f"{datetime.now()} - No data in last 10 minutes.")
        time.sleep(600)  # every 10 minutes
except KeyboardInterrupt:
    print("Stopped consuming data.")
finally:
    print("Exiting.")