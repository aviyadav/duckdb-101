
import duckdb
from pathlib import Path
import random
import json
import datetime
import multiprocessing
import time

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)
EVENTS_PATH = DATA_DIR / "events.parquet"

CITIES = [
    'Berlin', 'London', 'New York', 'Tokyo', 'Paris', 
    'Singapore', 'San Francisco', 'Toronto', 'Sydney', 'Mumbai'
]

EVENT_IDS = [f"evt_{i}" for i in range(1, 51)]

def generate_chunk(num_rows):
    """
    Generates a list of dictionaries for a chunk of rows.
    """
    # Re-seed random avoids identical chunks if processes start same time/state
    # (though multiprocessing usually handles this, explicit seed per process is safer if needed,
    # but random module usually handles forked state correctly).
    # We will just let it be.
    
    data = []
    start_date = datetime.datetime(2023, 1, 1)
    
    for _ in range(num_rows):
        row = (
            random.randint(1, 100),                                             # user_id
            start_date + datetime.timedelta(days=random.random() * 30),         # event_time
            'payment_processed',                                                # event_name
            random.choice(EVENT_IDS),                                           # event_id
            random.choice(CITIES),                                              # location
            'checkout-service',                                                 # source
            200,                                                                # status
            json.dumps({                                                        # details
                "amount": round(random.uniform(10.0, 500.0), 2), 
                "currency": "USD", 
                "card_last_4": f"{random.randint(1000, 9999)}", 
                "order_id": f"ord_{random.randint(100, 999)}"
            })
        )
        data.append(row)
    return data

def generate_data(total_rows: int = 10_000):
    num_processes = multiprocessing.cpu_count()
    chunk_size = total_rows // num_processes
    # Handle remainder
    chunks = [chunk_size] * num_processes
    chunks[-1] += (total_rows - sum(chunks))
    
    print(f"Generating {total_rows} rows using {num_processes} processes...")
    start_time = time.time()
    
    with multiprocessing.Pool(processes=num_processes) as pool:
        results = pool.map(generate_chunk, chunks)
    
    all_data = [item for sublist in results for item in sublist]
    
    generation_time = time.time() - start_time
    print(f"Generation took {generation_time:.2f} seconds.")

    print(f"Inserting into DuckDB...")
    con = duckdb.connect()
    con.execute("""
        CREATE TABLE events (
            user_id INTEGER,
            event_time TIMESTAMP,
            event_name VARCHAR,
            event_id VARCHAR,
            location VARCHAR,
            source VARCHAR,
            status INTEGER,
            details VARCHAR
        )
    """)
    
    insert_start = time.time()
    con.executemany("""
        INSERT INTO events VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, all_data)
    insert_time = time.time() - insert_start
    print(f"Insertion took {insert_time:.2f} seconds.")
    
    print(f"Writing to {EVENTS_PATH}...")
    con.execute(f"COPY events TO '{EVENTS_PATH}' (FORMAT 'parquet')")
    print("Done!")

if __name__ == "__main__":
    generate_data()
