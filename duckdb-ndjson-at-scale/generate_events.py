import orjson
import datetime
import random
import time
import psutil
import os
import multiprocessing
from concurrent.futures import ProcessPoolExecutor

def generate_batch(num_events, worker_id, output_dir):
    """Generates a batch of events and writes them to a file."""
    # Each process needs its own random seed for unique data
    random.seed(os.getpid() + int(time.time()))
    
    output_filename = os.path.join(output_dir, f"events_part_{worker_id}.ndjson")
    
    # Pre-caching some values to speed up generation
    events = [b'checkout', b'refund', b'login', b'view_product', b'add_to_cart', b'logout']
    now = datetime.datetime.now(datetime.timezone.utc)
    
    with open(output_filename, 'wb') as f:
        for _ in range(num_events):
            # Generate random timestamp within the last year
            random_days = random.randint(0, 365)
            random_seconds = random.randint(0, 86400)
            ts = now - datetime.timedelta(days=random_days, seconds=random_seconds)

            user_id = random.randint(1, 50)
            event_type = random.choice(events)

            amount = None
            if event_type == b'checkout':
                amount = random.randint(100, 5000)
            elif event_type == b'refund':
                amount = random.randint(-5000, -100)
            elif event_type == b'add_to_cart':
                amount = random.randint(10, 1000)

            # Generate a random IP address
            ip_address = f"{random.randint(1, 254)}.{random.randint(1, 254)}.{random.randint(1, 254)}.{random.randint(1, 254)}"

            event_data = {
                "ts": ts, # orjson handles datetime directly
                "user": {"id": user_id},
                "event": event_type.decode(),
                "meta": {"ip": ip_address}
            }
            if amount is not None:
                event_data["amount"] = amount

            # orjson.dumps returns bytes, which is much faster than string dumps
            f.write(orjson.dumps(event_data) + b'\n')
    
    return num_events

def main():
    num_events_total = 1_000_000
    # For testing/demo purposes, we are generating 1 million events.
    # Note: 1M events is a reasonable size for local testing.
    
    num_workers = multiprocessing.cpu_count()
    events_per_worker = num_events_total // num_workers
    output_dir = "data/partitions"
    
    if not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)

    # Performance tracking
    process = psutil.Process(os.getpid())
    start_time = time.time()
    start_mem = process.memory_info().rss / (1024 * 1024)
    process.cpu_percent(interval=None)

    print(f"Generating {num_events_total:,} events using {num_workers} processes...")
    print(f"Output directory: {output_dir}")

    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        futures = []
        for i in range(num_workers):
            # The last worker gets the remainder
            count = events_per_worker + (num_events_total % num_workers if i == num_workers - 1 else 0)
            futures.append(executor.submit(generate_batch, count, i, output_dir))
        
        # Wait for all workers and sum up generated events
        total_generated = sum(f.result() for f in futures)

    # End performance tracking
    end_time = time.time()
    end_mem = process.memory_info().rss / (1024 * 1024)
    cpu_usage = process.cpu_percent(interval=None)
    duration = end_time - start_time

    print(f"\nSuccessfully generated {total_generated:,} events.")
    print("\n" + "="*30)
    print("Performance Metrics (Parallel):")
    print(f"  Execution Time: {duration:.4f} seconds")
    print(f"  CPU Utilization: {cpu_usage:.2f}% (Main Process)")
    print(f"  Memory Usage: {end_mem - start_mem:.2f} MB (Main Process Change)")
    print(f"  Throughput: {total_generated / duration:,.0f} events/sec")
    print("="*30)

if __name__ == "__main__":
    main()
