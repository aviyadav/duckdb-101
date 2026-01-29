# DuckDB NDJSON at Scale

This project demonstrates generating and processing large-scale NDJSON data using DuckDB.

## structure

- `generate_events.py`: A Python script to generate synthetic event data in NDJSON format.
- `data/`: Directory where generated data resides.

## Usage

### 1. Setup Environment

Ensure you have Python installed. It is recommended to use a virtual environment.

```bash
# Install dependencies (if any are added later, currently uses standard library + psutil/orjson)
pip install orjson psutil
```

### 2. Generate Events

To generate 1 million synthetic events, run the `generate_events.py` script.

```bash
python generate_events.py
```

This will create a `data/partitions` directory containing partitioned NDJSON files.

### 3. Processing

(Add details here about how to process the data with DuckDB if applicable)
