# Synthetic Data

This directory holds pre-generated synthetic datasets for development and testing
when Kafka/Spark infrastructure is not available locally.

## Files

| File | Rows | Description |
|------|------|-------------|
| *(run generator to populate)* | — | — |

## Generate Synthetic Data

```bash
# Generate 10,000 sample events (fast, for quick tests)
python producer/event_generator.py --num-events 10000 --output data/synthetic/

# Generate full 10M dataset (takes ~5 min)
python producer/event_generator.py --num-events 10000000 --output data/synthetic/
```

## Schema

All synthetic files follow the same schema as `data/raw/sample_events.csv`.
