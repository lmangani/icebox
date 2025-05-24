# Icebox Streaming Demo

This demo shows how to:
- Initialize an Icebox project
- Import and append demo data to a table at intervals
- Query the table after each append

## What it does

- Creates a temporary Icebox project
- Imports a CSV file as a table (`demo_table`)
- Every 10 seconds, appends a new row to the CSV and re-imports it
- After each append, runs `SELECT count(*) FROM demo_table` and prints the result
- Runs for 5 intervals by default

## How to run

```sh
cd examples
# Make sure you have built the icebox binary in the parent directory
# and Go is installed

# Optionally set ICEBOX_BIN to specify the path to the icebox binary
# export ICEBOX_BIN=/path/to/icebox

go run streaming_demo.go
```

### Custom interval

Set the `ICEBOX_DEMO_INTERVAL` environment variable (e.g., `5s`, `1m`):

```sh
ICEBOX_DEMO_INTERVAL=5s go run streaming_demo.go
```

You should see the row count increase every interval. 