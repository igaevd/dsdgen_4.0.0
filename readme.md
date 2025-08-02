# TPC-DS Data Generator (dbgen2 / dsdgen)

---

## Basic Parameters

| Parameter         | Description | Example |
|------------------|-------------|---------|
| `-SCALE <int>`   | Data volume in GB (default: `1`) | `-scale 100` |
| `-TABLE <str>`   | Generate specific table(s) (default: ALL) | `-table store_sales` or `-table customer,store` |
| `-DIR <str>`     | Output directory (default: `.`) | `-dir /data/tpcds` |
| `-SUFFIX <str>`  | Output file extension (default: `.dat`) | `-suffix .csv` |
| `-DELIMITER <str>` | Field separator (default: `|`) | `-delimiter ,` |
| `-HELP`    | Display help |
| `-RELEASE` | Show version info |


**All Table Options (-TABLE):**

```
ALL
call_center / cc
catalog_page / cp
catalog_returns / cr
catalog_sales / cs
customer / cu
customer_address / ca
customer_demographics / cd
date_dim / da
household_demographics / hd
income_band / ib
inventory / inv
item / it
promotion / pr
reason / re
ship_mode / sm
store / st
store_returns / sr
store_sales / ss
time_dim / ti
warehouse / wa
web_page / wp
web_returns / wr
web_sales / ws
web_site / web
````

---

## Parallel Processing

| Parameter            | Description | Example |
|---------------------|-------------|---------|
| `-PARALLEL <int>`   | Number of parallel chunks | `-parallel 8` |
| `-CHILD <int>`      | Generate specific chunk (default: `1`) | `-child 3` |

---

## Data Generation Control

| Parameter              | Description | Example |
|------------------------|-------------|---------|
| `-UPDATE <int>`        | Generate update dataset | `-update 1` |
| `-RNGSEED <int>`       | Random seed (default: `19620718`) | `-rngseed 12345` |
| `-DISTRIBUTIONS <str>` | Distribution file (default: `tpcds.idx`) | `-distributions custom.idx` |

---

## Output Control

| Parameter         | Description                                      | Example |
|------------------|--------------------------------------------------|---------|
| `-TERMINATE <Y/N>` | End records with delimiter (default: `Y`)        | `-terminate N` |
| `-FORCE`          | Overwrite existing files without prompt          | `-force` |
| `-QUIET`          | Suppress all output                              | `-quiet` |
| `-VERBOSE`        | Enable detailed output                           | `-verbose` |
| `-STDOUT`        | Output to stdout instead of files                | `-stdout` |
| `-_FILTER`       | Legacy -STDOUT, left for backward compatibility  | `-_filter` |

---

## Validation Mode

| Parameter          | Description | Example |
|-------------------|-------------|---------|
| `-VALIDATE`       | Generate validation data | `-validate` |
| `-VCOUNT <int>`   | Validation row count (default: `50`) | `-vcount 100` |
| `-VSUFFIX <str>`  | Validation file suffix (default: `.vld`) | `-vsuffix .val` |

---

## Configuration

| Parameter             | Description | Example |
|----------------------|-------------|---------|
| `-PARAMS <str>`       | Read parameters from file | `-params config.txt` |
| `-ABREVIATION <str>`  | Build table by abbreviation | `-abreviation ss` |

---

## Hidden Parameters

| Parameter         | Description |
|------------------|-------------|
| `-CHKSEEDS`      | Validate RNG for parallel generation |
| `-_SCALE_INDEX`  | Internal scale band selector |

---

## Usage Examples

```bash
# Generate 10GB TPC-DS dataset
dsdgen -scale 10

# Generate only store_sales table at 100GB scale
dsdgen -scale 100 -table store_sales

# Parallel generation: 4 processes
dsdgen -scale 1000 -parallel 4 -child 1
dsdgen -scale 1000 -parallel 4 -child 2
dsdgen -scale 1000 -parallel 4 -child 3
dsdgen -scale 1000 -parallel 4 -child 4

# CSV format with custom directory
dsdgen -scale 10 -delimiter , -suffix .csv -dir /data/csv

# Validation mode
dsdgen -validate -vcount 100 -table item
````
