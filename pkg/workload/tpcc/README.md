# TPC-C Workload Guide

## Table of Contents
- [Basic Usage](#basic-usage)
  - [Essential Flags](#essential-flags)
- [Warehouse Configuration](#warehouse-configuration)
- [Single Region Setup](#single-region-setup)
  - [Connection Distribution with Partitioning](#connection-distribution-with-partitioning)
  - [Connection Calculation](#connection-calculation)
  - [Worker Calculation](#worker-calculation)
  - [Client Partition Configuration](#client-partition-configuration)
  - [Running Workload with Client-side Data Partitioning Example](#running-workload-with-client-side-data-partitioning-example)
- [Multi-region Setup](#running-tpc-c-in-multi-region-setup)
  - [Prerequisites](#prerequisites)
  - [Multi-region Configuration](#multi-region-configuration)
  - [Example Setup](#example-setup)

TPC-C is an industry-standard OLTP benchmark that simulates a complex order-entry system to evaluate database performance.

## Basic Usage

```bash
cockroach workload run tpcc \
  --warehouses=100 \
  --db=tpcc \
  --duration=1h \
  --ramp=10m
```
### Essential Flags
```bash
--warehouses=N          # Total number of warehouses
--active-warehouses=N   # Number of active warehouses (defaults to --warehouses)
--workers=N             # Number of concurrent workers. Defaults to --warehouses * 10
--duration=1h           # The duration to run (in addition to --ramp). If 0, run forever.
--ramp=10m              # Initial period where workers linearly scale from 1 to --workers. The duration over which to ramp up load.
```

## Warehouse Configuration
The TPC-C workload operates on a set of warehouses, where you can specify both total warehouses and active warehouses. 
Active warehouses are the ones that receive traffic during the workload run.

Consider a configuration with:
- Total warehouses: 10,000
- Active warehouses: 5,000

This means:
- The database will be populated with 10,000 warehouses
- Only 5,000 warehouses will receive actual traffic during the workload run


## Single-region Setup
When using multiple workload generators within a single-region CockroachDB cluster, workload partitioning is essential. 
This feature divides warehouses among workload nodes, with each node targeting only its assigned warehouses. Without 
partitioning, nodes would compete for the same warehouses, causing transaction contention and deadlocks that reduce 
performance.

```bash
--client-partitions=N    # Number of partitions to divide warehouses into
--partition-affinity=K   # Specifies which partition(s) this client targets, from 0 to N-1. Multiple values can be specified using commas
```

Consider a configuration with:
- Total warehouses: 10,000
- Active warehouses: 5,000
- Client partitions: 5

The warehouse partitioning logic is implemented in [`pkg/workload/tpcc/partition.go`](https://github.com/cockroachdb/cockroach/blob/master/pkg/workload/tpcc/partition.go#L274).
Warehouse distribution across partitions:
```bash
Total Warehouse Ranges:
Partition 0: warehouses [0-2000)      # 2000 warehouses
Partition 1: warehouses [2000-4000)   # 2000 warehouses
Partition 2: warehouses [4000-6000)   # 2000 warehouses
Partition 3: warehouses [6000-8000)   # 2000 warehouses
Partition 4: warehouses [8000-10000)  # 2000 warehouses

Active Warehouse Ranges (5000 active):
Partition 0: warehouses [0-1000)      # 1000 active warehouses
Partition 1: warehouses [2000-3000)   # 1000 active warehouses
Partition 2: warehouses [4000-5000)   # 1000 active warehouses
Partition 3: warehouses [6000-7000)   # 1000 active warehouses
Partition 4: warehouses [8000-9000)   # 1000 active warehouses
```

### Connection Distribution with Partitioning
This section explains how connections are distributed when using client-side partitioning. When `--client-partitions`
and `--partition-affinity` are specified, each client establishes connections only to the warehouses in its assigned partition(s).
```bash
--conns=N  # Number of connections. Defaults to --warehouses * 2 (except in nowait mode, where it defaults to --workers)
```
Implementation details can be found in [`pkg/workload/tpcc/tpcc.go`](https://github.com/cockroachdb/cockroach/blob/master/pkg/workload/tpcc/tpcc.go#L967).

1. Single Partition Affinity
```bash
# Example Configuration
--client-partitions=3
--partition-affinity=0
URLs=[node1:26257, node2:26257, node3:26257]

# Distribution
Partition 0: [node1, node2, node3]  # All connections assigned to partition 1
```

2. When multiple partition affinities are specified, connections are mapped one-to-one in order of URLs and affinity partitions.
```bash
# Example Configuration
--partitions=3
--partition-affinity=0,1
URLs=[node1:26257, node2:26257, node3:26257]

# Distribution
Partition 0: [node1]                  # From affinity mapping
Partition 1: [node2]                  # From affinity mapping
Partition 2: [node1,node2,node3]      # From fallback, this won't be used as affinity is not specified
```

### Connection Calculation
The number of connections for each workload generator node is calculated based on the number of active warehouses in its assigned partition.

```bash
# Formula
Connections per Client = (Active Warehouses ÷ Client Partitions) × 2

# Example
# Active warehouses per partition = 40 ÷ 4 = 10
# Connections per client = 10 × 2 = 20 connections
--warehouses=100 --active-warehouses=40 --client-partitions=4 --conns=20
```

This means each partition maintains twice as many connections as it has active warehouses. For example, if a partition is responsible for 10 active warehouses, it will establish 20 database connections.

### Worker Calculation
The number of workers for each workload generator node is calculated based on the number of active warehouses in its assigned partition:

```bash
# Formula
Workers per Client = (Active Warehouses ÷ Client Partitions) × 10

# Example
# Active warehouses per partition = 40 ÷ 4 = 10
# Workers per client = 10 × 10 = 100 workers
--warehouses=100 --active-warehouses=40 --client-partitions=4 --workers=10
```

### Client Partition Configuration
When choosing the number of client partitions, use either:
- The number of workload generator nodes, or
- The number of database nodes in your cluster

This ensures optimal pgurl distribution and resource utilization across crdb cluster.
Connection and workers are calculated the same way as in single region setup.

### Running workload with Client side data partitioning example
Here's a comprehensive example for running TPC-C with:
- 1200 total warehouses
- 600 active warehouses
- 3 client partitions
- 1 hour test duration with 10 minute ramp-up
```bash
# Calculated values:
# - Active warehouses per partition = 1200 ÷ 6 = 200
# - Workers per workload node = 200 × 10 = 2000
# - Connections per client = 200 × 2 = 400

# Node 1 (Partition 0)
cockroach workload run tpcc \
  --warehouses=1200 \
  --active-warehouses=600 \
  --client-partitions=3 \
  --partition-affinity=0 \
  --workers=2000 \
  --conns=400 \
  --duration=1h \
  --ramp=10m \
  "<PGURLS of all node>"

# Node 2 (Partition 1)
cockroach workload run tpcc \
  --warehouses=1200 \
  --active-warehouses=600 \
  --client-partitions=3 \
  --partition-affinity=1 \
  --workers=2000 \
  --conns=400 \
  --duration=1h \
  --ramp=10m \
  "<PGURLS of all node>"

# Node 3 (Partition 2)
cockroach workload run tpcc \
  --warehouses=1200 \
  --active-warehouses=600 \
  --client-partitions=3 \
  --partition-affinity=2 \
  --workers=2000 \
  --conns=400 \
  --duration=1h \
  --ramp=10m \
  "<PGURLS of all node>"
```

## Running TPC-C in Multi-region Setup

### Prerequisites
- A multi-region CockroachDB cluster
- One workload generator node per region
- Database configured with appropriate regions and survival goals

### Multi-region Configuration
#### Initial Data Loading and Warehouse Distribution
Before running the workload, initialize the TPC-C database with partitioned data across regions.
The warehouses are distributed in a round-robin fashion across regions.

For example, with 1200 warehouses and 3 regions:
- Region 1 (northamerica-northeast2): warehouses [1,4,7,...,1198]
- Region 2 (us-east5): warehouses [2,5,8,...,1199]
- Region 3 (us-central1): warehouses [3,6,9,...,1200]

Each region gets 400 warehouses total, distributed round-robin. This distribution helps in:
- Ensuring even load distribution across regions
- Avoiding hot spots in any single region
- Better utilization of cross-region bandwidth

Use this command to load data:
```bash
cockroach workload init tpcc \
    --data-loader=IMPORT \
    --partitions=<number of regions> \
    --warehouses=<number of warehouses> \
    --survival-goal region \
    --regions=northamerica-northeast2,us-east5,us-central1 \
```

Running the workload generator.
```bash
--partitions=N          # Number of regions in your cluster
--partition-affinity=K  # Region number (0-based) this workload node targets
--regions=REGIONS      # Comma-separated list of regions (e.g., "us-east1,us-west1,eu-west1")
--survival-goal=GOAL   # "region" for multi-region setups
```

### Connection and Worker Configuration
The formulas for calculating connections and workers remain the same as single-region setup:
- Connections per partition = (Active Warehouses ÷ Partitions) × 2
- Workers per partition = (Active Warehouses ÷ Partitions) × 10

For example, with 1200 warehouses and 3 regions:
- Active warehouses per region = 1200 ÷ 3 = 400
- Connections per region = 400 × 2 = 800
- Workers per region = 400 × 10 = 4000

### Example Setup
For a 3-region cluster with 1200 total warehouses:

```bash
# Region 1 Workload Node (us-east1)
cockroach workload run tpcc \
    --warehouses=1200 \
    --partitions=3 \
    --partition-affinity=0 \
    --workers=4000 \
    --duration=24h \
    --ramp=10m \
    --regions=northamerica-northeast2,us-east5,us-central1 \
    --survival-goal=region \
    "<PGURLS for nodes in the target region>"

# Region 2 Workload Node (us-west1)
cockroach workload run tpcc \
    --warehouses=12000 \
    --partitions=3 \
    --partition-affinity=1 \
    --workers=4000 \
    --duration=24h \
    --ramp=10m \
    --regions=northamerica-northeast2,us-east5,us-central1 \
    --survival-goal=region \
    "<PGURLS for nodes in the target region>"

# Region 3 Workload Node (eu-west1)
cockroach workload run tpcc \
    --warehouses=12000 \
    --partitions=3 \
    --partition-affinity=2 \
    --workers=4000 \
    --duration=24h \
    --ramp=10m \
    --regions=northamerica-northeast2,us-east5,us-central1 \
    --survival-goal=region \
    "<PGURLS for nodes in the target region>"
```
