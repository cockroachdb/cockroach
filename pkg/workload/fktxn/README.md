# fktxn

`fktxn` is a workload that drives concurrent, FK-constrained transactions
against a target database. It exists to stress LDR's apply-side handling of
constraint handling, i.e. ensure we are properly synthesizing locks to avoid
constraint violations.

## Goals
1. We want to stress different (all) column types that might be seen in LDR so we know lock synthesis can handle all of them. This includes composite types and constraint types (primary key, unique constraint, foreign keys)
2. We want to stress interesting foreign key schemas e.g. cycles, long chains, self cycles etc.
3. We want to stress different interleavings of operations on the same constraint in order to generate lock collisions of all types (PK, UC, FK). 
4. We want enough throughput and lock contention such that we are exercising the buffer logic. 


## Non Goals
1. We are concerned more with correctness than performance, i.e. we expect the randomization of schema will cause this workload to have inconsistent throughput and that's fine.
2. We don't need perfect transaction generation such that every transaction is appliable, e.g. its fine if two transactions race and use the same primary key and one fails. Since the intended user of this is LDR, we really only care that the destination side is consistent with the source side, i.e. we can ignore+retry failed source transactions. 


## High Level Design
Note that all FKs must reference a unique constraint or primary key. To teach a workload generator how to create FK compliant transactions, we implicitly need to generate UC/PK complaint transactions.  Thus, we can assume that if we are thoroughly stressing FK lock synthesis, (with some tuning on table creation) we will also stress UC and PK lock synthesis as well, i.e. this design will mainly focus on FKs.

The crux of the problem we are trying to solve is that changes to rows with FKs are not self-contained to the row itself like they are for PKs/UCs. For example, if we randomly generate an delete on a parent table, we need to first verify there are no child tables that are pointing to this row. In other words, for any given row event, we need to find all potential FK dependencies.

The key observation here is that foreign key references are directional from one table to another, i.e. parent table to child table. If we model tables as nodes in a graph and FK dependencies as directed edges, we can model a schema as a directed graph of foreign key dependencies.

Consider the following example:

```sql
CREATE TABLE customers   (customer_id INT PRIMARY KEY);
CREATE TABLE products    (product_id  INT PRIMARY KEY);
CREATE TABLE orders      (order_id    INT PRIMARY KEY,
                          customer_id INT REFERENCES customers(customer_id));
CREATE TABLE order_items (order_id    INT REFERENCES orders(order_id),
                          product_id  INT REFERENCES products(product_id),
                          PRIMARY KEY (order_id, product_id));
```

we get the _directed_ FK graph:

```
                  ┌────────────┐         ┌────────────┐
                  │ customers  │         │  products  │
                  └─────┬──────┘         └─────┬──────┘
                        ▼                      │
                  ┌────────────┐               │
                  │   orders   │               │
                  └─────┬──────┘               │
                        └──────────┬───────────┘
                                   │
                                   ▼
                            ┌─────────────┐
                            │ order_items │
                            └─────────────┘
```

From this graph, we can walk to find our dependencies and what is dependent on us. Consider the two examples:

1. INSERT into orders: we need to make sure customer_id exists. We should generate an INSERT into customers if it doesn't. We also know that we could generate a random row for order_items now.
2. DELETE from customers: we need to make sure orders doesn't reference us. We should delete any orders referencing us, as well as order_items that reference orders.

This is the core idea of how we will generate valid FK transactions. Note that we want one directed graph per set of connected FK edges. e.g. the following schema would give us two FK graphs:

```sql
CREATE TABLE parent (id  INT PRIMARY KEY,
                     tag INT NOT NULL UNIQUE);
CREATE TABLE child1 (id        INT PRIMARY KEY,
                     parent_id INT REFERENCES parent(id));
CREATE TABLE child2 (id         INT PRIMARY KEY,
                     parent_tag INT REFERENCES parent(tag));
```

`child1` references `parent`'s PK; `child2` references `parent`'s UC. Even though both children point at `parent`, they go through *different* unique constraints, so they land in two independent FK graphs:

```
                  ┌──────────┐         ┌──────────┐
                  │  parent  │         │  parent  │
                  │   (id)   │         │  (tag)   │
                  └────┬─────┘         └────┬─────┘
                       ▼                    ▼
                  ┌──────────┐         ┌──────────┐
                  │  child1  │         │  child2  │
                  └──────────┘         └──────────┘
```

### Generating DAGs from FK Graphs
Note that cycles are allowed in FK dependencies but are not allowed in a transaction. We need a way to break said cycles such that we get DAGs and can cleanly resolve FK dependencies.

However, we don't want to use the same cycle break the entire time as we could be missing an interesting edge. Instead, we should occasionally rotate the DAG we are using.

This can be done with a topological sort and randomly removing edges to tables that appear multiple times.

While we are doing this, we can also "trim" the DAG. Note in the orders example that while we could INSERT to `customer`, `orders` and `order_items`, it would also be valid to insert to a subset of them as long as they are still connected. We can do this by randomly removing root or leaf nodes from the graph. We should be careful that doing so at too high of a frequency will cause a higher rate of invalid transactions.

### Generating Contention
There are two types of lock contention we care about: same and cross transaction lock contention:

1. Same txn: 
```sql
BEGIN
DELETE FROM t1 ("keyA");
INSERT INTO t1 ("keyA");
COMMIT
```

2. Cross txn:
```sql
BEGIN
DELETE FROM t1 ("keyA");
COMMIT

BEGIN
INSERT INTO t1 ("keyA");
COMMIT
```

We need to stress both types of contention. We will test the former through an approach we will call "chaining" and the latter through concurrent workers and constraint pooling.

### Concurrent Workers + Constraint Pools

**Key assumption**: The state space of random schema shapes is orders of magnitude larger than the ways we can interleave row events. In other words, we expect that if a bug can be found with our workload on the generated schema, we should hit it relatively quickly (~10 minutes). As a result, this workload should ideally be run multiple times (with new schemas) for short durations rather than stressed once over a long period of time. This assumption is important to note because it allows us to make several simplifications to the scope of our workload.

Namely, it allows us to easily create transactions across different workers that contend on the same constraint by precomputing a fixed size pool of rows to pull from. The general idea is that we want the source side to commit two transactions that touch the same constraint in a relatively short period of time. This way on the destination side, we will attempt to replicate both and be forced to order them.

We do this by precomputing columns for every constraint in the schema to pull from. This ensures that we get collisions between different workers. It may be possible for two concurrent workers to pick the same constraint too closely together such that we get a serialization error, but this should be ignorable as it's on the source side as long as the frequency doesn't dominate the workload.

After running for a while, we will eventually saturate all the constraints such that our state machine generator will usually pick a PK/UC that is already used. This greatly reduces the amount of transactions that will be successful, but at this point we can fall back to the above assumption and say we should restart with a new schema. Note that precomputing a new set of columns is not necessarily viable because we could have columns of type `CHAR` which will always quickly get saturated.

### Transaction Chaining
Within a transaction, we want to have many updates to the same row (and any FK dependencies it has) so lock synthesis detects a dependency and needs to order them. One idea is that we can "pin" a PK such that all operations we create will be forced to be on the same row. However, we don't want to naively pick a random INSERT/UPDATE/DELETE operation as not every operation will possible (e.g. can't DELETE the same row twice) and the longer we want the transaction to be the more likely we get an invalid sequence.

Instead, we implement a simple state machine as shown:

```
                                            ┌──── UPSERT ──┐
                                            ▼              │
       ┌──────────┐    UPSERT     ┌──────────┐             │
       │ Unknown  │ ────────────▶ │          │ ────────────┘
       └──────────┘               │          │
            │                     │  Exists  │
            │ DELETE              │          │
            ▼                     │          │
       ┌──────────┐    UPSERT     │          │
       │   None   │ ────────────▶ │          │
       └──────────┘               └──────────┘
            ▲                          │
            └────────── DELETE ────────┘
```

States:
- `Unknown`: This is our entry point into the state machine. We don't know if the row exists so we can either attempt to DELETE (if exists) it or UPSERT.
- `None`: We know that our PK doesn't exist, so we have to attempt to UPSERT it.
- `Exists`: The PK should exist, so we can either UPDATE or DELETE it.

Events:
- `UPSERT`: Walk the DAG in topological order (parents then children) and UPSERT one row per table.
  - Given our pinned PK, we randomly construct a row by picking UCs from the UC pool and randomly generating values for other columns.
- `DELETE`: Walk the DAG in reverse topological order (child then parents) and DELETE one row per table.

From this state machine, we can construct a chain of events that compose an "interesting" transaction. Note that this doesn't fully protect us from generating invalid transactions. Two potential reasons are:
1. We might have concurrent workers pick the same PK/UC. This is intentional and fine as described in the `Concurrent Workers` section. We can just skip the transaction in this case.
2. UPSERT is not foolproof in the case of UCs, as the UPDATE only works on PKs. We can attempt to retry with a different UC val, but at some point we should abort and DELETE.

## System diagram

```
                        ┌──────────────────────┐
                        │   Schema discovery   │
                        └──────────┬───────────┘
                                   │
                                   ▼
                        ┌──────────────────────┐
                        │       FK Graph       │
                        └──────────┬───────────┘
                                   │
                                   ▼
                        ┌──────────────────────┐
                        │     Orchestrator     │
                        └──────────┬───────────┘
                   ┌───────────────┼───────────────┐
                   ▼               ▼               ▼
              ┌──────────┐   ┌──────────┐    ┌──────────┐
              │ Worker 1 │   │ Worker 2 │ …  │ Worker N │
              └─────┬────┘   └─────┬────┘    └─────┬────┘
                    └──────────────┼───────────────┘
                   ┌───────────────┼───────────────┐
                   ▼               ▼               ▼
              ┌──────────┐   ┌──────────┐    ┌──────────┐
              │  Node 1  │   │  Node 2  │ …  │  Node M  │
              └──────────┘   └──────────┘    └──────────┘
```

### Schema Discovery
To generate schemas with sufficient randomization, we leverage the `randgen.RandCreateTables` package which knows how to create random tables with all the supported types/features. This is done as part of `workload init` but we abstract this from the workload driver and require it to reparse the schema for two reasons:
1. `randgen.RandCreateTables` returns CREATE and ALTER TABLE SQL statements. It's easier to parse the finalized schema for the exact fields we care about than the SQL statement.
2. This allows us to skip random schema generation if desired, i.e. we can hardcode a specific schema we want to test and the workload should work fine.

Since this is one time setup work, we don't have to do anything too innovative and can just query the cluster to extract relevant table information, e.g. column names, types, constraints, etc.

### FK Graph
We use union find to group FK edges into connected components. Roughly speaking, two FK edges belong to the same group if they touch the same unique constraint on the same table. Each resulting group is one "FK graph."

Note that this means a single table can appear in multiple FK graphs as mentioned earlier.


### Orchestrator
Given a set of FK graphs, the orchestrators job is to:
1. Precompute a pool of constraint values for each table. 
2. Convert FK graphs into DAGs and potentially trim them.
3. Assign a DAG to each worker and tell them the constraint pools.
4. Occasionally rotate what DAGs are assigned to each worker (re-breaking cycles and re-trimming).

### Worker
The worker is the driver of the workload and is in charge of generating transactions as described from the state machine above. It is also in charge of attempting to apply the transaction to a random node in the cluster.
