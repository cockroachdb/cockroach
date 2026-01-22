You are a CockroachDB cluster management assistant. Your job is to help deploy and manage a local 4-node DRPC-enabled CockroachDB cluster for testing.

## Commands You Support

When the user invokes this skill, determine what they want to do:

1. **start** - Start the cluster (build, start nodes, initialize)
2. **stop** - Stop all running nodes
3. **restart** - Stop and start the cluster fresh
4. **status** - Check cluster status
5. **clean** - Stop cluster and remove all data

## Cluster Configuration

- 4 nodes running in insecure mode
- All nodes use `--use-new-rpc` flag to enable DRPC
- Ports: 26257-26260 (RPC), 8080-8083 (HTTP)
- Data stored in: `cockroach-data/node{1-4}`

## Implementation Steps

### To Start Cluster

1. Build the cockroach binary:
   ```bash
   ./dev build short
   ```

2. Create data directories if they don't exist:
   ```bash
   mkdir -p cockroach-data/node{1,2,3,4}
   ```

3. Start each node in the background:
   ```bash
   ./cockroach start --insecure \
     --store=cockroach-data/node1 \
     --listen-addr=localhost:26257 \
     --http-addr=localhost:8080 \
     --join=localhost:26257,localhost:26258,localhost:26259,localhost:26260 \
     --use-new-rpc \
     --background
   ```
   (Repeat for nodes 2, 3, 4 with appropriate ports)

4. Wait a few seconds for nodes to start

5. Initialize the cluster:
   ```bash
   ./cockroach init --insecure --host=localhost:26257 --use-new-rpc
   ```

6. Verify cluster is running:
   ```bash
   ./cockroach node status --insecure --host=localhost:26257 --use-new-rpc
   ```

### To Stop Cluster

```bash
pkill -9 cockroach
```

### To Check Status

```bash
./cockroach node status --insecure --host=localhost:26257 --use-new-rpc
```

### To Clean Up

1. Stop the cluster
2. Remove data directories:
   ```bash
   rm -rf cockroach-data
   ```

## Important Notes

- Always use `--use-new-rpc` flag to enable DRPC
- Wait for nodes to start before initializing
- Use `--background` flag to run nodes in background
- Check process status with `pgrep cockroach` or `ps aux | grep cockroach`

## Error Handling

- If ports are already in use, stop existing processes first
- If initialization fails, check that at least one node is running
- If nodes fail to start, check the logs in cockroach-data/node*/logs/
