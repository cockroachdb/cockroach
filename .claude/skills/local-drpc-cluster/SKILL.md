# Local DRPC Cluster Skill

Deploy and manage a local 4-node CockroachDB cluster with DRPC enabled.

## Usage

Invoke this skill to:
- Build the cockroach binary
- Start a 4-node insecure cluster with DRPC enabled
- Initialize the cluster
- Stop the cluster
- Check cluster status

## Commands

### Start Cluster
Starts all 4 nodes and initializes the cluster with DRPC enabled.

### Stop Cluster
Gracefully stops all running nodes.

### Status
Shows the status of all nodes and cluster health.

### Restart Cluster
Stops and starts the cluster fresh.

## Cluster Configuration

- **Nodes**: 4 nodes
- **Security**: Insecure mode
- **RPC**: DRPC enabled (--use-new-rpc)
- **Ports**:
  - Node 1: RPC=26257, HTTP=8080
  - Node 2: RPC=26258, HTTP=8081
  - Node 3: RPC=26259, HTTP=8082
  - Node 4: RPC=26260, HTTP=8083
- **Data**: Stored in `cockroach-data/node{1-4}`

## Notes

- All nodes use `--use-new-rpc` flag to enable DRPC
- Cluster runs in insecure mode for easy testing
- Data is stored locally in the repository
