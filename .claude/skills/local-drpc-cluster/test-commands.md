# Test Commands for DRPC Cluster

Once the cluster is running, use these commands to test DRPC functionality:

## Node Commands

### Check node status (tests DRPC RPC)
```bash
./cockroach node status --insecure --host=localhost:26257 --use-new-rpc
```

### Decommission a node (tests DRPC RPC)
```bash
./cockroach node decommission 4 --insecure --host=localhost:26257 --use-new-rpc
```

### Recommission a node (tests DRPC RPC)
```bash
./cockroach node recommission 4 --insecure --host=localhost:26257 --use-new-rpc
```

### Drain a node (tests DRPC RPC)
```bash
./cockroach node drain 4 --insecure --host=localhost:26257 --use-new-rpc
```

## Debug Commands

### Debug gossip values (tests DRPC RPC)
```bash
./cockroach debug gossip-values --insecure --host=localhost:26257 --use-new-rpc
```

### Debug zip (tests DRPC RPC)
```bash
./cockroach debug zip /tmp/debug.zip --insecure --host=localhost:26257 --use-new-rpc
```

### Debug list-files (tests DRPC RPC)
```bash
./cockroach debug list-files --insecure --host=localhost:26257 --use-new-rpc
```

### Debug tsdump (tests DRPC RPC)
```bash
./cockroach debug tsdump --insecure --host=localhost:26257 --use-new-rpc --format=text
```

## Gen Commands

### Gen HAProxy config (tests DRPC RPC)
```bash
./cockroach gen haproxy --insecure --host=localhost:26257 --use-new-rpc
```

## Verification

After running commands, verify DRPC is being used by:

1. Check server logs for DRPC connection messages
2. Compare behavior with same commands without --use-new-rpc flag
3. Monitor network traffic to confirm DRPC protocol usage

## Cleanup

Stop cluster when done:
```bash
pkill -9 cockroach
rm -rf cockroach-data
```
