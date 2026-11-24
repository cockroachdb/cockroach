# Investigating Synthetic Failure Injection

Use when test failures involve synthetic fault injection (network partitions, node kills, disk failures).

Synthetic failures are injected as part of larger tests: this guide has tips for investigating the
failure injection behavior in particular, but should be used *in conjunction with* the guide for the
applicable test type.

Tests inject synthetic failures to verify resilience and recovery behavior. It is important to
distinguish between expected recovery responses and actual product bugs.

## Critical: Partition Documentation

When network partitions are present, **always** include a "Network Partition Analysis" section in
`investigation.md` that specifies:

**Communication topology for each partition** - be explicit about which nodes can/cannot reach
which:
```
## Network Partition Analysis

### Partition 1: 14:23:15 - 14:28:30 (5m 15s)
**Communication groups:**
- Group A: n1, n2 (can reach each other)  
- Group B: n3, n4, n5 (can reach each other)
- Cross-group: Group A cannot reach Group B

### Partition 2: 14:45:22 - 14:47:10 (1m 48s)
**Communication groups:**
- Isolated: n5 (cannot reach any other node)
- Connected: n1, n2, n3, n4 (can reach each other, cannot reach n5)
```

**Timeline references** - link partition events to observed behaviors:
```
14:23:15 - Partition begins (see Partition 1)
14:23:18 - Raft leadership lost on ranges 1-50 (n1,n2 had replicas)
```

## Expected vs Unexpected Behavior

**Expected during synthetic failures:**
- Temporary unavailability, leadership changes, job stalls
- Automatic recovery after fault injection ends

**Investigate further if:**
- Failures persist after fault injection ends
- Data inconsistencies appear post-recovery
- Recovery takes excessive time

## Quick Identification

```bash
# Check for failure injection
grep -E "partition|kill|inject|mutator|chaos" test.log

# Map partition timeline  
grep -E "partition|network.*split" test.log | grep -E "\d{2}:\d{2}:\d{2}"

# Find kill events
grep -E "SIGTERM|SIGKILL|kill.*node" test.log
```