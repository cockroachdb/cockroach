# ASIM Tests

## Generated Artifacts

ASIM tests produce artifacts under `testdata/generated/{mma,sma}/<test_name>/`.
These are gitignored — they only exist after running the tests locally with
`--rewrite`:

```bash
./dev test pkg/kv/kvserver/asim/tests -v -f TestDataDriven -- \
  --test_env COCKROACH_RUN_ASIM_TESTS=true --rewrite
```

The golden files (under `testdata/non_rand/`) only contain summary statistics
(first/last values, stddev, thrash percentages). The generated artifacts contain
the full time-series and decision traces, which are essential for understanding
*why* a test produces particular results.

### Artifact types

**`<test>_setup.txt`** — Human-readable snapshot of the initial simulation state:
cluster topology (nodes, regions, zones, vCPUs, store capacities), key space
distribution (ranges per store, leaseholders marked with `*`), scheduled events
(span config changes, mode switches), workload configuration, and changed
settings.

**`<test>_<config>_<seed>.json`** — Time-series metrics for the entire
simulation. Structure:

```json
{
  "tickInterval": 500000000,
  "metrics": {
    "<metric_name>": {
      "s1": [v0, v1, ...],
      "s2": [v0, v1, ...],
    },
  }
}
```

`tickInterval` is in nanoseconds (500000000 = 500ms per tick), so tick index
120 corresponds to 60s into the simulation. The set of metrics emitted is
controlled by the `metrics=(...)` parameter in the test's `eval` directive.

Use these to find exactly *when* convergence happened (e.g., find the first
tick where all stores have the expected replica count), or to identify
oscillation/thrash patterns that the summary statistics obscure.

The `asimview` tool (`tests/cmd/asimview/`) provides a browser-based viewer
for these JSON files. See `tests/cmd/asimview/README.md`.

**`traces/s<N>/`** — Per-store execution traces. Each file covers one
invocation of a component at a specific simulation time. Filename format:

```
<test>_<config><seed>_<time>_<component>_s<N>
```

Example: `repair_add_voter_mma-repair1_061.00s_mma.ComputeChanges_s1`

Components include `mma.ComputeChanges`, `StoreRebalancer`, and
`replicateQueue.PlanOneChange`. Each trace contains microsecond-resolution
structured log lines, for example:

```
  0.000ms   0.000ms  === operation:mma.ComputeChanges ...
  0.013ms   0.013ms  event:... mma: adding s3 with change=ADD_VOTER
```

These show which changes were proposed, which candidates were considered, and
why specific stores were chosen or rejected. Trace files are only generated for
invocations that produce log output; quiet ticks (no changes proposed) have no
trace file.

### Tips

- When a test assertion fails, look at the JSON metrics to see the actual
  trajectory: does the metric converge and then diverge, or never converge?
- When repair/rebalance behavior is surprising, grep the trace files for the
  range or store in question to see the allocator's reasoning.
- To quickly check when repair/rebalance completed, look for the last tick
  in the JSON where the metric value changes (e.g., `replicas` stops growing).
