## `roachprod create` Test Coverage Rationale

### Strategy

The test suite uses a **two-tier approach**: a small set of deterministic tests that pin down distinct code paths, plus a single randomized test that explores the combinatorial space over time.

This design reflects two constraints:
1. Each test creates a **real GCE cluster**, so cost and runtime are proportional to test count.
2. The `roachprod create` flag space is too large to enumerate — there are 20+ flags with interacting constraints (e.g., ARM64 requires T2A machine types and T2A-compatible zones; FIPS requires AMD64; local SSD availability depends on machine type).

### Coverage map

| Code path | Deterministic test | Randomized hit rate |
|---|---|---|
| Default GCE create (local SSD, ext4, amd64) | `TestCloudCreate` | always |
| Zone pinning (`--gce-zones`) | `TestCloudCreateWithSpecificZone` | always |
| Zone:N distribution (`zone:count` syntax) | `TestCloudCreateWithZoneCounts` | never (syntax not used by randomizer) |
| ARM64 arch + T2A machine type + T2A zone restrictions | `TestCloudCreateARM64` | ~20% per run |
| FIPS arch + ZFS filesystem | `TestCloudCreateFIPS` | ~2.5% per run (5% FIPS × 50% ZFS) |
| Persistent disk (local-ssd=false, PD type/size) | `TestCloudCreatePersistentDisk` | ~50% per run |
| Spot instances | `TestCloudCreatePersistentDisk` | ~90% per run (but never asserted deterministically) |
| Multi-zone clusters | `TestCloudCreateWithZoneCounts` | ~30% per run |
| Randomized flag combinations + constraint validation | `TestCloudCreateRandomized` | by definition |

### Why this is sufficient

**Every distinct code path is covered deterministically.** The `roachprod create` command has several branching code paths based on flags — ARM64 vs AMD64 image selection, FIPS OpenSSL setup, local SSD vs persistent disk provisioning, ZFS vs ext4 filesystem setup, spot vs on-demand instance request, and zone distribution logic. Each of these branches is exercised by at least one deterministic test, so regressions in any branch are caught on every CI run regardless of random seed.

**The randomized test covers interactions.** Flag combinations like "ARM64 + spot + multi-zone + ZFS" or "FIPS + boot-disk-only + cron" are unlikely to get dedicated deterministic tests (there are hundreds of such combinations). The randomized test with constrained randomization explores this space over time, with the unit test (`TestRandomGCECreateOptions`, 100 seeds) validating that every generated configuration satisfies compatibility invariants.

**The zone:N test covers a syntax path the randomizer cannot reach.** The `zone:count` format (e.g., `us-east1-b:2,us-west1-b:3`) is a distinct CLI parsing path that the randomizer doesn't exercise, so it has its own deterministic test.

### What could be removed (not recommended)

- `TestCloudCreate` overlaps with the randomized test's default path, but it serves as the simplest possible smoke test and costs only one 3-node cluster. Removing it would save one cluster but lose the only test that verifies the absolute baseline with no special flags.
- `TestCloudCreateWithSpecificZone` partially overlaps with `TestCloudCreateARM64` (which also pins a zone), but it tests zone pinning with AMD64 machine types and verifies a different zone pool (`DefaultZones` vs `SupportedT2AZones`).

Neither removal is recommended — the marginal cost (one cluster each) is low relative to the coverage lost.

### What's intentionally not tested

- **AWS and Azure providers**: Not yet supported by the test framework. When added, they should follow the same pattern — one baseline test per provider plus provider-specific flag tests.
- **`--gce-enable-cron`** and **`--gce-boot-disk-only`**: These are low-probability GCE options (5% each in the randomizer) that don't alter fundamental VM provisioning. They're covered by the randomized test and validated by unit tests. Adding dedicated E2E tests for them would cost two more clusters with minimal additional confidence.
- **`pd-extreme` disk type**: Intentionally excluded from randomization because it requires provisioned IOPS configuration. Not tested.
- **`--gce-image` and `--os-volume-size`**: These override defaults but don't exercise meaningfully different code paths from what's already covered.
