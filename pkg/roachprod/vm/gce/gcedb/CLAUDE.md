# GCE Machine Database

This package (gcedb) acts as a static "knowledge database" for GCE machine
types. It provides a `GetMachineInfo(machineType string) (MachineInfo, error)`
method which returns:

```go
type MachineInfo struct {
  // Number of CPU cores. Always at least 1, even for shared-core machines.
  CPUCores              int
  // Amount of memory in GiB (2^30 bytes). Always at least 1.
  MemoryGiB             int
  // CPU architecture: "amd64" or "arm64".
  Architecture          string
  // Allowed number of local SSDs, in ascending order; empty if local SSD not
  // supported.
  AllowedLocalSSDCount  []int
  // List of allowed storage types, e.g. "pd-ssd", "hyperdisk-balanced".
  StorageTypes          []string
  // CPU platforms supported by this machine type, in oldest-to-newest order.
  // Empty if documentation doesn't specify exact platforms (e.g., E2).
  CPUPlatforms          []string
}
```

`MachineInfo` also has a `String()` method that returns a human-readable
representation with one field per line.

For example, for `n2-standard-32`, `GetMachineInfo()` would return:

```go
MachineInfo{
  CPUCores:             32,
  MemoryGiB:            128,
  Architecture:         "amd64",
  AllowedLocalSSDCount: []int{4, 8, 16, 24},
  StorageTypes:         []string{"pd-standard", "pd-balanced", "pd-ssd", "pd-extreme", "hyperdisk-balanced", "hyperdisk-throughput"},
  CPUPlatforms:         []string{"Intel Cascade Lake", "Intel Ice Lake"},
}
```

## Updating the Database

### IMPORTANT: Accuracy Over Code Reuse

**Prioritize correctness over code simplicity.** This database must accurately reflect
GCE's actual machine specifications. When in doubt:

1. **Use exact vCPU→value mappings instead of ranges.** For local SSD counts, each
   vCPU size has a specific allowed count. Use explicit `switch` statements with
   exact vCPU values rather than range-based logic (`vcpus <= 48`) that may be
   inaccurate for some sizes.

2. **Do NOT share functions between machine families.** Even if C4, C4A, and C4D
   look similar, they have different SSD count tables. Create separate functions
   for each family (e.g., `lssdCountC4()`, `lssdCountC4A()`, `lssdCountC4D()`).

3. **Verify against documentation for every vCPU size.** The reference implementation
   in `~/roach/pkg/roachprod/vm/gce/gcloud.go` has been validated against GCE APIs.
   When adding new machine types, verify each vCPU size individually.

4. **Add comprehensive test cases.** Each machine family should have tests for
   multiple vCPU sizes, especially boundary cases. The test file
   `testdata/machine_info` includes test cases derived from gcloud.go.

5. **When in doubt, be explicit.** A longer switch statement with correct values
   is better than a short formula that might be wrong for some cases.

To update the code, consult the following Google Cloud documentation pages:

### Primary Documentation URLs

| Topic | URL |
|-------|-----|
| Machine Overview | https://docs.cloud.google.com/compute/docs/machine-resource |
| General Purpose | https://docs.cloud.google.com/compute/docs/general-purpose-machines |
| Compute Optimized | https://docs.cloud.google.com/compute/docs/compute-optimized-machines |
| Memory Optimized | https://docs.cloud.google.com/compute/docs/memory-optimized-machines |
| Accelerator Optimized | https://docs.cloud.google.com/compute/docs/accelerator-optimized-machines |
| CPU Platforms | https://docs.cloud.google.com/compute/docs/instances/specify-min-cpu-platform |
| Local SSD | https://docs.cloud.google.com/compute/docs/disks/local-ssd |
| Hyperdisk Support | https://docs.cloud.google.com/compute/docs/disks/hyperdisks |
| Persistent Disk Support | https://docs.cloud.google.com/compute/docs/disks/persistent-disks |

### Implementation Patterns

1. **Memory calculation** (formulas OK): `memory_gib = vcpus * ratio` where ratio
   depends on family and variant. Formulas are acceptable here because memory
   scales predictably with vCPUs:
   - `standard`: typically 4 GiB/vCPU (3.75 for N1, C4)
   - `highmem`: typically 8 GiB/vCPU (6.5 for N1)
   - `highcpu`: typically 1-2 GiB/vCPU (0.9 for N1)

2. **Local SSD counts** (exact mappings required): GCE has two distinct models:
   - **Older families (N1, N2, N2D, C2, C2D, M1, M3)**: Local SSDs can be attached to
     base machine types. The allowed counts vary by vCPU count.
   - **Newer families (C3, C3D, C4, C4A, C4D)**: Base types do NOT support local SSD.
     Only `-lssd` variants (e.g., `c4a-standard-8-lssd`) support local SSD, with
     fixed (not variable) counts. For these families, `AllowedLocalSSDCount` should
     be empty for base types.

   **Important**: The full list of allowed local SSD counts for each machine type
   and vCPU configuration is documented at:
   https://docs.cloud.google.com/compute/docs/disks/local-ssd#choose_number_local_ssds

   When updating the `localSSDCounts*` and `lssdCount*` functions in the code,
   always verify against this documentation to ensure the allowed counts are accurate.

3. **Architecture**: determined by machine family:
   - ARM64: T2A, C4A, N4A
   - AMD64: all other families

4. **CPU Platforms**: The `CPUPlatforms` field lists supported CPU platforms in
   oldest-to-newest order. Each machine family typically supports one or more
   specific CPU platforms:
   - Use pre-defined platform slices (e.g., `platformsN2`, `platformsC4`) for each family
   - If documentation doesn't specify exact platforms (e.g., E2), set `CPUPlatforms` to nil
   - For families with variant-specific platforms (e.g., A3), set platform per variant
   - Shared-core machines inherit platforms from their parent family

### CPU Platforms by Family

| Family | CPU Platforms (oldest → newest) |
|--------|--------------------------------|
| N1 | Intel Sandy Bridge, Ivy Bridge, Haswell, Broadwell, Skylake |
| N2 | Intel Cascade Lake, Ice Lake |
| N2D | AMD Rome, Milan |
| N4 | Intel Emerald Rapids |
| N4A | Google Axion |
| N4D | AMD Turin |
| E2 | (empty - platform auto-selected) |
| T2A | Ampere Altra |
| T2D | AMD Milan |
| C2 | Intel Cascade Lake |
| C2D | AMD Milan |
| C3 | Intel Sapphire Rapids |
| C3D | AMD Genoa |
| C4 | Intel Emerald Rapids, Granite Rapids |
| C4A | Google Axion |
| C4D | AMD Turin |
| H3 | Intel Sapphire Rapids |
| M1 | Intel Broadwell, Skylake |
| M2 | Intel Cascade Lake |
| M3 | Intel Ice Lake |
| M4 | Intel Sapphire Rapids |
| A2 | Intel Cascade Lake |
| A3 High/Mega/Edge | Intel Sapphire Rapids |
| A3 Ultra | Intel Emerald Rapids |
| G2 | Intel Cascade Lake |

### What Typically Changes

When updating for new machine types:
- **New families** (e.g., N5, C5): Add new case in switch, define memory ratios, architecture, and CPU platforms
- **Memory ratios**: Check if ratios changed for existing families
- **Storage types**: New hyperdisk types may be added; check compatibility matrices
- **Local SSD limits**: May change with new machine sizes
- **Architecture**: New ARM-based families may be added
- **CPU platforms**: New processor generations may be added; check the machine family documentation pages for platform information

### Supported Machine Families

| Category | Families | Architecture |
|----------|----------|--------------|
| General Purpose | N1, N2, N2D, N4, N4D, E2, T2D | amd64 |
| General Purpose (ARM) | T2A, N4A | arm64 |
| Compute Optimized | C2, C2D, C3, C3D, C4, C4D, H3 | amd64 |
| Compute Optimized (ARM) | C4A | arm64 |
| Memory Optimized | M1, M2, M3, M4 | amd64 |
| Accelerator Optimized | A2, A3, G2 | amd64 |

**Important**: When updating this package, verify that all machine families documented
in https://docs.cloud.google.com/compute/docs/general-purpose-machines are accounted
for. Google periodically adds new families (e.g., N4D, N4A were added in 2025).

### Storage Types

| Type | Description |
|------|-------------|
| pd-standard | Standard persistent disk (HDD) |
| pd-balanced | Balanced persistent disk (SSD, lower cost) |
| pd-ssd | SSD persistent disk |
| pd-extreme | Extreme persistent disk (highest IOPS) |
| hyperdisk-balanced | Hyperdisk Balanced |
| hyperdisk-extreme | Hyperdisk Extreme |
| hyperdisk-throughput | Hyperdisk Throughput |
| hyperdisk-ml | Hyperdisk ML (for AI/ML workloads) |

## Machine Type Naming Conventions

### Predefined Types
Format: `{family}-{variant}-{vcpus}`
Example: `n2-standard-32`, `c3-highmem-176`

### Custom Types
Format: `{family}-custom-{vcpus}-{memory_mb}`
Example: `n2-custom-16-32768` (16 vCPUs, 32 GiB RAM)

### Extended Memory
Format: `{family}-custom-{vcpus}-{memory_mb}-ext`
Example: `n2-custom-16-65536-ext`

### Shared-Core Types
Special names: `e2-micro`, `e2-small`, `e2-medium`, `f1-micro`, `g1-small`

## Region Aggregation

When there are discrepancies between regions, the database should reflect the
**superset of supported features** (most capable configuration). This applies
to regions:
- asia-southeast1
- us-central1
- us-east1
- us-west2
- southamerica-east1
- europe-west1
- europe-west2
- europe-west3

## Testing

Run tests with:
```bash
go test -v
```

Tests use the `datadriven` package with test cases in `testdata/machine_info`.
Each test case has a machine type as the command and expects either the
`MachineInfo.String()` output or an error message. To update expected outputs
after code changes:
```bash
go test . -rewrite
```

Add test cases for new machine families as needed. Do not remove any test cases.
If a change to the code requires updates to existing tests, double-check that
the change is correct and we are not introducing a regression.

### Verification Checklist

When adding or modifying machine families, verify the following:

1. **Suffix handling**: Machine types can have `-lssd` and/or `-metal` suffixes
   (e.g., `c4a-standard-8-lssd`, `c4a-highmem-96-metal`). Ensure the parsing
   correctly handles these suffixes in any combination.

2. **Local SSD model**: Determine which local SSD model applies:
   - **Older families (N1, N2, N2D, C2, C2D, M1, M3)**: Base types support
     attaching local SSDs with varying counts.
   - **Newer families (C3, C3D, C4, C4A, C4D)**: Base types do NOT support
     local SSD. Only `-lssd` variants do, with fixed (not variable) counts.

3. **Test coverage**: Add test cases for both base types AND `-lssd` variants
   to verify the local SSD behavior is correct for both.

4. **CPU platforms**: Check the machine family documentation for CPU platform
   information. Add the appropriate `platformsXXX` slice assignment and update
   the "CPU Platforms by Family" table in this document.
