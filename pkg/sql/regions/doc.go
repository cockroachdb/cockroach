// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

/*
Package regions provides zone config synthesis for CockroachDB's multi-region
databases. Given a RegionConfig (from
pkg/sql/catalog/multiregion) and a table's locality, this package
deterministically produces the zone configuration that enforces the desired
replica placement.

# Zone Config Synthesis Overview

Multi-region zone configs are generated, not hand-crafted. The synthesis
functions take a RegionConfig and a table locality as input and produce a
ZoneConfig as output. Because the mapping is deterministic, the system can
validate that a table's current zone config matches what would be generated
from the current RegionConfig — any divergence means the user has manually
modified the zone config.

The two primary entry points are:

  - ZoneConfigForMultiRegionTable: Generates zone configs for GLOBAL and
    REGIONAL BY TABLE tables. For REGIONAL BY ROW tables, this function
    returns an empty zone config because placement is handled per-partition.

  - ZoneConfigForMultiRegionPartition: Generates zone configs for individual
    partitions of REGIONAL BY ROW tables. Each partition corresponds to a
    region and gets its own zone config as a subzone of the table.

At minimum, both functions set num_voters, voter_constraints, and
lease_preferences at the table or partition level. They also set
num_replicas and constraints when the table belongs to a super region
or cannot inherit these from the database-level zone config.

# Replica Counts

GetNumVotersAndNumReplicas computes replica counts based on the survival
goal, the number of regions, and the placement policy:

Zone survival (SURVIVE ZONE FAILURE):

  - 3 voters, all placed in the home region.
  - Total replicas = 3 voters + 1 non-voter per additional region. For
    example, a 3-region database has 3 voters + 2 non-voters = 5 replicas.
  - With PLACEMENT RESTRICTED, non-voters are suppressed:
    num_replicas = num_voters = 3.

Region survival (SURVIVE REGION FAILURE):

  - 5 voters, distributed across regions (see below).
  - Total replicas = 2 replicas in the home region + 1 replica in each of
    the other (numRegions - 1) regions. For 3 regions: 2 + 2 = 4. Since
    num_replicas must be >= num_voters (5), the result is clamped to 5.
  - A secondary region, if configured, receives an additional replica.

MaxFailuresBeforeUnavailability computes the quorum threshold: for N voters,
the maximum tolerable failures are (N+1)/2 - 1. For 5 voters this is 2,
meaning a majority quorum of 3 is needed.

# Voter Constraint Synthesis

SynthesizeVoterConstraints generates the voter_constraints field. The
strategy differs by survival goal:

Zone survival: All voters are constrained to the home region. The
constraint has no NumReplicas, meaning all voters must satisfy it. This is
sufficient for zone survivability because the KV allocator's diversity
heuristic spreads voters across availability zones within the region. The
result looks like:

	voter_constraints: [{constraints: [+region=home]}]

Region survival: Exactly (quorum - 1) voters are pinned to the home
region. For 5 voters, this is 2. The remaining 3 voters "float" — the
allocator distributes them across other regions using its diversity
heuristic, resulting in a 2-2-1 distribution across 3 regions. This
ensures that even if the home region is lost, a majority quorum can still
be formed from the remaining regions. The result looks like:

	voter_constraints: [{num_replicas: 2, constraints: [+region=home]}]

When a secondary region is configured (and the home region differs from
the secondary), a second entry pins (quorum - 1) voters to the secondary
region as well, for deterministic failover:

	voter_constraints: [
	  {num_replicas: 2, constraints: [+region=home]},
	  {num_replicas: 2, constraints: [+region=secondary]},
	]

# Non-Voter Replica Constraints

SynthesizeReplicaConstraints generates the constraints field (which governs
both voters and non-voters). The behavior depends on placement policy:

DEFAULT placement: One replica is required in every database region.
This ensures low-latency follower reads from any region.

	constraints: [
	  {num_replicas: 1, constraints: [+region=A]},
	  {num_replicas: 1, constraints: [+region=B]},
	  {num_replicas: 1, constraints: [+region=C]},
	]

RESTRICTED placement: No constraints are set (nil). Only voters are
placed, and they all go in the home region. This eliminates non-voting
replicas, reducing replication cost at the expense of cross-region read
performance.

# Lease Preferences

SynthesizeLeasePreferences generates the lease_preferences field, which
tells the KV allocator where to place the leaseholder. The primary
preference is the home region. If a secondary region is configured, it
is added as a fallback preference:

	lease_preferences: [
	  [+region=home],
	  [+region=secondary],
	]

# Super Region Constraint Scoping

When data is homed in a region that belongs to a super region (see
pkg/sql/catalog/multiregion for the conceptual model),
AddConstraintsForSuperRegion confines all replicas to the super region's
member regions by:

 1. Looking up the super region's member regions and effective survival
    goal (which may differ from the database-level goal if the super region
    has its own goal).
 2. Computing num_voters and num_replicas for the scoped region set.
 3. Checking whether zone config extensions will override num_replicas,
    and using the effective value for constraint generation.
 4. Distributing replicas across the super region's member regions using
    distributeReplicasAcrossRegions.

The distribution strategy depends on the survival goal:

Zone survival within a super region: The affinity (home) region receives
at least num_voters replicas (since voter_constraints pin all voters
there). Remaining replicas (non-voters) are distributed across other
member regions. With RESTRICTED placement there are no remaining replicas.

Region survival within a super region: Replicas are distributed evenly
across all member regions, with the affinity region receiving priority
for any remainder. This ensures no single region holds a majority of
replicas, so the loss of any one member region does not prevent quorum.

For example, a 3-region super region with region survival and 5 replicas
distributes them 2-2-1, with the affinity region getting 2.

# Zone Config Extensions

Zone config extensions (see pkg/sql/catalog/multiregion for the precedence
model) are applied during synthesis via
RegionConfig.ExtendZoneConfigWithRegionalIn and
ExtendZoneConfigWithGlobal. Extensions are not allowed to set
lease_preferences at the regional or super region level, since lease
preferences are synthesized from the affinity region and must remain
consistent with the domiciling guarantees.

# Zone Config Validation

ValidateZoneConfigForMultiRegionTable validates that a table's zone config
matches what would be generated from the current RegionConfig. It:

  - Filters out subzones for transitioning regions and indexes (which may
    not yet reflect the target configuration).
  - Compares the current and expected zone configs using DiffWithZone,
    checking only the multi-region protected fields
    (zonepb.MultiRegionZoneConfigFields).
  - Produces specific errors for mismatched fields, missing subzones, and
    extra subzones.

This validation is shared between the legacy and declarative schema changers
via the ZoneConfigForMultiRegionValidator and MultiRegionTableValidatorData
interfaces.

# GLOBAL Tables

GLOBAL table zone config synthesis differs from regional tables:

  - Voters are constrained to the primary region.
  - Non-voters are placed in all other regions (always DEFAULT placement,
    even if the database uses RESTRICTED).
  - Not constrained by super regions.

If the database configuration allows it
(GlobalTablesInheritDatabaseConstraints), a GLOBAL table inherits its
constraints from the database zone config and only sets GlobalReads.
Otherwise, it sets its own full zone config.

# Region Provider

This package also provides the Provider type (implementing
descs.RegionProvider), which offers a live view of which regions the
cluster can serve and is used during DDL planning to verify region
availability.
*/
package regions
