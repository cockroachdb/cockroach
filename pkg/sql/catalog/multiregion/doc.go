// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

/*
Package multiregion provides the core abstractions for CockroachDB's
multi-region SQL layer. It defines the read-only RegionConfig struct that
represents a database's user-configured multi-region state and the utilities
that consume it.

# Multi-Region Architecture

CockroachDB's multi-region implementation spans several packages. This package
sits at the center, providing the metadata layer that other packages depend on.
The key packages and their roles:

  - pkg/sql/catalog/multiregion (this package) — Defines RegionConfig, the
    central read-only abstraction that captures a database's multi-region state:
    regions, primary and secondary regions, survival goal, placement policy,
    super regions, and zone config extensions. All downstream operations
    consume a RegionConfig rather than reading descriptors directly.

  - pkg/sql/regions — Zone config synthesis engine. Given a RegionConfig and
    a table's locality, it deterministically produces the zone configuration
    that enforces the desired replica placement. This includes voter and
    non-voter constraint generation, lease preference synthesis, and
    super-region-aware constraint scoping.

  - pkg/sql/regionliveness — Region availability probing. Uses the
    system.region_liveness table to detect and quarantine unavailable regions.
    This is consulted before DDL operations to prevent changes that reference
    unreachable regions.

  - pkg/ccl/multiregionccl — Contains the multi-region initialization.
    Constructs the initial RegionConfig when a database first becomes
    multi-region (CREATE DATABASE ... PRIMARY REGION or ALTER DATABASE ...
    SET PRIMARY REGION). This package also determines the sorted insertion
    position for new region enum values.

# Data Flow

The typical flow when multi-region state changes is:

 1. The database descriptor and the multi-region enum type descriptor store
    the persistent multi-region state. Regions are represented as enum values
    in a special enum type descriptor associated with the database. This
    enum-based representation enables transactional region addition and
    removal: a region being added has its enum member Direction set to ADD;
    once the transition completes, the Direction becomes NONE and the
    member becomes fully public. A region being removed transitions through
    the REMOVE direction before the member is dropped entirely.

 2. SynthesizeRegionConfig reads those descriptors and produces a RegionConfig.
    This is an immutable snapshot of the database's multi-region state at that
    point in time. For DDL operations, the config is synthesized fresh (not
    cached) so it reflects in-flight changes. For DML operations, a cached
    version may be used.

 3. The zone config synthesis layer (pkg/sql/regions) consumes the RegionConfig
    along with each table's locality to produce zone configurations. These zone
    configs specify voter constraints, non-voter constraints, replica counts,
    and lease preferences that enforce the multi-region semantics.

 4. Zone configs are applied to the database and all affected tables, either
    directly during DDL execution or via the databaseRegionChangeFinalizer
    that runs after region enum value transitions complete.

# RegionConfig

Key fields of RegionConfig:

  - regions: The set of PUBLIC regions in the database. These are the regions
    currently available for replica placement.
  - transitioningRegions: Regions being added or removed (in-flight enum
    transitions). These are tracked separately so that DDL validation can
    account for them.
  - addingRegions: The subset of transitioningRegions that are specifically
    being added (not removed). Exposed via the AddingRegions() method.
  - regionEnumID: The ID of the multi-region enum type descriptor that
    this RegionConfig was synthesized from. Links the config back to its
    source descriptor and is used during validation.
  - primaryRegion: The database's primary region. Tables without an explicit
    home region default to this region for leaseholder placement.
  - secondaryRegion: An optional failover region. If the primary region becomes
    unavailable, leaseholders move to the secondary region rather than to an
    arbitrary region.
  - survivalGoal: Either SURVIVE ZONE FAILURE (default) or SURVIVE REGION
    FAILURE. This controls how many voters and replicas are created and how
    they are distributed.
  - placement: Either DEFAULT or RESTRICTED. RESTRICTED suppresses non-voting
    replicas outside of the home region or super region, reducing replication
    cost at the expense of cross-region read performance.
  - superRegions: Named groupings of regions that define domiciling boundaries.
    See the Super Regions section below.
  - zoneCfgExtensions: Per-locality zone config overrides that let users
    customize zone config properties at different granularities.

# Table Localities

Multi-region databases support three table locality types, each of which
produces different zone configurations:

GLOBAL tables replicate data to all database regions with non-blocking
transactions (GlobalReads = true). Reads are fast everywhere because every
region has a local replica. Writes pay the cost of writing to all regions.
GLOBAL tables are not constrained by super regions — they intentionally
replicate everywhere.

REGIONAL BY TABLE tables have an affinity to a single region (either the
database primary region or an explicitly specified region). Leaseholders are
placed in the affinity region. Voters are placed according to the survival
goal: for zone survival, all voters go in the home region; for region
survival, voters are distributed across regions to tolerate a region loss.

REGIONAL BY ROW tables are partitioned by a crdb_region column. Each
partition behaves like a REGIONAL BY TABLE with affinity to the partition's
region. This enables data domiciling: different rows in the same table can
reside in different regions, and when super regions are configured, each
row's replicas are confined to the super region that contains its region.

# Super Regions

A super region is a named grouping of database regions that defines a
domiciling boundary. When data is homed in a region that belongs to a super
region, all replicas — both voters and non-voters — are confined to the
member regions of that super region. This is the foundation for data
domiciling use cases where regulatory requirements mandate that data stays
within a geographic or legal boundary.

Super regions are orthogonal to the survival goal. A super region with zone
survivability confines replicas to its member regions and tolerates zone
loss within those regions. A super region with region survivability also
confines replicas but tolerates the loss of an entire member region. In
both cases, no replica is placed outside the super region.

There are two kinds of super regions:

Explicit super regions are created by the user via ALTER DATABASE ... ADD
SUPER REGION. They are stored in the multi-region enum type descriptor and
appear in RegionConfig.SuperRegions(). A region may belong to at most one
explicit super region.

Implicit super regions arise when a database has exactly three regions and
uses SURVIVE REGION FAILURE. In this case, the system treats all three
regions as a single implicit super region so that tables receive explicit
per-region replica constraints (e.g. a 2+2+1 voter distribution) rather
than inheriting the looser database-level zone config. This produces more
deterministic replica placement across the three regions.
HasImplicitSuperRegion returns true in this case.

RegionConfig provides methods to query super region membership, retrieve
the member regions of a containing super region, and determine the
effective survival goal for a given region (the super region's own goal
if set, otherwise the database-level goal).

Super regions interact with table localities as follows:

  - REGIONAL BY ROW: Each row's replicas are confined to the super region
    containing the row's crdb_region value. Different rows may be in
    different super regions within the same table.
  - REGIONAL BY TABLE: Replicas are confined to the super region containing
    the table's home region.
  - GLOBAL: Unaffected by super regions. GLOBAL tables replicate everywhere.

# Zone Config Extensions

Zone config extensions allow users to customize zone config properties at
different levels of the multi-region hierarchy. Extensions are applied in
a fixed precedence order during zone config synthesis:

 1. Regional — Applies to all REGIONAL tables and partitions.
 2. Super Region — Applies to tables/partitions homed in a specific super
    region. Overrides the regional extension.
 3. Regional In — Applies to tables/partitions homed in a specific region.
    Overrides both the regional and super region extensions.

For GLOBAL tables, only the global extension applies.

Extension inheritance works by having the extension "fill in" any fields
not explicitly set from the base zone config, then replacing the base
with the result. This means an extension only overrides the fields it
explicitly sets.

The ExtendZoneConfigWithRegionalIn method applies all three regional tiers
in order. The ExtendZoneConfigWithGlobal method applies the global
extension. Both methods validate that the resulting zone config does not
violate survival goal requirements (e.g., num_voters cannot be reduced
below the minimum required for the survival goal).

# Validation

RegionConfig provides several validation functions:

  - ValidateRegionConfig: Validates overall consistency — region enum ID is
    set, at least one region exists, placement and survival goal are
    compatible, super regions are valid, and zone config extensions reference
    valid regions.
  - ValidateSuperRegions: Validates that super region names are unique and
    sorted, regions within each super region are unique, sorted, and belong
    to the database, no region appears in multiple super regions, and each
    super region has enough regions for its effective survival goal.
  - CanSatisfySurvivalGoal: Checks whether the given number of regions is
    sufficient for the survival goal (REGION_FAILURE requires at least 3).
  - CanDropRegion: Checks whether dropping a region would violate the
    survival goal or break a super region membership constraint.
*/
package multiregion
