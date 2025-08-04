# High-Speed Vector Index Backfill Design

## Executive Summary

This design document outlines a new approach for dramatically improving the
performance of vector index backfill operations in CockroachDB. The current
implementation processes vectors one-at-a-time during backfill, which is too
slow for large datasets. This proposal introduces a k-means clustering approach
that pre-computes optimal partition structures through a new declarative schema
changer step, allowing for high-throughput bulk insertion during the subsequent
backfill phase.

## Current State Analysis

### Existing Vector Index Architecture

CockroachDB implements the **C-SPANN** (CockroachDB Scalable Partitioned
Approximate Nearest Neighbor) algorithm with the following characteristics:

- **Hierarchical Structure**: K-means tree with quantized vectors (~100 vectors
  per partition)
- **Distance Metrics**: L2Squared, Cosine, and InnerProduct
- **Sophisticated Operations**: Atomic split/merge with 15-step state
  transitions
- **Background Maintenance**: Asynchronous fixup processors for partition
  optimization

### Current Backfill Process Issues

The existing backfill process (`pkg/sql/backfill/backfill.go:497-540`) has
significant performance bottlenecks:

1. **Vector-by-Vector Processing**: Each vector requires individual partition
   lookup and re-encoding
2. **Search Overhead**: Repeated `SearchForInsert` operations with unstable
   centroids during backfill
3. **Re-encoding Complexity**: Dynamic quantization with partition-specific
   centroids
4. **Limited Batching**: Current approach doesn't leverage batch processing
   effectively

### Performance Characteristics

- **Current Approach**: Performance varies significantly with vector dimensions
  and dataset size
- **Scalability Issues**: Performance degrades with large datasets due to
  individual vector processing
- **Resource Usage**: High CPU overhead from repeated search operations during
  backfill

### Current Schema Changer Flow

Vector indexes currently follow the standard secondary index creation path:

```
ABSENT → BACKFILL_ONLY → BACKFILLED → DELETE_ONLY → MERGE_ONLY → MERGED →
WRITE_ONLY → VALIDATED → PUBLIC
```

This flow processes through the declarative schema changer's operation
generation system defined in
`pkg/sql/schemachanger/scplan/internal/opgen/opgen_secondary_index.go`.

## Proposed Solution: Declarative Schema Changer Integration

### Architecture Overview

Rather than modifying the general backfill infrastructure, we propose adding
a **new schema changer step** that pre-builds the vector index structure before
the standard backfill phase. This keeps vector-specific logic isolated and
follows existing schema changer patterns.

### New Schema Changer Step: Structure Building

The new `STRUCTURE_BUILDING` step performs one or more full scans over the
primary index to construct the complete vector index hierarchy before any
backfill operations begin. This approach:

**Primary Index Scanning Strategy:**
- **Single-Pass Collection**: Ideally, one scan over the primary index
  collects both ~1% of vectors for initial centroids (`randint() % 100 == 0`)
  and constructs multiple batches of 1000 random vectors using reservoir
  sampling
- **Two-Pass Fallback**: If row count estimation is needed to determine optimal
  batch count (N), perform two scans: first for row estimation and centroid
  collection, second for batch construction
- **Batch-Based Refinement**: Use collected batches for mini-batch k-means to
  refine centroids until convergence
- **Interior Partition Construction**: Final step builds interior partitions
  based on the optimized leaf centroids, handling prefixed indexes
  appropriately
- **Shared Structure**: The resulting hierarchy is used by both the new vector
  index and any temporary indexes created during the process

**Key Benefits:**
- **Stable Partition Structure**: Enables effective caching of
  `SearchForInsert` results due to stable centroids
- **Bulk Adder Compatibility**: Pre-built structure allows use of standard
  bulk adders (vastly faster than transactional inserts)
- **Reduced Vector-Specific Code**: Minimizes vector-specific logic in general
  backfill and merge infrastructure
- **Shared Structure Usage**: Both new and temporary indexes use the same
  optimized structure, avoiding metadata duplication
- **Automatic Fixup Avoidance**: Fixup logic naturally avoids indexes under
  construction without explicit disabling
- **Atomic Construction**: Structure building is atomic - either completes
  fully or rolls back cleanly

### Enhanced Schema Changer Flow

We introduce new statuses `STRUCTURE_BUILDING` and `STRUCTURE_BUILT` between
`ABSENT` and `BACKFILL_ONLY`:

```
ABSENT → **STRUCTURE_BUILDING** → **STRUCTURE_BUILT** → BACKFILL_ONLY →
BACKFILLED → DELETE_ONLY → MERGE_ONLY → MERGED → WRITE_ONLY → **VALIDATED** →
PUBLIC
```

**Note**: The `VALIDATED` step includes both standard index validation and
vector-specific partition size validation to ensure merged partitions haven't
become oversized.

#### STRUCTURE_BUILDING Phase

During this phase, the schema changer performs the computationally intensive
work of building the optimal partition structure for the vector index:

1. **Single-Pass Data Collection**: Performs one scan over the primary index,
   simultaneously collecting ~1% of vectors as initial centroids
   (`randint() % 100 == 0`) and constructing N batches of random vectors using
   reservoir sampling. If row count is unknown, falls back to two-pass
   approach.
2. **Batch-Based Refinement**: Uses the pre-collected batches to refine
   initial centroids with mini-batch k-means until convergence
3. **Memory Management**: Maintains configurable memory limits (default 600MB)
   with LRU caching and disk spilling for large datasets
4. **Admission Control Integration**: Integrates with CockroachDB's existing
   admission control infrastructure for background operations, using elastic CPU
   work handles to respect cluster resource limits and prevent system overload
   during CPU-intensive k-means computation
5. **Prefixed Index Support**: Handles prefixed indexes by maintaining
   separate centroid sets for each unique prefix
6. **Convergence Detection**: Monitors centroid changes across iterations to
   determine when the clustering has stabilized

This phase is the most resource-intensive part of the process, involving
either one or two scans over potentially large datasets and iterative
mini-batch k-means computation.

**Scanning Strategy Details:**
- **Single-Pass Preferred**: When row count can be estimated (from table
  statistics), collect both initial centroids and random batches in one scan
  using reservoir sampling
- **Two-Pass Fallback**: When row count is unknown, first scan estimates rows
  and collects centroids, second scan collects optimal number of random
  batches
- **Batch Count Optimization**: Number of batches (N) is calculated as
  min(estimated_rows/batch_size, memory_limit/(batch_size*vector_bytes)) to
  balance memory usage and convergence quality

#### STRUCTURE_BUILT Phase

This phase indicates that the partition structure has been successfully
constructed and is ready for use:

1. **Complete Hierarchy Available**: All leaf and interior partitions have
   been created with stable centroids
2. **Interior Partition Construction**: Builds the hierarchical tree structure
   on top of the leaf centroids, creating interior partitions that enable
   efficient tree traversal
3. **Temporary Index Configuration**: Sets up temporary indexes to use the
   same partition structure as the main index, avoiding metadata duplication
4. **Fixup Avoidance**: Ensures background fixup processes do not interfere
   with the carefully constructed structure
5. **Ready for Backfill**: The index is now ready to transition to
   `BACKFILL_ONLY` where standard bulk loading can begin

The transition from `STRUCTURE_BUILT` to `BACKFILL_ONLY` marks the handoff
from vector-specific construction logic to the standard, high-performance bulk
loading infrastructure.

#### VALIDATED Phase Enhancement

For vector indexes, the `VALIDATED` phase includes additional vector-specific
validation:

1. **Standard Index Validation**: Row count verification and uniqueness
   checking (same as other indexes)
2. **Partition Size Validation**: Checks that merged partitions haven't
   exceeded recommended size limits
3. **Automatic Fixup Triggering**: Optionally triggers immediate partition
   splits for oversized partitions
4. **Non-Blocking**: Validation warnings don't prevent index from becoming
   `PUBLIC`

This ensures that the merge process hasn't created problematic partition
sizes that could affect search performance.

## Integration with Existing Systems

### Schema Changer Integration

The new structure building phases integrate cleanly with the existing
declarative schema changer framework:

#### New Schema Operations
Two new operation types are added to handle vector index structure building:
- `BuildVectorIndexStructure`: Performs the actual k-means clustering and
  partition construction
- `MarkVectorIndexStructureBuilt`: Marks the structure building as complete
  and ready for backfill

#### Status Transitions
The new statuses follow existing schema changer patterns, with dependency
rules ensuring proper ordering and atomicity. The schema changer's existing
transaction and rollback mechanisms handle failures during structure building.

#### Vector-Specific Logic Isolation
Only vector indexes follow the new `STRUCTURE_BUILDING` → `STRUCTURE_BUILT`
path and include additional partition validation. Regular secondary indexes
bypass these phases entirely, maintaining full backward compatibility.

### C-SPANN Integration

The structure building algorithm leverages some existing components from the
`pkg/sql/vecindex/cspann` package while implementing new mini-batch k-means
components:

#### Reused Components
- **Workspace Management**: Existing memory allocation system for temporary
  vectors and computations
- **Quantizer Infrastructure**: Proven quantization algorithms (RaBitQ, etc.)
  for vector encoding
- **Distance Metrics**: Support for L2Squared, Cosine, and InnerProduct
  distance functions
- **Partition Structures**: Existing PartitionKey, TreeKey, and metadata
  infrastructure

#### New Components
- **MiniBatchKMeans**: New mini-batch k-means implementation separate from
  BalancedKMeans, supporting k > 2 and different memory management
- **BulkKmeansBuilder**: Coordinator for bulk construction using the new
  mini-batch algorithm
- **Random Sampling**: Implementation of the `randint() % 100 == 0` sampling
  strategy for initial centroids
- **Reservoir Sampling**: Implementation for collecting random batches during
  single-pass scanning
- **Batch Processing**: Memory-bounded batch processing system with LRU
  caching and disk spilling
- **Convergence Detection**: Monitoring system for k-means iteration
  termination

### Backfill Infrastructure Integration

The pre-built structure enables vector indexes to use standard bulk loading
infrastructure without modification:

#### Elimination of Vector-Specific Backfill Code
- **No VectorIndexHelper.ReEncodeVector**: This function is completely
  removed from the backfill process
- **Standard Chunk Construction**: Vector indexes use the same chunk building
  process as regular secondary indexes
- **Bulk Adder Compatibility**: Pre-built structure allows direct use of
  high-performance bulk adders

#### Temporary Index Structure Sharing
- **Shared Partition Metadata**: Temporary indexes reference the main index's
  partition structure directly
- **Reduced Write Operations**: Eliminates the need to copy partition metadata
  during merge operations
- **Memory Efficiency**: Single structure serves both temporary and main
  indexes

#### Fixup Process Integration
- **Automatic Avoidance**: Existing fixup logic is modified to skip indexes
  under construction (non-PUBLIC status)
- **No Explicit Disabling**: No need for special fixup disable/enable
  operations
- **Natural Integration**: Works with existing `fixupProcessor.Enqueue()`
  logic

### Performance Benefits

#### Bulk Adder Performance
The pre-built partition structure enables vector indexes to use the same
high-performance bulk adders that other secondary indexes use, providing
orders of magnitude performance improvements over transactional inserts.

#### Stable Centroid Caching
With centroids remaining stable during backfill, `SearchForInsert` can use
cached partitions, which should provide a large performance boost.

#### Reduced Memory Pressure
Structure sharing between temporary and main indexes reduces memory usage and
increases cache hit rates, especially beneficial for large datasets where
partition metadata might not fit entirely in memory.

## Key Design Decisions

**Single-Pass Data Collection**: Based on feedback, the design prioritizes
collecting both initial centroids and random vector batches in a single scan
using reservoir sampling. This approach:
- Minimizes I/O overhead by avoiding multiple full table scans
- Uses reservoir sampling to ensure randomness without knowing exact row
  counts
- Falls back to two-pass collection only when row count estimation is
  unavailable
- Calculates optimal batch count (N) based on memory limits and estimated
  dataset size

**Separate Mini-Batch K-Means Implementation**: Rather than adapting the
existing BalancedKMeans component, a new MiniBatchKMeans implementation is
required because:
- Mini-batch k-means supports k > 2 (vs. binary splits in BalancedKMeans)
- Different memory management patterns for batch processing
- Simplified algorithm focused on bulk structure building rather than
  incremental updates
- Avoids complexity of adapting binary k-means for multi-centroid scenarios

## Benefits of Schema Changer Integration

This approach provides several key advantages:

1. **Bulk Adder Utilization**: With pre-built structure, backfill can use
   standard bulk adders instead of slow transactional inserts
2. **Minimal Vector-Specific Code**: Reduces vector-specific logic in general
   backfill infrastructure to near zero
3. **Zero Vector Code in Merge**: Merge operations require no vector-specific
   modifications at all
4. **Clean Separation**: Vector-specific logic remains isolated in the
   structure building phase
5. **Standard Patterns**: Follows existing schema changer
   operation/status/rule patterns exactly
6. **Backwards Compatible**: Non-vector indexes are completely unaffected
7. **Atomic and Transactional**: Leverages existing transaction boundaries
   and rollback mechanisms
8. **Progress Tracking**: Gets checkpointing and monitoring infrastructure
   for free
9. **Parallel Execution**: Can leverage existing parallel operation execution
   framework

## Performance Improvements

### Bulk Adder Performance Benefits

The pre-built partition structure enables vector indexes to use the same
high-performance bulk adders that other secondary indexes use:

- **Bulk vs Transactional**: Bulk adders are orders of magnitude faster than
  transactional inserts
- **Standard Infrastructure**: Leverages existing, highly optimized bulk
  loading code paths
- **No Special Cases**: Vector indexes follow the same efficient patterns as
  regular secondary indexes
- **Merge Compatibility**: Merge operations work identically for vector and
  non-vector indexes

This architectural alignment means vector index creation benefits from all
existing performance optimizations in the bulk loading infrastructure.

### Expected Performance Gains

| Metric | Current | Proposed | Expected Improvement |
|--------|---------|----------|---------------------|
| Throughput | Vector-by-vector processing | Bulk processing with pre-built structure | Significant increase |
| CPU Usage | High (search overhead) | Lower (batch processing) | Substantial reduction |
| Memory Usage | Variable | Predictable with configurable limits | More efficient |
| Scalability | Degrades with dataset size | Linear scaling with dataset size | Better scalability |

### Benchmarking Strategy

Performance improvements will be measured through comprehensive benchmarking
across different dataset sizes and vector dimensions. Actual performance
targets will be established through empirical testing during implementation.

## Implementation Phases

### Phase 1: Schema Changer Structure Pre-building
- Add new `STRUCTURE_BUILDING` and `STRUCTURE_BUILT` statuses to schema
  changer
- Implement vector index partition pre-building with k-means clustering
- Add operation generation rules for vector indexes only
- Ensure existing backfill continues to work even with new structure
  building step
- Create dependency rules and execution framework integration

### Phase 2: Fixup Management and Bulk Adder Integration
- Disable fixups while vector index is in construction states
  (non-PUBLIC)
- Modify backfill process to detect pre-built partition structure
- Enable BulkAdder usage when stable structure is available
- Maintain backward compatibility with existing vector index backfill
- Add performance monitoring to measure BulkAdder improvements

### Phase 3: High-Speed Caching Vecstore
- Create caching vecstore implementation for fast partition lookups
- Implement in-memory cached tree structure with LRU eviction
- Add disk spilling for large partition metadata that exceeds memory
- Integrate cached lookups with `SearchForInsert` operations
- Optimize for stable centroids during backfill phase

### Phase 4: Shared Structure and Merger Simplification
- Implement temporary index structure sharing with main index
- Modify vector mutation logic to use shared caching vecstore
- Remove vector-specific code from merge operations
- Eliminate duplicate partition metadata writes during merge
- Ensure memory efficiency through shared partition structure

### Phase 5: Vector Index Validation
- Add partition size validation to `VALIDATED` step
- Implement oversized partition detection after merge operations
- Add configurable response to validation issues (warn/fixup/fail)
- Create automatic fixup triggering for problematic partitions
- Ensure validation is non-blocking for index availability

## Configuration & Tuning

### Cluster Settings

The high-speed vector index backfill can be configured through several cluster
settings:

```sql
-- Enable random sampling structure building
SET CLUSTER SETTING vecindex.structure_building.enabled = true;

-- Configure memory limit for batch processing
SET CLUSTER SETTING vecindex.structure_building.memory_limit = '600MB';

-- Configure random sampling rate for initial centroids (1% = randint() % 100 == 0)
SET CLUSTER SETTING vecindex.structure_building.initial_centroid_rate = 100;

-- Configure batch processing parameters
SET CLUSTER SETTING vecindex.structure_building.batch_size = 1000;
SET CLUSTER SETTING vecindex.structure_building.max_iterations = 50;

-- Enable partition validation during VALIDATED step
SET CLUSTER SETTING vecindex.validation.enabled = true;
```

### Key Parameters

- **`structure_building.enabled`**: Global enable/disable for the new structure building approach
- **`structure_building.memory_limit`**: Memory limit for k-means batch processing and caching
- **`structure_building.initial_centroid_rate`**: Sampling rate for initial centroids (100 = 1%)
- **`structure_building.batch_size`**: Number of vectors per mini-batch k-means iteration
- **`validation.enabled`**: Enable post-merge partition size validation

## Observability & Progress Tracking

The STRUCTURE_BUILDING phase can take significant time for large tables,
requiring progress visibility equivalent to standard index backfill operations.

### Progress Tracking

Vector index structure building integrates with CockroachDB's existing job
progress infrastructure, providing the same level of visibility as index
backfill.

#### Job Progress Integration

The structure building process reports progress through the standard schema
changer job system:

```go
// Reports fraction_completed and status_message like existing backfill
func updateStructureBuildingProgress(
    ctx context.Context, job *jobs.Job, phase string, progress float32, details string,
) error {
    return job.Update(ctx, func(txn *client.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
        // Update fraction completed (0.0 to 1.0)
        ju.UpdateProgress(jobspb.Progress{
            Progress: &jobspb.Progress_FractionCompleted{
                FractionCompleted: progress,
            },
            StatusMessage: fmt.Sprintf("vector index structure building: %s", details),
        })
        return nil
    })
}
```

#### Progress Phases

Structure building progress is divided into phases with weighted progress:

1. **Scanning Phase (60% of total)**: Primary index scanning and vector sampling
2. **Clustering Phase (35% of total)**: K-means clustering iterations
3. **Hierarchy Phase (5% of total)**: Interior partition construction

#### SHOW JOB Integration

Vector index structure building appears in `SHOW JOB` output with status:

```sql
-- Example SHOW JOB output during structure building
SHOW JOB 12345;

       job_id | job_type | ... | fraction_completed | status
    ----------+----------+-----+--------------------+----------------------------------------
     12345    | SCHEMA   | ... | 0.42              | vector index structure building: scanning primary index (42%)
```

### Status Messages

Clear status messages provide phase-specific progress information:

```
-- Scanning phase examples
"vector index structure building: scanning primary index (42%)"
"vector index structure building: sampling vectors for clustering (67%)"

-- Clustering phase examples
"vector index structure building: k-means clustering iteration 12/50 (75%)"
"vector index structure building: refining centroids (89%)"

-- Hierarchy phase examples
"vector index structure building: creating partition hierarchy (95%)"
"vector index structure building: complete (100%)"
```

This provides customers with the same progress visibility they have for standard
index backfill operations.

## Conclusion

This design represents a significant advancement in vector index construction
performance while maintaining architectural integrity. By integrating cleanly
with existing schema changer patterns and leveraging proven C-SPANN components,
the approach delivers substantial performance improvements with minimal risk.

The phased implementation ensures manageable development while the structure
sharing approach eliminates redundant operations. Most importantly, this
enhancement positions CockroachDB to handle large-scale vector workloads with
enterprise-grade performance and reliability.

---

## Appendix: Implementation Details

*This section contains detailed code snippets and implementation specifics.
Most readers can skip this section as the main document provides the
architectural overview and integration approach.*

### A.1 Schema Changer Operation Definitions

**File: `pkg/sql/schemachanger/scop/backfill.go`**
```go
// BuildVectorIndexStructure pre-builds the partition hierarchy for fast backfill
type BuildVectorIndexStructure struct {
    backfillOp
    TableID       descpb.ID
    IndexID       descpb.IndexID
    SourceIndexID descpb.IndexID  // Primary index to sample from
}

func (op *BuildVectorIndexStructure) Description() redact.RedactableString {
    return redact.Sprintf("building vector index structure for index %d on table %d",
        op.IndexID, op.TableID)
}

// MarkVectorIndexStructureBuilt marks the structure building as complete
type MarkVectorIndexStructureBuilt struct {
    mutationOp
    TableID descpb.ID
    IndexID descpb.IndexID
}

func (op *MarkVectorIndexStructureBuilt) Description() redact.RedactableString {
    return redact.Sprintf("marking vector index structure built for index %d on table %d",
        op.IndexID, op.TableID)
}
```

#### 2. New Element Status

**File: `pkg/sql/schemachanger/scpb/state.proto`**
```proto
enum Status {
    // ... existing statuses ...
    STRUCTURE_BUILDING = 15;  // New status: vector index structure building in progress
    STRUCTURE_BUILT = 16;     // New status: vector index structure building completed
}
```

#### 3. Operation Generation Rules

**File: `pkg/sql/schemachanger/scplan/internal/opgen/opgen_secondary_index.go`**
```go
func init() {
    opRegistry.register((*scpb.SecondaryIndex)(nil),
        toPublic(
            scpb.Status_ABSENT,
            // NEW: Vector index structure building phases (before BACKFILL_ONLY)
            to(scpb.Status_STRUCTURE_BUILDING,
                emitIfVectorIndex(func(this *scpb.SecondaryIndex, md *opGenContext) *scop.BuildVectorIndexStructure {
                    return &scop.BuildVectorIndexStructure{
                        TableID:       this.TableID,
                        IndexID:       this.IndexID,
                        SourceIndexID: md.sourceIndexIDForBackfill(this.TableID),
                    }
                }),
            ),
            to(scpb.Status_STRUCTURE_BUILT,
                // No operation needed - transition happens when BuildVectorIndexStructure completes
                emitIfVectorIndex(func(this *scpb.SecondaryIndex) *scop.MarkVectorIndexStructureBuilt {
                    return &scop.MarkVectorIndexStructureBuilt{
                        TableID: this.TableID,
                        IndexID: this.IndexID,
                    }
                }),
            ),
            to(scpb.Status_BACKFILL_ONLY,
                emit(func(this *scpb.SecondaryIndex) *scop.MakeAbsentIndexBackfilling { ... }),
            ),
            to(scpb.Status_BACKFILLED,
                emit(func(this *scpb.SecondaryIndex, md *opGenContext) *scop.BackfillIndex { ... }),
            ),
            // ... rest unchanged
        ),
    )
}

// Helper to only emit for vector indexes
func emitIfVectorIndex(f opGenFunc) opGenFunc {
    return func(element scpb.Element, md *opGenContext) scop.Op {
        if idx := element.(*scpb.SecondaryIndex); idx.EmbeddedIndex.VecConfig != nil {
            return f(element, md)
        }
        return nil // Skip for non-vector indexes
    }
}
```

#### 4. Execution Logic

**File: `pkg/sql/schemachanger/scexec/exec_backfill.go`**
```go
func executeBackfillOps(ctx context.Context, deps Dependencies, ops []scop.Op) error {
    switch op := ops[0].(type) {
    case *scop.BackfillIndex:
        return executeIndexBackfill(ctx, deps, ops)
    case *scop.BuildVectorIndexStructure:  // NEW
        return executeBuildVectorIndexStructure(ctx, deps, ops)
    case *scop.MergeIndex:
        return executeMergeIndex(ctx, deps, ops)
    // ... other cases
    }
}

func executeBuildVectorIndexStructure(
    ctx context.Context, deps Dependencies, ops []scop.Op,
) error {
    // Add elastic CPU work handle for background admission control
    ctx = admission.ContextWithElasticCPUWorkHandle(ctx, deps.AdmissionHandle())

    // Group by table for efficient processing
    opsByTable := make(map[descpb.ID][]*scop.BuildVectorIndexStructure)
    for _, op := range ops {
        buildOp := op.(*scop.BuildVectorIndexStructure)
        opsByTable[buildOp.TableID] = append(opsByTable[buildOp.TableID], buildOp)
    }

    // Process each table
    for tableID, tableOps := range opsByTable {
        if err := buildVectorIndexStructuresForTable(ctx, deps, tableID, tableOps); err != nil {
            return err
        }
    }
    return nil
}
```

#### 5. Dependency Rules

**File: `pkg/sql/schemachanger/scplan/internal/rules/current/dep_add_index.go`**
```go
// Structure building must complete before transitioning to BACKFILL_ONLY
registerDepRule(
    "vector index structure built precedes backfill_only",
    scgraph.Precedence,
    "structure_built", "backfill_only",
    func(from, to NodeVars) rel.Clauses {
        return rel.Clauses{
            from.Type((*scpb.SecondaryIndex)(nil)),
            to.Type((*scpb.SecondaryIndex)(nil)),
            JoinOnIndexID(from, to, "table-id", "index-id"),
            from.AttrEqVar(screl.IndexID, "index-id"),
            to.AttrEqVar(screl.IndexID, "index-id"),
            StatusesToPublicOrTransient(from, scpb.Status_STRUCTURE_BUILT, to, scpb.Status_BACKFILL_ONLY),
            FilterElements("IsVectorIndex", from, to),
        }
    },
)

// Structure building phase must complete before structure built phase
registerDepRule(
    "vector index structure building precedes structure built",
    scgraph.Precedence,
    "structure_building", "structure_built",
    func(from, to NodeVars) rel.Clauses {
        return rel.Clauses{
            from.Type((*scpb.SecondaryIndex)(nil)),
            to.Type((*scpb.SecondaryIndex)(nil)),
            JoinOnIndexID(from, to, "table-id", "index-id"),
            from.AttrEqVar(screl.IndexID, "index-id"),
            to.AttrEqVar(screl.IndexID, "index-id"),
            StatusesToPublicOrTransient(from, scpb.Status_STRUCTURE_BUILDING, to, scpb.Status_STRUCTURE_BUILT),
            FilterElements("IsVectorIndex", from, to),
        }
    },
)
```

#### Fixup Integration

Rather than explicitly disabling fixups, we modify the fixup enqueueing logic to
check index status:

```go
// Modified fixup enqueueing in existing cspann code
func (index *Index) maybeEnqueueFixup(ctx context.Context, fixup fixup) {
    // Skip fixups for indexes under construction
    if index.isUnderConstruction() {
        return
    }

    // Existing fixup logic
    index.fixupProcessor.Enqueue(fixup)
}

func (index *Index) isUnderConstruction() bool {
    // Check if index is in STRUCTURE_BUILDING, STRUCTURE_BUILT, BACKFILL_ONLY, etc.
    return index.status != PUBLIC
}
```

This integration approach leverages the existing, well-tested k-means
infrastructure while adapting it for the specific needs of bulk structure
construction.

### Vector Index Partition Validation

The validation step is enhanced for vector indexes to check partition sizes
after merge operations, ensuring optimal search performance.

#### New Validation Operation

**File: `pkg/sql/schemachanger/scop/validation.go`**
```go
// ValidateVectorIndexPartitions validates partition sizes after merge operations
type ValidateVectorIndexPartitions struct {
    validationOp
    TableID descpb.ID
    IndexID descpb.IndexID
}

func (ValidateVectorIndexPartitions) Description() redact.RedactableString {
    return "Validating vector index partition sizes"
}
```

#### Conditional Operation Generation

**File: `pkg/sql/schemachanger/scplan/internal/opgen/opgen_secondary_index.go`**
```go
to(scpb.Status_VALIDATED,
    emit(func(this *scpb.SecondaryIndex, md *opGenContext) *scop.ValidateIndex {
        // Standard validation for all indexes
        if checkIfDescriptorIsWithoutData(this.TableID, md) {
            return nil
        }
        return &scop.ValidateIndex{
            TableID: this.TableID,
            IndexID: this.IndexID,
        }
    }),
    // NEW: Vector-specific partition validation
    emitIfVectorIndex(func(this *scpb.SecondaryIndex, md *opGenContext) *scop.ValidateVectorIndexPartitions {
        if checkIfDescriptorIsWithoutData(this.TableID, md) {
            return nil
        }
        return &scop.ValidateVectorIndexPartitions{
            TableID: this.TableID,
            IndexID: this.IndexID,
        }
    }),
),
```

#### Validation Implementation

**File: `pkg/sql/schemachanger/scexec/exec_validation.go`**
```go
func executeValidateVectorIndexPartitions(
    ctx context.Context, deps Dependencies, op *scop.ValidateVectorIndexPartitions,
) error {
    descs, err := deps.Catalog().MustReadImmutableDescriptors(ctx, op.TableID)
    if err != nil {
        return err
    }
    table, ok := descs[0].(catalog.TableDescriptor)
    if !ok {
        return catalog.WrapTableDescRefErr(descs[0].GetID(), catalog.NewDescriptorTypeError(descs[0]))
    }
    index, err := catalog.MustFindIndexByID(table, op.IndexID)
    if err != nil {
        return err
    }

    // Execute vector-specific partition validation
    execOverride := sessiondata.NodeUserSessionDataOverride
    err = deps.Validator().ValidateVectorIndexPartitions(ctx,
        deps.TransactionalJobRegistry().CurrentJob(), table, index, execOverride)
    if err != nil {
        return scerrors.SchemaChangerUserError(err)
    }
    return nil
}

// Updated main validation dispatcher
func executeValidationOp(ctx context.Context, deps Dependencies, op scop.Op) (err error) {
    switch op := op.(type) {
    case *scop.ValidateIndex:
        // ... existing logic
    case *scop.ValidateVectorIndexPartitions:  // NEW
        if err = executeValidateVectorIndexPartitions(ctx, deps, op); err != nil {
            if !scerrors.HasSchemaChangerUserError(err) {
                return errors.Wrapf(err, "%T: %v", op, op)
            }
            return err
        }
    // ... other cases
    }
    return nil
}
```

#### Vector Index Validation Logic

**File: `pkg/sql/vecindex/validation.go`**
```go
// ValidatePartitionSizes checks for oversized partitions after merge operations
func ValidatePartitionSizes(ctx context.Context, index *cspann.Index) error {
    var oversizedPartitions []PartitionKey

    // Scan all leaf partitions to check sizes
    err := index.ForEachPartition(ctx, func(partition *Partition) error {
        if partition.VectorCount() > MaxRecommendedPartitionSize {
            oversizedPartitions = append(oversizedPartitions, partition.Key())
        }
        return nil
    })
    if err != nil {
        return err
    }

    if len(oversizedPartitions) > 0 {
        log.Warningf(ctx, "Found %d oversized partitions after merge in index %d: %v",
            len(oversizedPartitions), index.IndexID(), oversizedPartitions)

        // Configurable response to oversized partitions
        switch GetPartitionValidationAction() {
        case "warn":
            // Just log warning - don't block index from becoming PUBLIC
            return nil
        case "fixup":
            // Trigger immediate fixup for oversized partitions
            return triggerPartitionFixups(ctx, index, oversizedPartitions)
        case "fail":
            // Block index validation (for testing/strict environments)
            return errors.Errorf("validation failed: %d partitions exceed size limit",
                len(oversizedPartitions))
        }
    }

    return nil
}

// triggerPartitionFixups immediately schedules split operations for oversized partitions
func triggerPartitionFixups(ctx context.Context, index *cspann.Index, oversizedPartitions []PartitionKey) error {
    for _, partitionKey := range oversizedPartitions {
        splitFixup := &SplitPartitionFixup{
            PartitionKey: partitionKey,
            Reason:      "Post-merge size validation",
            Priority:    HighPriority,
        }
        index.FixupProcessor().EnqueueImmediately(splitFixup)
    }

    log.Infof(ctx, "Triggered immediate fixups for %d oversized partitions", len(oversizedPartitions))
    return nil
}
```

#### Benefits of This Validation Approach

1. **Clean Integration**: Follows existing schema changer validation patterns
exactly
2. **Zero Contamination**: Vector-specific logic isolated to vector packages
3. **Configurable Response**: Can warn, auto-fix, or fail based on configuration
4. **Post-Merge Timing**: Runs after merge operations complete, perfect for
checking results
5. **Non-Blocking Default**: Warnings don't prevent index from becoming PUBLIC
in production
6. **Automatic Remediation**: Can trigger immediate fixups for problematic
partitions

This ensures that the high-speed backfill process doesn't create problematic
partition structures that could degrade search performance, while maintaining
clean architectural boundaries.

### Random Sampling K-Means Structure Building Algorithm

The `buildVectorIndexStructuresForTable` function implements a scalable random
sampling k-means approach:

#### Phase 1: Initial Centroid Selection via Random Sampling

```go
func buildVectorIndexStructuresForTable(
    ctx context.Context, deps Dependencies, tableID descpb.ID, ops []*scop.BuildVectorIndexStructure,
) error {
    for _, op := range ops {
        // 1. Single-pass or two-pass data collection
        initialCentroids, vectorBatches, err := collectCentroidsAndBatches(ctx, deps, op)
        if err != nil {
            return err
        }

        // 2. Refine centroids using pre-collected mini-batch data
        refinedCentroids, err := refineCentroidsWithCollectedBatches(ctx, deps, op, initialCentroids, vectorBatches)
        if err != nil {
            return err
        }

        // 3. Build interior partition hierarchy for prefixed indexes
        hierarchy, err := buildInteriorPartitions(ctx, deps, op, refinedCentroids)
        if err != nil {
            return err
        }

        // 4. Write complete partition structure to vector index
        if err := writePartitionStructure(ctx, deps, op.IndexID, hierarchy); err != nil {
            return err
        }

        // 5. Configure temporary index to use the same structure (avoids metadata duplication)
        if err := configureTemporaryIndexStructure(ctx, deps, op.IndexID, hierarchy); err != nil {
            return err
        }
    }

    return nil
}

// collectCentroidsAndBatches performs single-pass or two-pass data collection
// Returns initial centroids and pre-collected batches for mini-batch k-means
func collectCentroidsAndBatches(
    ctx context.Context, deps Dependencies, op *scop.BuildVectorIndexStructure,
) (map[string][]vector.T, [][]VectorWithPrefix, error) {
    config := getCentroidRefinementConfig(deps)

    // Try single-pass approach first
    if config.EstimatedRowCount > 0 {
        // We have row count estimate - use single-pass collection
        return singlePassCollection(ctx, deps, op, config)
    }

    // Fall back to two-pass approach
    return twoPassCollection(ctx, deps, op, config)
}

// singlePassCollection collects both centroids and batches in a single scan
func singlePassCollection(
    ctx context.Context, deps Dependencies, op *scop.BuildVectorIndexStructure, config CentroidRefinementConfig,
) (map[string][]vector.T, [][]VectorWithPrefix, error) {
    centroidsByPrefix := make(map[string][]vector.T)

    // Calculate optimal batch count based on estimated row count
    estimatedVectors := config.EstimatedRowCount
    optimalBatchCount := min(estimatedVectors/config.BatchSize, config.MemoryLimit/(config.BatchSize*config.VectorSizeBytes))

    // Initialize reservoir samplers for each batch
    reservoirSamplers := make([]*ReservoirSampler, optimalBatchCount)
    for i := range reservoirSamplers {
        reservoirSamplers[i] = NewReservoirSampler(config.BatchSize)
    }

    scanner := &PrimaryIndexScanner{
        TableID:       op.TableID,
        SourceIndexID: op.SourceIndexID,
    }

    err := scanner.Scan(ctx, func(prefix string, vec vector.T) error {
        // Collect initial centroids (1% sampling)
        if randint()%100 == 0 {
            centroidsByPrefix[prefix] = append(centroidsByPrefix[prefix], vec)
        }

        // Add to random batch using reservoir sampling
        batchIndex := randint() % optimalBatchCount
        reservoirSamplers[batchIndex].Add(VectorWithPrefix{Prefix: prefix, Vector: vec})

        return nil
    })

    if err != nil {
        return nil, nil, err
    }

    // Extract batches from reservoir samplers
    vectorBatches := make([][]VectorWithPrefix, optimalBatchCount)
    for i, sampler := range reservoirSamplers {
        vectorBatches[i] = sampler.GetSample()
    }

    return centroidsByPrefix, vectorBatches, nil
}

// twoPassCollection falls back to two scans when row count is unknown
func twoPassCollection(
    ctx context.Context, deps Dependencies, op *scop.BuildVectorIndexStructure, config CentroidRefinementConfig,
) (map[string][]vector.T, [][]VectorWithPrefix, error) {
    // First pass: collect centroids and estimate row count
    centroidsByPrefix, rowCount, err := firstPassCentroidsAndCount(ctx, deps, op)
    if err != nil {
        return nil, nil, err
    }

    // Calculate optimal batch count based on actual row count
    optimalBatchCount := min(rowCount/config.BatchSize, config.MemoryLimit/(config.BatchSize*config.VectorSizeBytes))

    // Second pass: collect batches using reservoir sampling
    vectorBatches, err := secondPassBatchCollection(ctx, deps, op, optimalBatchCount, config.BatchSize)
    if err != nil {
        return nil, nil, err
    }

    return centroidsByPrefix, vectorBatches, nil
}
```

#### Phase 2: Batch-Based Centroid Refinement

```go
// refineCentroidsWithCollectedBatches refines centroids using pre-collected batch data
// with mini-batch k-means algorithm optimized for k > 2
func refineCentroidsWithCollectedBatches(
    ctx context.Context, deps Dependencies, op *scop.BuildVectorIndexStructure,
    initialCentroids map[string][]vector.T, vectorBatches [][]VectorWithPrefix,
) (map[string][]vector.T, error) {

    config := getCentroidRefinementConfig(deps)

    // Initialize mini-batch k-means (separate from BalancedKMeans)
    miniBatchKMeans := NewMiniBatchKMeans(config)

    // LRU cache for centroid partitions with disk spilling
    centroidCache := NewLRUCentroidCache(config.MemoryLimit)

    // Load initial centroids into cache
    for prefix, centroids := range initialCentroids {
        centroidCache.LoadCentroids(prefix, centroids)
    }

    for iteration := 0; iteration < config.MaxIterations; iteration++ {
        converged := true

        // Process each pre-collected batch
        for _, batch := range vectorBatches {
            if len(batch) == 0 {
                continue
            }

            // Update centroids with this batch using mini-batch k-means
            batchConverged := miniBatchKMeans.UpdateCentroidsWithBatch(ctx, centroidCache, batch)
            if !batchConverged {
                converged = false
            }
        }

        if converged {
            log.Infof(ctx, "Mini-batch k-means converged after %d iterations", iteration+1)
            break
        }
    }

    // Extract final centroids from cache (handles disk spilling)
    return centroidCache.GetAllCentroids(ctx)
}

type CentroidRefinementConfig struct {
    BatchSize           int   // Default: 1000 vectors per batch
    MemoryLimit         int64 // Default: 600MB
    VectorSizeBytes     int   // Estimated vector size (e.g., 6000 bytes)
    MaxIterations       int   // Default: 50
    ConvergenceThresh   float64 // Default: 1e-4
    EstimatedRowCount   int64 // Estimated number of rows (0 = unknown, triggers two-pass)
    PreferSinglePass    bool  // Default: true
}

// ReservoirSampler implements reservoir sampling for random batch collection
type ReservoirSampler struct {
    reservoir []VectorWithPrefix
    capacity  int
    count     int64
}

func NewReservoirSampler(capacity int) *ReservoirSampler {
    return &ReservoirSampler{
        reservoir: make([]VectorWithPrefix, 0, capacity),
        capacity:  capacity,
    }
}

func (rs *ReservoirSampler) Add(vec VectorWithPrefix) {
    rs.count++

    if len(rs.reservoir) < rs.capacity {
        rs.reservoir = append(rs.reservoir, vec)
    } else {
        // Reservoir sampling: replace with probability k/n
        j := randint() % int(rs.count)
        if j < rs.capacity {
            rs.reservoir[j] = vec
        }
    }
}

func (rs *ReservoirSampler) GetSample() []VectorWithPrefix {
    return rs.reservoir
}

// MiniBatchKMeans implements mini-batch k-means algorithm
// Separate from BalancedKMeans to handle k > 2 and different memory management
type MiniBatchKMeans struct {
    config CentroidRefinementConfig
    workspace *workspace.T
}

func NewMiniBatchKMeans(config CentroidRefinementConfig) *MiniBatchKMeans {
    return &MiniBatchKMeans{
        config: config,
        workspace: workspace.New(/* workspace config */),
    }
}

// UpdateCentroidsWithBatch updates centroids using mini-batch k-means algorithm
func (mbk *MiniBatchKMeans) UpdateCentroidsWithBatch(
    ctx context.Context, centroidCache *LRUCentroidCache, batch []VectorWithPrefix,
) bool {
    converged := true

    // Group batch by prefix
    batchByPrefix := make(map[string][]vector.T)
    for _, vec := range batch {
        batchByPrefix[vec.Prefix] = append(batchByPrefix[vec.Prefix], vec.Vector)
    }

    // Update centroids for each prefix using mini-batch k-means
    for prefix, vectors := range batchByPrefix {
        centroids := centroidCache.GetCentroids(prefix)
        if len(centroids) == 0 {
            continue
        }

        // Mini-batch k-means update (supports k > 2 unlike BalancedKMeans)
        centroidUpdated := mbk.updateCentroidsForPrefix(prefix, centroids, vectors)
        if centroidUpdated {
            converged = false
            centroidCache.UpdateCentroids(prefix, centroids)
        }
    }

    return converged
}
```

#### Phase 3: Interior Partition Construction

```go
// buildInteriorPartitions constructs the interior partition hierarchy based on leaf centroids
// For prefixed indexes, this is done separately for each unique prefix that has >1 leaf partition
func buildInteriorPartitions(
    ctx context.Context, deps Dependencies, op *scop.BuildVectorIndexStructure,
    leafCentroids map[string][]vector.T,
) (*PartitionHierarchy, error) {

    hierarchy := &PartitionHierarchy{
        LeafPartitions:     make(map[string][]*LeafPartition),
        InteriorPartitions: make(map[string][]*InteriorPartition),
    }

    for prefix, centroids := range leafCentroids {
        // Create leaf partitions with their centroids
        leafPartitions := make([]*LeafPartition, len(centroids))
        for i, centroid := range centroids {
            leafPartitions[i] = &LeafPartition{
                Prefix:   prefix,
                Centroid: centroid,
                ID:       generatePartitionID(),
            }
        }
        hierarchy.LeafPartitions[prefix] = leafPartitions

        // Build interior partitions if we have multiple leaf partitions for this prefix
        if len(centroids) > 1 {
            interiorPartitions, err := buildInteriorHierarchy(ctx, leafPartitions)
            if err != nil {
                return nil, err
            }
            hierarchy.InteriorPartitions[prefix] = interiorPartitions
        }
    }

    return hierarchy, nil
}

// buildInteriorHierarchy creates the interior partition tree using hierarchical k-means
// on the leaf centroids for a specific prefix
func buildInteriorHierarchy(ctx context.Context, leafPartitions []*LeafPartition) ([]*InteriorPartition, error) {
    // Extract centroids for interior clustering
    centroids := make([]vector.T, len(leafPartitions))
    for i, leaf := range leafPartitions {
        centroids[i] = leaf.Centroid
    }

    // Apply hierarchical k-means to build interior tree
    // This creates internal nodes that partition the leaf centroids
    return hierarchicalKMeans(ctx, centroids, leafPartitions)
}

type PartitionHierarchy struct {
    LeafPartitions     map[string][]*LeafPartition     // prefix -> leaf partitions
    InteriorPartitions map[string][]*InteriorPartition // prefix -> interior partitions
}

type LeafPartition struct {
    Prefix   string    // Empty string for non-prefixed indexes
    Centroid vector.T  // The centroid vector for this partition
    ID       PartitionID
}

type InteriorPartition struct {
    Prefix     string        // Empty string for non-prefixed indexes
    Centroid   vector.T      // Centroid of child partition centroids
    Children   []PartitionID // Child partition IDs (leaf or interior)
    ID         PartitionID
}
```

### Integration with Existing Backfill

Once the structure is built, vector indexes can use the standard bulk backfill
process without vector-specific re-encoding. The
`VectorIndexHelper.ReEncodeVector` function is eliminated entirely.
Additionally, fixup processes are managed to prevent interference:

#### Fixup Process Management

Vector indexes have sophisticated background fixup processes that continuously
optimize partition structure. During structure building and backfill, these are
naturally managed through existing logic:

- **Automatic Avoidance**: Fixup logic is modified to not enqueue fixups for indexes under construction (non-PUBLIC)
- **Shared Structure Benefits**: Both new and temporary indexes use the same structure, eliminating metadata copying
- **Write Operation Savings**: Temporary index uses the main index's structure directly, avoiding duplicate partition metadata writes

#### Temporary Index Structure Sharing

When a vector index is under construction, insertion logic is modified to use
the main index's structure for the temporary index:

```go
// configureTemporaryIndexStructure sets up structure sharing between indexes
func configureTemporaryIndexStructure(
    ctx context.Context, deps Dependencies, mainIndexID descpb.IndexID,
    hierarchy *PartitionHierarchy,
) error {
    // Find the temporary index for this vector index
    tempIndexID, err := findTemporaryIndexForVector(deps, mainIndexID)
    if err != nil {
        return err
    }

    // Configure temporary index to reference main index's structure
    // This avoids copying partition metadata and saves write operations
    return linkTemporaryToMainStructure(ctx, deps, tempIndexID, mainIndexID, hierarchy)
}

// Modified insertion logic to use shared structure
func insertIntoVectorIndex(ctx context.Context, indexID descpb.IndexID, vec vector.T) error {
    // Check if this is a temporary index with a main index under construction
    if mainIndexID := getLinkedMainIndex(indexID); mainIndexID != nil {
        // Use main index's structure for partition lookup
        return insertUsingSharedStructure(ctx, indexID, mainIndexID, vec)
    }

    // Standard vector index insertion
    return insertUsingOwnStructure(ctx, indexID, vec)
}
```

This approach eliminates the need to maintain multiple in-memory caches of
partition metadata, reducing memory pressure due to backfill. If the partition
metadata is too large to fit in memory, sharing a common cache of metadata will
increase cache hit rates. Finally, all the data in the temporary index will be
leaf vectors, eliminating writes of partition metadata that's already written to
the destination index.

#### Chunk Construction Process

```go
// VectorIndexChunkBuilder constructs backfill chunks using SearchForInsert
// with cached tree structure, then adds keys via bulk insert
type VectorIndexChunkBuilder struct {
    indexID       descpb.IndexID
    treeCache     *CachedVectorTree  // In-memory cached tree structure
    bulkAdder     *bulkio.BulkAdder
}

func (builder *VectorIndexChunkBuilder) BuildChunk(
    ctx context.Context, vectors []VectorEntry,
) ([]roachpb.KeyValue, error) {
    var kvs []roachpb.KeyValue

    for _, entry := range vectors {
        // SearchForInsert using cached tree structure (stable centroids enable caching)
        // No re-encoding needed - just determine partition placement
        partitionKey, err := builder.searchForInsertWithCachedTree(ctx, entry.Vector)
        if err != nil {
            return nil, err
        }

        // Build key directly without re-encoding
        key := VectorIndexKey{
            PartitionKey: partitionKey,
            Level:        cspann.LeafLevel,
            VectorID:     entry.VectorID,
        }

        // Quantize vector with stable partition centroid
        quantizedVector := builder.quantizeWithStablePartition(entry.Vector, partitionKey)

        // Add to chunk for bulk insert
        kvs = append(kvs, roachpb.KeyValue{
            Key:   key.Encode(),
            Value: quantizedVector.Encode(),
        })
    }

    return kvs, nil
}

// searchForInsertWithCachedTree uses cached tree structure for fast partition lookup
func (builder *VectorIndexChunkBuilder) searchForInsertWithCachedTree(
    ctx context.Context, vec vector.T,
) (PartitionKey, error) {
    // Use cached tree structure if available (fits in memory)
    if builder.treeCache != nil && builder.treeCache.IsComplete() {
        return builder.treeCache.SearchPartition(vec), nil
    }

    // Fall back to standard SearchForInsert with caching
    // Results are cacheable because centroids are stable during backfill
    return builder.searchForInsertWithResultCaching(ctx, vec)
}

// Standard bulk backfill integration - no vector-specific code needed
func BackfillVectorIndex(ctx context.Context, indexID descpb.IndexID) error {
    // Use standard bulk adder infrastructure
    bulkAdder := bulkio.NewBulkAdder(/* standard parameters */)

    chunkBuilder := &VectorIndexChunkBuilder{
        indexID:   indexID,
        treeCache: LoadCachedVectorTree(indexID), // Load pre-built structure
        bulkAdder: bulkAdder,
    }

    // Standard chunked backfill process - identical to other secondary indexes
    return runChunkedBackfill(ctx, chunkBuilder, bulkAdder)
}
```


#### Configurable Parameters

```go
type StructureBuildingConfig struct {
    // Random sampling configuration for initial centroids
    InitialCentroidRate  int     // Default: 100 (1% sampling via randint() % 100 == 0)

    // Batch processing configuration (mini-batch k-means, not BalancedKMeans)
    BatchSize           int     // Default: 1000 vectors per batch
    MaxIterations       int     // Default: 50
    ConvergenceThreshold float64 // Default: 1e-4
    VectorSizeEstimate  int     // Default: 6000 bytes per vector

    // Scanning strategy configuration
    PreferSinglePass    bool    // Default: true (single-pass when row count known)
    RowCountThreshold   int64   // Default: 1000 (minimum rows to attempt estimation)

    // Memory management with LRU cache and disk spilling
    MemoryLimit         int64   // Default: 600MB
    EnableDiskSpill     bool    // Default: true
    CacheEvictionPolicy string  // Default: "LRU"

    // Parallelization and optimization
    MaxWorkers          int     // Default: runtime.NumCPU()
    UseMiniKmeans       bool    // Default: true (use MiniBatchKMeans, not BalancedKMeans)
}
```

### Monitoring & Observability

#### Metrics

```go
var (
    // Structure building metrics
    structureBuildingDuration = prometheus.NewHistogramVec(
        prometheus.HistogramOpts{
            Name: "vecindex_structure_building_duration_seconds",
            Help: "Duration of vector index structure building phase",
        },
        []string{"table_id", "index_id"},
    )

    structureBuildingMemoryUsage = prometheus.NewGaugeVec(
        prometheus.GaugeOpts{
            Name: "vecindex_structure_building_memory_bytes",
            Help: "Memory usage during structure building",
        },
        []string{"table_id", "index_id"},
    )

    kmeansConvergenceIterations = prometheus.NewHistogramVec(
        prometheus.HistogramOpts{
            Name: "vecindex_kmeans_convergence_iterations",
            Help: "Number of iterations for k-means convergence",
        },
        []string{"table_id", "index_id"},
    )

    // Enhanced backfill metrics
    backfillThroughputAfterStructure = prometheus.NewGaugeVec(
        prometheus.GaugeOpts{
            Name: "vecindex_backfill_throughput_vectors_per_second",
            Help: "Vector backfill throughput after structure building",
        },
        []string{"table_id", "index_id"},
    )
)
```
