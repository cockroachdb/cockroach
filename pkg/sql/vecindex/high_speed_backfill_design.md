# High-Speed Vector Index Backfill Design

## Executive Summary

This design document outlines a new approach for dramatically improving the performance of vector index backfill operations in CockroachDB. The current implementation processes vectors one-at-a-time during backfill, which is too slow for large datasets. This proposal introduces a k-means clustering approach that pre-computes optimal partition structures through a new declarative schema changer step, allowing for high-throughput bulk insertion during the subsequent backfill phase.

## Current State Analysis

### Existing Vector Index Architecture

CockroachDB implements the **C-SPANN** (CockroachDB Scalable Partitioned Approximate Nearest Neighbor) algorithm with the following characteristics:

- **Hierarchical Structure**: K-means tree with quantized vectors (~100 vectors per partition)
- **Distance Metrics**: L2Squared, Cosine, and InnerProduct
- **Sophisticated Operations**: Atomic split/merge with 15-step state transitions
- **Background Maintenance**: Asynchronous fixup processors for partition optimization

### Current Backfill Process Issues

The existing backfill process (`pkg/sql/backfill/backfill.go:497-540`) has significant performance bottlenecks:

1. **Vector-by-Vector Processing**: Each vector requires individual partition lookup and re-encoding
2. **Search Overhead**: Repeated `SearchForInsert` operations with unstable centroids during backfill
3. **Re-encoding Complexity**: Dynamic quantization with partition-specific centroids
4. **Limited Batching**: Current approach doesn't leverage batch processing effectively

### Performance Characteristics

- **Current Approach**: Performance varies significantly with vector dimensions and dataset size
- **Scalability Issues**: Performance degrades with large datasets due to individual vector processing
- **Resource Usage**: High CPU overhead from repeated search operations during backfill

### Current Schema Changer Flow

Vector indexes currently follow the standard secondary index creation path:

```
ABSENT → BACKFILL_ONLY → BACKFILLED → DELETE_ONLY → MERGE_ONLY → MERGED → WRITE_ONLY → VALIDATED → PUBLIC
```

This flow processes through the declarative schema changer's operation generation system defined in `pkg/sql/schemachanger/scplan/internal/opgen/opgen_secondary_index.go`.

## Proposed Solution: Declarative Schema Changer Integration

### Architecture Overview

Rather than modifying the general backfill infrastructure, we propose adding a **new schema changer step** that pre-builds the vector index structure before the standard backfill phase. This keeps vector-specific logic isolated and follows existing schema changer patterns.

### New Schema Changer Step: Structure Building

The new `STRUCTURE_BUILDING` step performs one or more full scans over the primary index to construct the complete vector index hierarchy before any backfill operations begin. This approach:

**Primary Index Scanning Strategy:**
- **Initial Centroid Selection**: First scan uses random sampling (`randint() % 100 == 0`) to select ~1% of vectors as initial leaf partition centroids
- **Batch-Based Refinement**: Subsequent scans process batches of 1000 random vectors, using mini-batch k-means to refine centroids until convergence
- **Interior Partition Construction**: Final step builds interior partitions based on the optimized leaf centroids, handling prefixed indexes appropriately
- **Shared Structure**: The resulting hierarchy is used by both the new vector index and any temporary indexes created during the process

**Key Benefits:**
- **Stable Partition Structure**: Enables effective caching of `SearchForInsert` results due to stable centroids
- **Bulk Adder Compatibility**: Pre-built structure allows use of standard bulk adders (vastly faster than transactional inserts)
- **Reduced Vector-Specific Code**: Minimizes vector-specific logic in general backfill and merge infrastructure
- **Shared Structure Usage**: Both new and temporary indexes use the same optimized structure, avoiding metadata duplication
- **Automatic Fixup Avoidance**: Fixup logic naturally avoids indexes under construction without explicit disabling
- **Atomic Construction**: Structure building is atomic - either completes fully or rolls back cleanly

### Enhanced Schema Changer Flow

We introduce new statuses `STRUCTURE_BUILDING` and `STRUCTURE_BUILT` between `ABSENT` and `BACKFILL_ONLY`:

```
ABSENT → **STRUCTURE_BUILDING** → **STRUCTURE_BUILT** → BACKFILL_ONLY → BACKFILLED → DELETE_ONLY → MERGE_ONLY → MERGED → WRITE_ONLY → VALIDATED → PUBLIC
```

#### STRUCTURE_BUILDING Phase

During this phase, the schema changer performs the computationally intensive work of building the optimal partition structure for the vector index:

1. **Random Sampling Scan**: Performs an initial scan over the primary index, randomly selecting approximately 1% of vectors (`randint() % 100 == 0`) to serve as initial leaf partition centroids
2. **Batch-Based Refinement**: Conducts subsequent scans that process batches of random vectors (typically 1000 per batch) to refine the initial centroids using mini-batch k-means until convergence
3. **Memory Management**: Maintains configurable memory limits (default 600MB) with LRU caching and disk spilling for large datasets
4. **Prefixed Index Support**: Handles prefixed indexes by maintaining separate centroid sets for each unique prefix
5. **Convergence Detection**: Monitors centroid changes across iterations to determine when the clustering has stabilized

This phase is the most resource-intensive part of the process, as it involves multiple scans over potentially large datasets and iterative k-means computation.

#### STRUCTURE_BUILT Phase  

This phase indicates that the partition structure has been successfully constructed and is ready for use:

1. **Complete Hierarchy Available**: All leaf and interior partitions have been created with stable centroids
2. **Interior Partition Construction**: Builds the hierarchical tree structure on top of the leaf centroids, creating interior partitions that enable efficient tree traversal
3. **Temporary Index Configuration**: Sets up temporary indexes to use the same partition structure as the main index, avoiding metadata duplication
4. **Fixup Avoidance**: Ensures background fixup processes do not interfere with the carefully constructed structure
5. **Ready for Backfill**: The index is now ready to transition to `BACKFILL_ONLY` where standard bulk loading can begin

The transition from `STRUCTURE_BUILT` to `BACKFILL_ONLY` marks the handoff from vector-specific construction logic to the standard, high-performance bulk loading infrastructure.

## Integration with Existing Systems

### Schema Changer Integration

The new structure building phases integrate cleanly with the existing declarative schema changer framework:

#### New Schema Operations
Two new operation types are added to handle vector index structure building:
- `BuildVectorIndexStructure`: Performs the actual k-means clustering and partition construction
- `MarkVectorIndexStructureBuilt`: Marks the structure building as complete and ready for backfill

#### Status Transitions
The new statuses follow existing schema changer patterns, with dependency rules ensuring proper ordering and atomicity. The schema changer's existing transaction and rollback mechanisms handle failures during structure building.

#### Vector-Specific Logic Isolation
Only vector indexes follow the new `STRUCTURE_BUILDING` → `STRUCTURE_BUILT` path. Regular secondary indexes bypass these phases entirely, maintaining full backward compatibility.

### C-SPANN K-Means Integration

The structure building algorithm leverages existing, proven components from the `pkg/sql/vecindex/cspann` package:

#### Reused Components
- **BalancedKmeans**: Binary k-means implementation adapted for hierarchical partition construction
- **Workspace Management**: Existing memory allocation system for temporary vectors and computations  
- **Quantizer Infrastructure**: Proven quantization algorithms (RaBitQ, etc.) for vector encoding
- **Distance Metrics**: Support for L2Squared, Cosine, and InnerProduct distance functions
- **Partition Structures**: Existing PartitionKey, TreeKey, and metadata infrastructure

#### New Adaptations
- **BulkKmeansBuilder**: Adapter that coordinates bulk construction while delegating to existing components
- **Random Sampling**: Implementation of the `randint() % 100 == 0` sampling strategy for initial centroids
- **Batch Processing**: Memory-bounded batch processing system with LRU caching and disk spilling
- **Convergence Detection**: Monitoring system for k-means iteration termination

### Backfill Infrastructure Integration

The pre-built structure enables vector indexes to use standard bulk loading infrastructure without modification:

#### Elimination of Vector-Specific Backfill Code
- **No VectorIndexHelper.ReEncodeVector**: This function is completely removed from the backfill process
- **Standard Chunk Construction**: Vector indexes use the same chunk building process as regular secondary indexes
- **Bulk Adder Compatibility**: Pre-built structure allows direct use of high-performance bulk adders

#### Temporary Index Structure Sharing
- **Shared Partition Metadata**: Temporary indexes reference the main index's partition structure directly
- **Reduced Write Operations**: Eliminates the need to copy partition metadata during merge operations
- **Memory Efficiency**: Single structure serves both temporary and main indexes

#### Fixup Process Integration
- **Automatic Avoidance**: Existing fixup logic is modified to skip indexes under construction (non-PUBLIC status)
- **No Explicit Disabling**: No need for special fixup disable/enable operations
- **Natural Integration**: Works with existing `fixupProcessor.Enqueue()` logic

### Performance Benefits

#### Bulk Adder Performance
The pre-built partition structure enables vector indexes to use the same high-performance bulk adders that other secondary indexes use, providing orders of magnitude performance improvements over transactional inserts.

#### Stable Centroid Caching
With centroids remaining stable during backfill, `SearchForInsert` can use cached partitions, which should provide a large performance boost.

#### Reduced Memory Pressure
Structure sharing between temporary and main indexes reduces memory usage and increases cache hit rates, especially beneficial for large datasets where partition metadata might not fit entirely in memory.

## Implementation Phases

### Phase 1: Schema Changer Framework
- Add new schema changer statuses and operations
- Implement operation generation rules and dependency management
- Create execution framework integration
- Establish proper rollback and error handling

### Phase 2: Structure Building Algorithm
- Implement random sampling centroid selection
- Develop batch processing with memory management
- Create interior partition hierarchy construction
- Add convergence detection and termination logic

### Phase 3: System Integration
- Integrate with existing C-SPANN k-means components
- Implement temporary index structure sharing
- Modify fixup logic for construction avoidance
- Add comprehensive testing and validation

### Phase 4: Production Readiness
- Performance tuning and optimization
- Large-scale testing across different workloads
- Operational procedures and monitoring
- Documentation and gradual rollout

## Conclusion

This design represents a significant advancement in vector index construction performance while maintaining architectural integrity. By integrating cleanly with existing schema changer patterns and leveraging proven C-SPANN components, the approach delivers substantial performance improvements with minimal risk.

The phased implementation ensures manageable development while the structure sharing approach eliminates redundant operations. Most importantly, this enhancement positions CockroachDB to handle large-scale vector workloads with enterprise-grade performance and reliability.

---

## Appendix: Implementation Details

*This section contains detailed code snippets and implementation specifics. Most readers can skip this section as the main document provides the architectural overview and integration approach.*

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

### Integration with Existing C-SPANN K-Means Code

The structure building algorithm leverages existing k-means implementation in `pkg/sql/vecindex/cspann` while adapting it for bulk structure construction:

#### Existing C-SPANN Components We Can Reuse

1. **BalancedKmeans** (`kmeans.go`): Binary k-means implementation with k-means++ initialization
2. **Partition Infrastructure** (`partition.go`): PartitionKey, TreeKey, and metadata structures  
3. **Workspace** (`workspace/`): Memory management for temporary allocations
4. **Quantizer** (`quantize/`): Vector quantization with multiple algorithms
5. **Distance Metrics**: Support for L2Squared, Cosine, and InnerProduct
6. **Fixup System** (`fixup_processor.go`): Background optimization (to be avoided during construction)

#### Adaptations Needed for Bulk Construction

```go
// BulkKmeansBuilder adapts existing cspann k-means for structure building
type BulkKmeansBuilder struct {
    // Reuse existing components
    workspace     *workspace.T
    balancedKmeans *BalancedKmeans
    quantizer     quantize.Quantizer
    
    // New components for bulk building
    centroidCache *LRUCentroidCache
    vectorSampler *RandomVectorSampler
    memoryLimit   int64
}

// buildLeafCentroids uses random sampling instead of k-means++ for initial centroids
func (builder *BulkKmeansBuilder) buildLeafCentroids(
    ctx context.Context, vectors []VectorWithPrefix,
) (map[string][]PartitionKey, error) {
    centroidsByPrefix := make(map[string][]PartitionKey)
    
    for _, vec := range vectors {
        // Random sampling: ~1% of vectors become initial centroids
        if randint()%100 == 0 {
            partitionKey := builder.createLeafPartition(vec.Prefix, vec.Vector)
            centroidsByPrefix[vec.Prefix] = append(centroidsByPrefix[vec.Prefix], partitionKey)
        }
    }
    
    return centroidsByPrefix, nil
}

// refineWithBatches uses mini-batch k-means to refine the initial centroids
func (builder *BulkKmeansBuilder) refineWithBatches(
    ctx context.Context, initialCentroids map[string][]PartitionKey,
) error {
    for iteration := 0; iteration < builder.maxIterations; iteration++ {
        // Generate batches within memory limit
        batchCount := builder.memoryLimit / (batchSize * vectorSizeBytes)
        
        for batchNum := 0; batchNum < int(batchCount); batchNum++ {
            batch := builder.vectorSampler.Sample(batchSize)
            
            // Use existing BalancedKmeans for centroid updates within each prefix
            for prefix, centroids := range initialCentroids {
                prefixBatch := filterByPrefix(batch, prefix)
                if len(prefixBatch) > 0 {
                    builder.updateCentroidsForPrefix(prefix, centroids, prefixBatch)
                }
            }
        }
        
        if builder.hasConverged() {
            break
        }
    }
    
    return nil
}

// buildInteriorHierarchy reuses existing BalancedKmeans for interior partitions
func (builder *BulkKmeansBuilder) buildInteriorHierarchy(
    ctx context.Context, leafCentroids []PartitionKey,
) ([]PartitionKey, error) {
    if len(leafCentroids) <= 1 {
        return leafCentroids, nil
    }
    
    // Extract centroids for hierarchical clustering
    vectors := builder.extractCentroidVectors(leafCentroids)
    
    // Use existing BalancedKmeans for binary splits
    interiorPartitions := []PartitionKey{}
    queue := [][]PartitionKey{leafCentroids}
    
    for len(queue) > 0 {
        partition := queue[0]
        queue = queue[1:]
        
        if len(partition) <= builder.maxPartitionSize {
            // Partition is small enough, create interior partition
            interiorKey := builder.createInteriorPartition(partition)
            interiorPartitions = append(interiorPartitions, interiorKey)
        } else {
            // Split using existing BalancedKmeans
            left, right := builder.splitPartition(partition)
            queue = append(queue, left, right)
        }
    }
    
    return interiorPartitions, nil
}

// splitPartition uses existing BalancedKmeans for binary partition splits
func (builder *BulkKmeansBuilder) splitPartition(
    partitionKeys []PartitionKey,
) ([]PartitionKey, []PartitionKey) {
    // Extract vectors for the partitions
    vectors := builder.workspace.AllocVectorSet(len(partitionKeys), builder.vectorDims)
    defer builder.workspace.FreeVectorSet(vectors)
    
    for i, key := range partitionKeys {
        centroid := builder.getCentroid(key)
        copy(vectors.At(i), centroid)
    }
    
    // Use existing BalancedKmeans for split
    leftCentroid := builder.workspace.AllocVector(builder.vectorDims)
    rightCentroid := builder.workspace.AllocVector(builder.vectorDims)
    defer builder.workspace.FreeVector(leftCentroid)
    defer builder.workspace.FreeVector(rightCentroid)
    
    builder.balancedKmeans.ComputeCentroids(vectors, leftCentroid, rightCentroid, false)
    
    // Assign partitions to left/right based on proximity to centroids
    assignments := builder.workspace.AllocUint64s(len(partitionKeys))
    defer builder.workspace.FreeUint64s(assignments)
    
    builder.balancedKmeans.AssignPartitions(vectors, leftCentroid, rightCentroid, assignments)
    
    // Split partition keys based on assignments
    var left, right []PartitionKey
    for i, assignment := range assignments {
        if assignment == 0 {
            left = append(left, partitionKeys[i])
        } else {
            right = append(right, partitionKeys[i])
        }
    }
    
    return left, right
}
```

#### Fixup Integration

Rather than explicitly disabling fixups, we modify the fixup enqueueing logic to check index status:

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

This integration approach leverages the existing, well-tested k-means infrastructure while adapting it for the specific needs of bulk structure construction.

### Random Sampling K-Means Structure Building Algorithm

The `buildVectorIndexStructuresForTable` function implements a scalable random sampling k-means approach:

#### Phase 1: Initial Centroid Selection via Random Sampling

```go
func buildVectorIndexStructuresForTable(
    ctx context.Context, deps Dependencies, tableID descpb.ID, ops []*scop.BuildVectorIndexStructure,
) error {
    for _, op := range ops {
        // 1. First scan: Create initial leaf partitions via random sampling
        initialCentroids, err := createInitialCentroidsFromPrimaryScan(ctx, deps, op)
        if err != nil {
            return err
        }
        
        // 2. Refine centroids using mini-batch k-means
        refinedCentroids, err := refineCentroidsWithBatches(ctx, deps, op, initialCentroids)
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

// createInitialCentroidsFromPrimaryScan performs the first scan over the primary index
// and creates ~1% of vectors as initial leaf partition centroids using random sampling
func createInitialCentroidsFromPrimaryScan(
    ctx context.Context, deps Dependencies, op *scop.BuildVectorIndexStructure,
) (map[string][]vector.T, error) {
    // Map from prefix -> list of centroids (empty map for non-prefixed indexes)
    centroidsByPrefix := make(map[string][]vector.T)
    
    scanner := &PrimaryIndexScanner{
        TableID:       op.TableID,
        SourceIndexID: op.SourceIndexID,
    }
    
    return scanner.Scan(ctx, func(prefix string, vec vector.T) error {
        // Random sampling: ~1% of vectors become initial centroids
        if randint()%100 == 0 {
            // Create new empty leaf partition with this vector as centroid
            centroidsByPrefix[prefix] = append(centroidsByPrefix[prefix], vec)
        }
        return nil
    })
}
```

#### Phase 2: Batch-Based Centroid Refinement

```go
// refineCentroidsWithBatches refines the initial centroids using mini-batch k-means
// with configurable memory limits and LRU caching for centroid partitions
func refineCentroidsWithBatches(
    ctx context.Context, deps Dependencies, op *scop.BuildVectorIndexStructure, 
    initialCentroids map[string][]vector.T,
) (map[string][]vector.T, error) {
    
    config := getCentroidRefinementConfig(deps)
    
    // LRU cache for centroid partitions with disk spilling
    centroidCache := NewLRUCentroidCache(config.MemoryLimit)
    
    // Load initial centroids into cache
    for prefix, centroids := range initialCentroids {
        centroidCache.LoadCentroids(prefix, centroids)
    }
    
    for iteration := 0; iteration < config.MaxIterations; iteration++ {
        converged := true
        
        // Build N batches of 1000 random vectors each, up to memory limit
        // Example: 100 batches * 1000 vectors * 6000 bytes = 600MB
        batchCount := config.MemoryLimit / (config.BatchSize * config.VectorSizeBytes)
        
        for batchNum := 0; batchNum < int(batchCount); batchNum++ {
            batch, err := generateRandomVectorBatch(ctx, deps, op, config.BatchSize)
            if err != nil {
                return nil, err
            }
            
            // Update centroids with this batch
            batchConverged := updateCentroidsWithBatch(ctx, centroidCache, batch)
            if !batchConverged {
                converged = false
            }
        }
        
        if converged {
            log.Infof(ctx, "K-means converged after %d iterations", iteration+1)
            break
        }
    }
    
    // Extract final centroids from cache (handles disk spilling)
    return centroidCache.GetAllCentroids(ctx)
}

type CentroidRefinementConfig struct {
    BatchSize         int   // Default: 1000 vectors per batch
    MemoryLimit       int64 // Default: 600MB
    VectorSizeBytes   int   // Estimated vector size (e.g., 6000 bytes)
    MaxIterations     int   // Default: 50
    ConvergenceThresh float64 // Default: 1e-4
}

// generateRandomVectorBatch creates a batch of random vectors from the primary index
// This can be combined with the initial scan for the first iteration to avoid extra scanning
func generateRandomVectorBatch(
    ctx context.Context, deps Dependencies, op *scop.BuildVectorIndexStructure, 
    batchSize int,
) ([]VectorWithPrefix, error) {
    // Implementation details:
    // - Use reservoir sampling for truly random selection
    // - Handle prefixed indexes appropriately
    // - Can be combined with initial centroid selection scan for efficiency
    
    scanner := &PrimaryIndexScanner{
        TableID:       op.TableID,
        SourceIndexID: op.SourceIndexID,
    }
    
    return scanner.RandomSample(ctx, batchSize)
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

Once the structure is built, vector indexes can use the standard bulk backfill process without vector-specific re-encoding. The `VectorIndexHelper.ReEncodeVector` function is eliminated entirely. Additionally, fixup processes are managed to prevent interference:

#### Fixup Process Management

Vector indexes have sophisticated background fixup processes that continuously optimize partition structure. During structure building and backfill, these are naturally managed through existing logic:

- **Automatic Avoidance**: Fixup logic is modified to not enqueue fixups for indexes under construction (non-PUBLIC)
- **Shared Structure Benefits**: Both new and temporary indexes use the same structure, eliminating metadata copying
- **Write Operation Savings**: Temporary index uses the main index's structure directly, avoiding duplicate partition metadata writes

#### Temporary Index Structure Sharing

When a vector index is under construction, insertion logic is modified to use the main index's structure for the temporary index:

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

This approach eliminates the need to maintain multiple in-memory caches of partition metadata, reducing memory pressure due to backfill. If the partition metadata is too large to fit in memory, sharing a common cache of metadata will increase cache hit rates. Finally, all the data in the temporary index will be leaf vectors, eliminating writes of partition metadata that's already written to the destination index.

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

### Benefits of Schema Changer Integration

This approach provides several key advantages:

1. **Bulk Adder Utilization**: With pre-built structure, backfill can use standard bulk adders instead of slow transactional inserts
2. **Minimal Vector-Specific Code**: Reduces vector-specific logic in general backfill infrastructure to near zero
3. **Zero Vector Code in Merge**: Merge operations require no vector-specific modifications at all
4. **Clean Separation**: Vector-specific logic remains isolated in the structure building phase
5. **Standard Patterns**: Follows existing schema changer operation/status/rule patterns exactly
6. **Backwards Compatible**: Non-vector indexes are completely unaffected
7. **Atomic and Transactional**: Leverages existing transaction boundaries and rollback mechanisms
8. **Progress Tracking**: Gets checkpointing and monitoring infrastructure for free
9. **Parallel Execution**: Can leverage existing parallel operation execution framework

### Performance Improvements

#### Bulk Adder Performance Benefits

The pre-built partition structure enables vector indexes to use the same high-performance bulk adders that other secondary indexes use:

- **Bulk vs Transactional**: Bulk adders are orders of magnitude faster than transactional inserts
- **Standard Infrastructure**: Leverages existing, highly optimized bulk loading code paths
- **No Special Cases**: Vector indexes follow the same efficient patterns as regular secondary indexes
- **Merge Compatibility**: Merge operations work identically for vector and non-vector indexes

This architectural alignment means vector index creation benefits from all existing performance optimizations in the bulk loading infrastructure.

#### Expected Performance Gains

| Metric | Current | Proposed | Expected Improvement |
|--------|---------|----------|---------------------|
| Throughput | Vector-by-vector processing | Bulk processing with pre-built structure | Significant increase |
| CPU Usage | High (search overhead) | Lower (batch processing) | Substantial reduction |
| Memory Usage | Variable | Predictable with configurable limits | More efficient |
| Scalability | Degrades with dataset size | Linear scaling with dataset size | Better scalability |

#### Benchmarking Strategy

Performance improvements will be measured through comprehensive benchmarking across different dataset sizes and vector dimensions. Actual performance targets will be established through empirical testing during implementation.

### Implementation Phases

#### Phase 1: Schema Changer Integration
- Add new operation type and status to schema changer
- Implement operation generation rules for vector indexes
- Add execution framework integration
- Create dependency rules for proper ordering

#### Phase 2: Random Sampling K-Means Algorithm Implementation
- Implement random sampling centroid selection with `randint() % 100 == 0`
- Develop batch processing with LRU cache and disk spilling
- Create interior partition hierarchy builder
- Add comprehensive testing and validation

#### Phase 3: Backfill Optimization
- Enhance `VectorIndexHelper` for fast partition lookup
- Integrate with existing backfill pipeline
- Implement fixup disable/enable logic during structure building
- Add performance monitoring and metrics

#### Phase 4: Production Readiness
- Performance tuning and optimization
- Large-scale testing and validation
- Documentation and operational procedures
- Gradual rollout and monitoring

### Risk Analysis & Mitigation

#### Technical Risks

**Risk**: Schema changer complexity and integration issues
- **Mitigation**: Follow existing patterns exactly; extensive testing with schema changer team
- **Fallback**: Ability to disable new step and fall back to standard backfill

**Risk**: K-means convergence issues with high-dimensional vectors
- **Mitigation**: Implement convergence detection and fallback mechanisms
- **Testing**: Comprehensive testing with various vector dimensions (128-2048)

**Risk**: Memory usage exceeding limits during structure building
- **Mitigation**: Configurable memory limits and disk spilling for large centroids
- **Monitoring**: Memory usage tracking and alerts

#### Operational Risks

**Risk**: Increased complexity in index creation process
- **Mitigation**: Comprehensive testing and gradual rollout with feature flags
- **Observability**: Enhanced monitoring and debugging capabilities

**Risk**: Rollback complexity for failed structure building
- **Mitigation**: Leverage existing schema changer rollback mechanisms
- **Testing**: Extensive failure scenario testing

### Configuration & Tuning

#### Cluster Settings

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
SET CLUSTER SETTING vecindex.structure_building.vector_size_estimate = 6000;
```

#### Configurable Parameters

```go
type StructureBuildingConfig struct {
    // Random sampling configuration for initial centroids
    InitialCentroidRate  int     // Default: 100 (1% sampling via randint() % 100 == 0)
    
    // Batch processing configuration
    BatchSize           int     // Default: 1000 vectors per batch
    MaxIterations       int     // Default: 50
    ConvergenceThreshold float64 // Default: 1e-4
    VectorSizeEstimate  int     // Default: 6000 bytes per vector
    
    // Memory management with LRU cache and disk spilling
    MemoryLimit         int64   // Default: 600MB
    EnableDiskSpill     bool    // Default: true
    CacheEvictionPolicy string  // Default: "LRU"
    
    // Parallelization and optimization
    MaxWorkers          int     // Default: runtime.NumCPU()
    CombineFirstScan    bool    // Default: true (combine initial centroid + first batch)
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

#### Logging

```go
log.Infof(ctx, "Starting vector index structure building: table=%d, index=%d, vectors_sampled=%d",
    tableID, indexID, sampledVectorCount)

log.Infof(ctx, "K-means clustering completed: iterations=%d, centroids=%d, memory_used=%s",
    iterations, centroidCount, humanize.Bytes(memoryUsed))

log.Infof(ctx, "Vector index structure building completed: duration=%s, partition_count=%d",
    duration, partitionCount)
```

### Testing Strategy

#### Unit Testing
- Schema changer operation generation and execution
- K-means algorithm correctness and convergence
- Partition hierarchy construction
- Memory management and spilling

#### Integration Testing
- End-to-end index creation with structure building
- Schema changer rollback scenarios
- Concurrent operations during structure building
- Performance regression testing

#### Performance Testing
- Large-scale dataset testing (1M+ vectors)
- Various vector dimensions and distance metrics
- Memory pressure and spilling scenarios
- Comparison with current backfill performance

## Conclusion

The proposed k-means structure building approach represents a significant advancement in CockroachDB's vector indexing capabilities. By integrating cleanly with the declarative schema changer's established patterns, we can achieve 10-100x performance improvements while maintaining:

- **Architectural Integrity**: No contamination of general backfill infrastructure
- **Operational Safety**: Leveraging existing transaction and rollback mechanisms
- **Code Maintainability**: Vector-specific logic remains isolated
- **Team Boundaries**: Minimal impact on schema changer team's responsibilities

The phased implementation approach ensures manageable risk while delivering incremental value. The integration strategy follows existing schema changer patterns exactly, making it straightforward to review and maintain.

This enhancement positions CockroachDB as a leader in high-performance vector database capabilities, supporting large-scale AI/ML workloads with enterprise-grade reliability and performance.
