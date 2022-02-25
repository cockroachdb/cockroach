// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// This file lives here instead of sql/distsql to avoid an import cycle.

package execinfra

import "github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"

// Version identifies the distsql protocol version.
//
// This version is separate from the main CockroachDB version numbering; it is
// only changed when the distsql API changes.
//
// The planner populates the version in SetupFlowRequest. A server only accepts
// requests with versions in the range [MinAcceptedVersion, Version].
//
// This mechanism can be used to provide a "window" of compatibility when new
// features are added. Example:
//  - we start with Version=1; distsql servers with version 1 only accept
//    requests with version 1.
//  - a new distsql feature is added; Version is bumped to 2. The
//    planner does not yet use this feature by default; it still issues
//    requests with version 1.
//  - MinAcceptedVersion is still 1, i.e. servers with version 2
//    accept both versions 1 and 2.
//  - after an upgrade cycle, we can enable the feature in the planner,
//    requiring version 2.
//  - at some later point, we can choose to deprecate version 1 and have
//    servers only accept versions >= 2 (by setting
//    MinAcceptedVersion to 2).
//
// Why does this all matter? Because of rolling upgrades, distsql servers across
// nodes may not have an overlapping window of compatibility, so only a subset
// of nodes can participate in a distsql flow on a given version -- hurting
// performance. However, we'll take the performance hit to prevent a distsql
// flow from blowing up. Here's an example:
//
// Suppose that nodes running 21.2 can handle flows with distsql version 59.
// Say we introduced a new distsql processor spec, ExportSpec,
// in 22.1 but didn't bump the distsql version from 59 to 60.
//
// During a rolling upgrade, suppose Node A has upgraded to 22.1 and plans a
// distSQL flow that uses the new ExportSpec. Node A thinks any node with distsql
// version 59 can handle this flow, which includes nodes still running a 21.2
// binary! As soon as a node running a 21.2 binary receives a ExportSpec proto,
// it will not recognize it, causing the distsql flow to error out.
//
// To avoid this sad tale, consider bumping the distsql version if you:
// - Modify a distsql processor spec in a released binary
// - Create a new distql processor spec
// - More examples below
//
// A few changes don't need to bump the distsql version:
// - Modifying a distsql processor spec that isn't on a released binary yet
// - Renaming any field or the processor spec itself. Nodes are naive to proto field names.
//
// ATTENTION: When updating these fields, add a brief description of what
// changed to the version history below.
const Version execinfrapb.DistSQLVersion = 64

// MinAcceptedVersion is the oldest version that the server is compatible with.
// A server will not accept flows with older versions.
const MinAcceptedVersion execinfrapb.DistSQLVersion = 63

/*

**  VERSION HISTORY **

Please add new entries at the top.

- Version: 64 (MinAcceptedVersion: 63)
  - final_covar_samp, final_corr, and final_sqrdiff aggregate functions were
    introduced to support local and final aggregation of the corresponding
    builtin functions. It would be unrecognized by a server running older
    versions, hence the version bump.
    However, a server running v64 can still process all plans from servers
    running v63, thus the MinAcceptedVersion is kept at 63.

- Version: 63 (MinAcceptedVersion: 63):
 - Changed JoinReaderSpec to use a descpb.IndexFetchSpec and a list of family
   IDs instead of table and index descriptors.

- Version: 62 (MinAcceptedVersion: 62):
 - Changed TableReaderSpec to use a descpb.IndexFetchSpec instead of table and
   index descriptors.

- Version: 61 (MinAcceptedVersion: 60)
  - final_regr_avgx, final_regr_avgy, final_regr_intercept, final_regr_r2, and
    final_regr_slope aggregate functions were introduced to support local and
    final aggregation of the corresponding builtin functions. It would be
    unrecognized by a server running older versions, hence the version bump.
    However, a server running v61 can still process all plans from servers
    running v60, thus the MinAcceptedVersion is kept at 60.

- Version: 60 (MinAcceptedVersion: 60):
 - Deprecated ExportWriterSpec and ParquetWriterSpec and merged them into ExportSpec

- Version: 59 (MinAcceptedVersion: 58)
  - final_regr_sxx, final_regr_sxy, and final_regr_syy aggregate functions were
    introduced to support local and final aggregation of the corresponding
    builtin functions. It would be unrecognized by a server running older
    versions, hence the version bump. However, a server running v59 can still
    process all plans from servers running v58, thus the MinAcceptedVersion is
    kept at 58.

- Version: 58 (MinAcceptedVersion: 58)
	- TableReaderSpec now contains a specific list of column IDs and the internal
		schema now corresponds to these columns (instead of all table columns). The
		HasSystemColumns, DeprecatedIsCheck fields have been removed.

- Version: 57 (MinAcceptedVersion: 56)
  - FINAL_COVAR_POP aggregate function was introduced to support local and final
    aggregation of the builtin function COVAR_POP. It would be unrecognized
    by a server running older versions, hence the version bump. However, a
    server running v57 can still process all plans from servers running v56,
    thus the MinAcceptedVersion is kept at 56.

- Version: 56 (MinAcceptedVersion: 56)
	- The Visibility fields from TableReaderSpec, IndexSkipTableReaderSpec,
	  JoinReaderSpec have been removed.

- Version: 55 (MinAcceptedVersion: 55)
  - The computation of the hash of JSONs in the vectorized engine has changed.
    As a result, the hash routing can be now done in a different manner, so we
    have to bump both versions.

- Version: 54 (MinAcceptedVersion: 52)
  - Field NeededColumns has been removed from the TableReaderSpec. It was being
    used for the setup of the vectorized ColBatchScans, but now the
    PostProcessSpec is examined instead. This means that older gateways still
    produce specs that newer remote nodes understand, thus there is no bump in
    the MinAcceptedVersion.

- Version: 53 (MinAcceptedVersion: 52)
  - FINAL_STDDEV_POP and FINAL_VAR_POP aggregate functions were introduced to
    support local and final aggregation of the builtin function STDDEV_POP. It
    would be unrecognized by a server running older versions, hence the version
    bump. However, a server running v53 can still process all plans from servers
    running v52, thus the MinAcceptedVersion is kept at 52.

- Version: 52 (MinAcceptedVersion: 52)
  - A new field added to table statistics. This is produced by samplers, so
    there is no backwards compatibility.

- Version: 51 (MinAcceptedVersion: 51)
  - Redundant TableReaderSpan message has been removed in favor of using
    roachpb.Spans directly.

- Version: 50 (MinAcceptedVersion: 48)
  - A new field, MinSampleSize, was added to both SamplerSpec and
    SamplerAggregatorSpec to support dynamically shrinking sample sets.

- Version: 49 (MinAcceptedVersion: 48)
  - A new field RemoteLookupExpr was added to JoinReaderSpec for supporting
    locality optimized lookup joins.

- Version: 48 (MinAcceptedVersion: 48)
  - Zero value for VectorizeExecMode changed meaning from "off" to "on".

- Version: 47 (MinAcceptedVersion: 47)
  - A new synchronizer type (serial unordered) has been introduced explicitly.
    MinAcceptedVersion needed to be bumped because if the older server receives
    a SetupFlowRequest with the new synchronizer type, it might lead to an
    undefined behavior.

- Version: 46 (MinAcceptedVersion: 44)
  - A new field LookupExpr was added to JoinReaderSpec for supporting
    lookup joins with multiple spans per input row.

- Version: 45 (MinAcceptedVersion: 44)
  - A new field PrefixEqualityColumns was added to InvertedJoinerSpec for
    performing inverted joins on multi-column inverted indexes.

- Version: 44 (MinAcceptedVersion: 44)
  - Changes to the component statistics proto.

- Version: 43 (MinAcceptedVersion: 43)
	- Filter was removed from PostProcessSpec and a new Filterer processor was
	  added.

- Version: 42 (MinAcceptedVersion: 42)
  - A new field NeededColumns is added to TableReaderSpec which is now required
    by the vectorized ColBatchScans to be set up.

- Version: 41 (MinAcceptedVersion: 40)
  - A paired joiner approach for lookup joins was added, for left
    outer/semi/anti joins involving a pair of joinReaders, where the
    first join uses a non-covering index.

- Version: 40 (MinAcceptedVersion: 40)
  - A new field was added execinfrapb.ProcessorSpec to propagate the result
    column types of a processor. This change is not backwards compatible
    because from now on the specs are expected to have that field set and the
    field can be used during the flow setup.

- Version: 39 (MinAcceptedVersion: 39)
  - Many parameters from sessiondata.SessionData object were pulled into a
    protobuf struct for easier propagation to the remote nodes during the
    execution.

- Version: 38 (MinAcceptedVersion: 37)
  - A paired joiner approach for inverted joins was added, for left
    outer/semi/anti joins involving the invertedJoiner and joinReader.

- Version: 37 (MinAcceptedVersion: 37)
  - An InterleavedReaderJoiner processor was removed, and the old processor
    spec would be unrecognized by a server running older versions, hence the
    bump to the version and to the minimal accepted version.

- Version: 36 (MinAcceptedVersion: 35)
    - Added an aggregator for ST_Collect. The change is backwards compatible
      (mixed versions will prevent parallelization).

- Version: 35 (MinAcceptedVersion: 35)
    - Changed how system columns are represented in DistSQL Flows.

- Version: 34 (MinAcceptedVersion: 30)
    - Added a boolean field to InvertedJoinerSpec to maintain ordering. The
      change is backwards compatible because the invertedJoiner already
      maintained ordering by default (mixed versions will prevent
      parallelization).

- Version: 33 (MinAcceptedVersion: 30)
    - Added an aggregator for ST_Union. The change is backwards compatible
      (mixed versions will prevent parallelization).

- Version: 32 (MinAcceptedVersion: 30)
    - Added an aggregator for ST_Extent. The change is backwards compatible
      (mixed versions will prevent parallelization).

- Version: 31 (MinAcceptedVersion: 30)
    - The TableReader field MaxResults was retired in favor of the new field
      Parallelize. The change is backwards compatible (mixed versions will
      prevent parallelization).

- Version: 30 (MinAcceptedVersion: 30)
    - The Sampler and SampleAggregator specs for optimizer statistics have
      added fields for inverted sketches. Two new row types are produced by
      Samplers, which are not backward compatible with the previous version.

- Version: 29 (MinAcceptedVersion: 29)
    - The hashing of DArray datums was changed and is incompatible with the
      existing method of hashing DArrays.

- Version: 28 (MinAcceptedVersion: 28)
    - The CORR aggregate function has been added which will not be recognized
      by older nodes. However, new nodes can process plans from older nodes,
      so MinAcceptedVersion is unchanged.

- Version: 27 (MinAcceptedVersion: 27)
    - Change how DArray datums are hashed for distinct and aggregation operations.

- Version: 26 (MinAcceptedVersion: 24)
    - Add the numeric tuple field accessor and the ByIndex field to ColumnAccessExpr.

- Version: 25 (MinAcceptedVersion: 24)
    - Add locking strength and wait policy fields to TableReader, JoinReader,
      IndexSkipTableReader, and InterleavedReaderJoiner spec.

- Version: 24 (MinAcceptedVersion: 24)
    - Remove the unused index filter expression field from the lookup join spec.

- Version: 23 (MinAcceptedVersion: 23)
    - Refactoring of window functions. The main changes were reusing of renders
      whenever possible (for arguments and OVER clauses) which changed the way
      that information is propagated to windowers - a new field ArgsIdxs was
      introduced in place of ArgIdxStart and ArgCount. Another field was added
      to specify the output column for each window function (previously, this
      was derived from ArgIdxStart during execution).

- Version: 22 (MinAcceptedVersion: 21)
    - Change date math to better align with PostgreSQL:
      https://github.com/cockroachdb/cockroach/pull/31146

- MinAcceptedVersion: 21
    - Bump in preparation for the 2.1 release. A large amount of changes
      in both IMPORT and SQL execution have merged since 2.0. We have not
      adequately tested them in skewed version clusters to have confidence
      that they are compatible. We decided it was safer to bump the min
      version to prevent possible bugs at the cost of performance during
      the upgrade.

- Version: 21 (MinAcceptedVersion: 6)
    - Permit non-public (mutation) columns in TableReader return values, when
      requested. The new processor spec would be ignored by old versions.

- Version: 20 (MinAcceptedVersion: 6)
    - Add labels to tuple types.

- Version: 19 (MinAcceptedVersion: 6)
    - More EvalContext fields.

- Version: 18 (MinAcceptedVersion: 6)
    - New EvalContext field.

- Version: 17 (MinAcceptedVersion: 6)
    - Add scalar vs non-scalar designation to aggregator.

- Version: 15 (MinAcceptedVersion: 6)
    - Add SRF support via a new ProjectSet processor. The new processor spec
      would not be recognized by old versions.

- Version: 14 (MinAcceptedVersion: 6)
    - Enhancements to lookup joins. They now support joining against secondary
      indexes as well as left outer joins. Left join support required two
      additional fields on JoinReaderSpec: index_filter_expr and type.
    - Add support for processing queries with tuples in DistSQL. Old versions
      would not recognize the new tuple field in the proto. (This change was
      introduced without bumping the version.)

- Version: 13 (MinAcceptedVersion: 6)
    - Add NumRows to ValuesSpec (used for zero-column case). The implementation
      tolerates this field being unset in existing planning cases (at lest one
      column).

- Version: 12 (MinAcceptedVersion: 6)
    - The AggregatorSpec has a new OrderedGroupCols field, which older versions
      will not recognize.

- Version: 11 (MinAcceptedVersion: 6)
    - The txn field in the SetupFlowRequest is made nullable. Backfiller flows
      no longer pass in a txn (it was passed by the gateway, but unused by the
      remote processors before).

- Version: 10 (MinAcceptedVersion: 6)
    - The AlgebraicSetOp processor was removed, as it is not used. It was never
      used in plans, but its absence does change the ProcessorCoreUnion message,
      so a version bump is required.

- Version: 9 (MinAcceptedVersion: 6)
  - Many changes were made without bumping the version. These changes involved
    changes to the semantics of existing processors. These changes include:
    - Two new join types (LEFT_SEMI, LEFT_ANTI) were added to the JoinType enum
      to support new types of joins that can be used to improve performance of
      certain queries. The new join types would not be recognized by a server
      with an old version number.
    - The JoinReaderSpec was modified by adding three new properties
      (lookup_columns, on_expr, and index_map) to enable the JoinReader to perform
      lookup joins enabling more efficient joins for certain queries. Old server
      versions would not recognize these new properties.
    - Two new join types (INTERSECT_ALL, EXCEPT_ALL) were added to the descpb.JoinType
      enum to support INTERSECT and EXCEPT queries. These "set operation" queries
      can now be planned by DistSQL, and older versions will not be capable of
      supporting these queries.
    - The ARRAY_AGG and JSON_AGG aggregate functions were added to the
      AggregatorSpec_Func enum. Older versions will not recognize these new enum
      members, and will not be able to support aggregations with either.
    - IMPORT was changed to support importing CSVs with no primary key specified.
      This required a change to the CSV proto semantics, making old and new
      versions mutually incompatible. This was an EXPERIMENTAL feature in 1.1 and
      thus we do not bump the MinAcceptedVersion, even though IMPORT in mixed
      clusters without primary keys will succeed with incorrect results.
    - TableReader was extended to support physical checks. Old versions would not
      recognize these new fields.

- Version: 8 (MinAcceptedVersion: 6)
  - An InterleavedReaderJoiner processor was introduced to enable more
    efficient joins between interleaved tables. The new processor spec would be
    unrecognized by a server running older versions, hence the version bump.
    Servers running v8 can still execute regular joins between interleaved
    tables from servers running v6, thus the MinAcceptedVersion is kept at 6.

- Version: 7 (MinAcceptedVersion: 6)
  - Three new aggregations (SQRDIFF, FINAL_VARIANCE, FINAL_STDDEV) were
    introduced to support local and final aggregation of the builtin
    functions STDDEV and VARIANCE. These new aggregations would be unrecognized
    by a server running older versions, hence the version bump. However, a
    server running v7 can still process all plans from servers running v6,
    thus the MinAcceptedVersion is kept at 6.

- Version: 6 (MinAcceptedVersion: 6)
  - The producer<->consumer (FlowStream RPC) protocol has changed: the
    consumer_signal.handshake field was introduced. The handshake messages are
    sent by the server-side of FlowStream informing the producer of the
    consumer's status. The messages would cause a server running the previous
    version to erroneously think that the consumer has sent a drain signal,
    therefore MinAcceptedVersion was increased.

*/
