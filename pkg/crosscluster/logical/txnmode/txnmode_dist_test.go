// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnmode

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/logical/ldrsettings"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/logical/txnpb"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

func TestBuildTxnReplicationPlan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tests := []struct {
		name               string
		gatewayID          base.SQLInstanceID
		applierInstanceIDs []base.SQLInstanceID
	}{
		{
			name:               "single node",
			gatewayID:          1,
			applierInstanceIDs: []base.SQLInstanceID{1},
		},
		{
			name:               "three nodes",
			gatewayID:          1,
			applierInstanceIDs: []base.SQLInstanceID{1, 2, 3},
		},
		{
			name:               "five nodes gateway not in appliers",
			gatewayID:          1,
			applierInstanceIDs: []base.SQLInstanceID{2, 3, 4, 5, 6},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			n := len(tc.applierInstanceIDs)

			infra := physicalplan.NewPhysicalInfrastructure(
				uuid.MakeV4(), tc.gatewayID,
			)
			plan := &sql.PhysicalPlan{
				PhysicalPlan: physicalplan.MakePhysicalPlan(infra),
			}

			spec := execinfrapb.TxnLDRCoordinatorSpec{
				JobID: 123,
			}

			err := buildTxnReplicationPlan(ctx, plan, tc.applierInstanceIDs, spec)
			require.NoError(t, err)

			// Count processors by type.
			var coordinators, appliers, depResolvers, noops int
			for _, p := range plan.Processors {
				switch {
				case p.Spec.Core.TxnLdrCoordinator != nil:
					coordinators++
				case p.Spec.Core.TxnLdrApplier != nil:
					appliers++
				case p.Spec.Core.TxnLdrDepResolver != nil:
					depResolvers++
				case p.Spec.Core.Noop != nil:
					noops++
				}
			}

			require.Equal(t, 1, coordinators, "expected 1 coordinator")
			require.Equal(t, n, appliers, "expected %d appliers", n)
			require.Equal(t, n, depResolvers, "expected %d dep resolvers", n)
			require.Equal(t, 1, noops, "expected 1 noop")
			require.Len(t, plan.Processors, 2*n+2, "total processors")

			// Coordinator and noop are on the gateway.
			for _, p := range plan.Processors {
				if p.Spec.Core.TxnLdrCoordinator != nil {
					require.Equal(t, tc.gatewayID, p.SQLInstanceID,
						"coordinator should be on gateway")
				}
				if p.Spec.Core.Noop != nil {
					require.Equal(t, tc.gatewayID, p.SQLInstanceID,
						"noop should be on gateway")
				}
			}

			// Applier and dep resolver co-location: for each instance,
			// there must be exactly one applier and one dep resolver.
			applierInstances := make([]base.SQLInstanceID, 0, n)
			depResolverInstances := make([]base.SQLInstanceID, 0, n)
			for _, p := range plan.Processors {
				if p.Spec.Core.TxnLdrApplier != nil {
					applierInstances = append(applierInstances, p.SQLInstanceID)
					require.Equal(t, int32(p.SQLInstanceID),
						p.Spec.Core.TxnLdrApplier.ApplierID,
						"applier ID must match instance ID")
				}
				if p.Spec.Core.TxnLdrDepResolver != nil {
					depResolverInstances = append(depResolverInstances, p.SQLInstanceID)
					require.Equal(t, int32(p.SQLInstanceID),
						p.Spec.Core.TxnLdrDepResolver.ApplierID,
						"dep resolver applier ID must match instance ID")
				}
			}

			sort.Slice(applierInstances, func(i, j int) bool {
				return applierInstances[i] < applierInstances[j]
			})
			sort.Slice(depResolverInstances, func(i, j int) bool {
				return depResolverInstances[i] < depResolverInstances[j]
			})
			require.Equal(t, applierInstances, depResolverInstances,
				"applier and dep resolver instances must match")

			// Stream count: N (coord→applier) + N² (applier→dep resolver)
			// + N (dep resolver→noop, added by AddSingleGroupStage).
			expectedStreams := n + n*n + n
			require.Len(t, plan.Streams, expectedStreams,
				"expected %d streams", expectedStreams)
		})
	}
}

func TestBoundingSpan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	sp := func(start, end string) roachpb.Span {
		return roachpb.Span{
			Key: roachpb.Key(start), EndKey: roachpb.Key(end),
		}
	}
	ps := func(spans ...roachpb.Span) execinfrapb.StreamIngestionPartitionSpec {
		return execinfrapb.StreamIngestionPartitionSpec{Spans: spans}
	}

	tests := []struct {
		name     string
		specs    []execinfrapb.StreamIngestionPartitionSpec
		expected roachpb.Span
	}{
		{
			name:     "single partition single span",
			specs:    []execinfrapb.StreamIngestionPartitionSpec{ps(sp("b", "d"))},
			expected: sp("b", "d"),
		},
		{
			name:     "single partition multiple spans",
			specs:    []execinfrapb.StreamIngestionPartitionSpec{ps(sp("b", "d"), sp("a", "f"))},
			expected: sp("a", "f"),
		},
		{
			name: "multiple partitions",
			specs: []execinfrapb.StreamIngestionPartitionSpec{
				ps(sp("c", "e")),
				ps(sp("a", "g")),
			},
			expected: sp("a", "g"),
		},
		{
			name: "partition zero empty",
			specs: []execinfrapb.StreamIngestionPartitionSpec{
				ps(),
				ps(sp("b", "d")),
				ps(sp("a", "f")),
			},
			expected: sp("a", "f"),
		},
		{
			name: "middle partition empty",
			specs: []execinfrapb.StreamIngestionPartitionSpec{
				ps(sp("b", "d")),
				ps(),
				ps(sp("a", "f")),
			},
			expected: sp("a", "f"),
		},
		{
			name: "all partitions empty",
			specs: []execinfrapb.StreamIngestionPartitionSpec{
				ps(), ps(),
			},
			expected: roachpb.Span{},
		},
		{
			name:     "no partitions",
			specs:    []execinfrapb.StreamIngestionPartitionSpec{},
			expected: roachpb.Span{},
		},
		{
			name:     "nil input",
			specs:    nil,
			expected: roachpb.Span{},
		},
		{
			name: "overlapping spans across partitions",
			specs: []execinfrapb.StreamIngestionPartitionSpec{
				ps(sp("b", "e"), sp("c", "d")),
				ps(sp("a", "c"), sp("d", "g")),
			},
			expected: sp("a", "g"),
		},
		{
			name: "disjoint spans",
			specs: []execinfrapb.StreamIngestionPartitionSpec{
				ps(sp("a", "b")),
				ps(sp("x", "z")),
			},
			expected: sp("a", "z"),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := boundingSpan(tc.specs)
			require.Equal(t, tc.expected, result)
		})
	}
}

func mkts(wallTime int64) hlc.Timestamp {
	return hlc.Timestamp{WallTime: wallTime}
}

func makeFrontierMeta(
	applierID base.SQLInstanceID, frontier hlc.Timestamp,
) *execinfrapb.ProducerMetadata {
	progressBytes, err := protoutil.Marshal(&txnpb.TxnLDRProcProgress{
		ApplierID:  int32(applierID),
		Checkpoint: frontier,
	})
	if err != nil {
		panic(err)
	}
	return &execinfrapb.ProducerMetadata{
		BulkProcessorProgress: &execinfrapb.RemoteProducerMetadata_BulkProcessorProgress{
			ProgressMessage: progressBytes,
		},
	}
}

func TestMinFrontier(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tests := []struct {
		name             string
		applierFrontiers map[base.SQLInstanceID]hlc.Timestamp
		expected         hlc.Timestamp
	}{
		{
			name:             "single applier set",
			applierFrontiers: map[base.SQLInstanceID]hlc.Timestamp{1: mkts(10)},
			expected:         mkts(10),
		},
		{
			name:             "single applier unset",
			applierFrontiers: map[base.SQLInstanceID]hlc.Timestamp{1: {}},
			expected:         hlc.Timestamp{},
		},
		{
			name:             "two appliers both set same",
			applierFrontiers: map[base.SQLInstanceID]hlc.Timestamp{1: mkts(5), 2: mkts(5)},
			expected:         mkts(5),
		},
		{
			name:             "two appliers both set different",
			applierFrontiers: map[base.SQLInstanceID]hlc.Timestamp{1: mkts(5), 2: mkts(10)},
			expected:         mkts(5),
		},
		{
			name:             "two appliers one unset",
			applierFrontiers: map[base.SQLInstanceID]hlc.Timestamp{1: mkts(5), 2: {}},
			expected:         hlc.Timestamp{},
		},
		{
			name:             "two appliers both unset",
			applierFrontiers: map[base.SQLInstanceID]hlc.Timestamp{1: {}, 2: {}},
			expected:         hlc.Timestamp{},
		},
		{
			name: "three appliers min in middle",
			applierFrontiers: map[base.SQLInstanceID]hlc.Timestamp{
				1: mkts(10), 2: mkts(3), 3: mkts(7),
			},
			expected: mkts(3),
		},
		{
			name:             "empty map",
			applierFrontiers: map[base.SQLInstanceID]hlc.Timestamp{},
			expected:         hlc.Timestamp{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ch := &checkpointHandler{applierFrontiers: tc.applierFrontiers}
			require.Equal(t, tc.expected, ch.minFrontier())
		})
	}
}

func TestCheckpointHandler(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	registry := s.JobRegistry().(*jobs.Registry)
	internalDB := s.InternalDB().(isql.DB)
	sv := &s.ApplicationLayer().ClusterSettings().SV
	db := sqlutils.MakeSQLRunner(s.ApplicationLayer().SQLConn(t))

	// Disable the checkpoint throttle for most subtests so back-to-back
	// handleMeta calls persist every frontier advance.
	ldrsettings.JobCheckpointFrequency.Override(ctx, sv, time.Nanosecond)

	createJob := func(t *testing.T) *jobs.Job {
		t.Helper()
		record := jobs.Record{
			Details:  jobspb.LogicalReplicationDetails{},
			Progress: jobspb.LogicalReplicationProgress{},
			Username: username.TestUserName(),
		}
		job, err := registry.CreateJobWithTxn(ctx, record, registry.MakeJobID(), nil)
		require.NoError(t, err)
		return job
	}

	drainChannel := func(ch <-chan hlc.Timestamp) []hlc.Timestamp {
		var result []hlc.Timestamp
		for {
			select {
			case ts := <-ch:
				result = append(result, ts)
			default:
				return result
			}
		}
	}

	t.Run("fresh_start", func(t *testing.T) {
		job := createJob(t)
		frontierCh := make(chan hlc.Timestamp, 100)
		applierIDs := []base.SQLInstanceID{1, 2}

		ch := newCheckpointHandler(
			job, internalDB, sv, frontierCh, applierIDs, hlc.Timestamp{}, hlc.MaxTimestamp,
			func() {},
		)

		// Only applier 1 reports: minFrontier blocked by applier 2.
		require.NoError(t, ch.handleMeta(ctx, makeFrontierMeta(1, mkts(5))))
		require.Empty(t, drainChannel(frontierCh))

		// Applier 2 reports: min is now ts(5).
		require.NoError(t, ch.handleMeta(ctx, makeFrontierMeta(2, mkts(10))))
		updates := drainChannel(frontierCh)
		require.Len(t, updates, 1)
		require.Equal(t, mkts(5), updates[0])

		// Verify exactly one progress history row with the right resolved value.
		db.CheckQueryResults(t,
			fmt.Sprintf(
				"SELECT resolved FROM system.job_progress_history WHERE job_id = %d",
				job.ID(),
			),
			[][]string{{mkts(5).AsOfSystemTime()}},
		)
	})

	t.Run("resume", func(t *testing.T) {
		job := createJob(t)
		frontierCh := make(chan hlc.Timestamp, 100)
		applierIDs := []base.SQLInstanceID{1, 2}
		resumeTS := mkts(10)

		ch := newCheckpointHandler(
			job, internalDB, sv, frontierCh, applierIDs, resumeTS, hlc.MaxTimestamp,
			func() {},
		)

		// Applier 1 at ts(12): min is ts(10) from applier 2's seed, which
		// equals the seeded replicatedTime, so no checkpoint is emitted.
		require.NoError(t, ch.handleMeta(ctx, makeFrontierMeta(1, mkts(12))))
		require.Empty(t, drainChannel(frontierCh))

		// Applier 2 at ts(15): min advances to ts(12), past the seed.
		require.NoError(t, ch.handleMeta(ctx, makeFrontierMeta(2, mkts(15))))
		require.Equal(t, mkts(12), drainChannel(frontierCh)[0])

		// Applier 1 at ts(20): min advances to ts(15).
		require.NoError(t, ch.handleMeta(ctx, makeFrontierMeta(1, mkts(20))))
		require.Equal(t, mkts(15), drainChannel(frontierCh)[0])

		// Verify all persisted checkpoints are monotonically non-decreasing.
		rows := db.QueryStr(t,
			fmt.Sprintf(
				"SELECT resolved FROM system.job_progress_history "+
					"WHERE job_id = %d ORDER BY written ASC",
				job.ID(),
			),
		)
		require.Len(t, rows, 2)
		for i := 1; i < len(rows); i++ {
			require.LessOrEqual(t, rows[i-1][0], rows[i][0],
				"checkpoint regressed at row %d: %s > %s",
				i, rows[i-1][0], rows[i][0],
			)
		}
	})

	t.Run("end_time_reached", func(t *testing.T) {
		job := createJob(t)
		frontierCh := make(chan hlc.Timestamp, 100)
		applierIDs := []base.SQLInstanceID{1, 2}
		endTime := mkts(20)
		var flowCanceled bool

		ch := newCheckpointHandler(
			job, internalDB, sv, frontierCh, applierIDs, hlc.Timestamp{}, endTime,
			func() { flowCanceled = true },
		)

		// Both appliers report below endTime: no error yet.
		require.NoError(t, ch.handleMeta(ctx, makeFrontierMeta(1, mkts(15))))
		require.NoError(t, ch.handleMeta(ctx, makeFrontierMeta(2, mkts(18))))
		updates := drainChannel(frontierCh)
		require.Len(t, updates, 1)
		require.Equal(t, mkts(15), updates[0])

		// Applier 1 advances: min frontier is ts(18), still below endTime.
		require.NoError(t, ch.handleMeta(ctx, makeFrontierMeta(1, mkts(25))))
		updates = drainChannel(frontierCh)
		require.Equal(t, mkts(18), updates[0])

		// Applier 2 advances: min becomes ts(25) which is > endTime.
		err := ch.handleMeta(ctx, makeFrontierMeta(2, mkts(30)))
		require.ErrorIs(t, err, ErrEndTimeReached)
		require.True(t, flowCanceled)
	})

	t.Run("checkpoint_throttle", func(t *testing.T) {
		ldrsettings.JobCheckpointFrequency.Override(ctx, sv, time.Hour)

		job := createJob(t)
		frontierCh := make(chan hlc.Timestamp, 100)
		applierIDs := []base.SQLInstanceID{1}

		endTime := mkts(100)
		var flowCanceled bool
		ch := newCheckpointHandler(
			job, internalDB, sv, frontierCh, applierIDs, hlc.Timestamp{}, endTime,
			func() { flowCanceled = true },
		)

		// First advance always persists (zero-value lastPersistenceTime).
		require.NoError(t, ch.handleMeta(ctx, makeFrontierMeta(1, mkts(10))))
		require.Len(t, drainChannel(frontierCh), 1)

		// Intermediate advances within the throttle window are skipped.
		require.NoError(t, ch.handleMeta(ctx, makeFrontierMeta(1, mkts(20))))
		require.Empty(t, drainChannel(frontierCh))
		require.NoError(t, ch.handleMeta(ctx, makeFrontierMeta(1, mkts(30))))
		require.Empty(t, drainChannel(frontierCh))

		// Reaching endTime forces persistence regardless of throttle.
		err := ch.handleMeta(ctx, makeFrontierMeta(1, mkts(100)))
		require.ErrorIs(t, err, ErrEndTimeReached)
		require.True(t, flowCanceled)
		require.Equal(t, mkts(100), drainChannel(frontierCh)[0])
	})
}
