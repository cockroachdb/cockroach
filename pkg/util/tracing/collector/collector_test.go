// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package collector_test

import (
	"context"
	gosql "database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlinstance"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/collector"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/types"
	"github.com/stretchr/testify/require"
)

// testStructuredImpl is a testing implementation of Structured event.
type testStructuredImpl struct {
	*types.StringValue
}

var _ tracing.Structured = &testStructuredImpl{}

func (t *testStructuredImpl) String() string {
	return fmt.Sprintf("structured=%s", t.Value)
}

func newTestStructured(i string) *testStructuredImpl {
	return &testStructuredImpl{
		&types.StringValue{Value: i},
	}
}

// setupTraces takes two tracers (potentially on different nodes), and creates
// two span hierarchies as depicted below. The method returns the traceIDs for
// both these span hierarchies, along with a cleanup method to Finish() all the
// opened spans.
//
// Trace for t1:
// -------------
// root													<-- traceID1
//
//	root.child								<-- traceID1
//
// root2.child.remotechild 			<-- traceID2
// root2.child.remotechild2 		<-- traceID2
//
// Trace for t2:
// -------------
// root.child.remotechild				<-- traceID1
// root.child.remotechilddone		<-- traceID1
// root2												<-- traceID2
//
//	root2.child								<-- traceID2
func setupTraces(t1, t2 *tracing.Tracer) (tracingpb.TraceID, tracingpb.TraceID, func()) {
	// Start a root span on "node 1".
	root := t1.StartSpan("root", tracing.WithRecording(tracingpb.RecordingVerbose))
	root.RecordStructured(newTestStructured("root"))

	time.Sleep(10 * time.Millisecond)

	// Start a child span on "node 1".
	child := t1.StartSpan("root.child", tracing.WithParent(root))

	// Sleep a bit so that everything that comes afterwards has higher timestamps
	// than the one we just assigned. Otherwise the sorting is not deterministic.
	time.Sleep(10 * time.Millisecond)

	// Start a remote child span on "node 2".
	childRemoteChild := t2.StartSpan("root.child.remotechild", tracing.WithRemoteParentFromSpanMeta(child.Meta()))
	childRemoteChild.RecordStructured(newTestStructured("root.child.remotechild"))

	time.Sleep(10 * time.Millisecond)

	// Start another remote child span on "node 2" that we finish.
	childRemoteChildFinished := t2.StartSpan("root.child.remotechilddone", tracing.WithRemoteParentFromSpanMeta(child.Meta()))
	child.ImportRemoteRecording(childRemoteChildFinished.FinishAndGetRecording(tracingpb.RecordingVerbose))

	// Start a root span on "node 2".
	root2 := t2.StartSpan("root2", tracing.WithRecording(tracingpb.RecordingVerbose))
	root2.RecordStructured(newTestStructured("root2"))

	// Start a child span on "node 2".
	child2 := t2.StartSpan("root2.child", tracing.WithParent(root2))
	// Start a remote child span on "node 1".
	child2RemoteChild := t1.StartSpan("root2.child.remotechild", tracing.WithRemoteParentFromSpanMeta(child2.Meta()))

	time.Sleep(10 * time.Millisecond)

	// Start another remote child span on "node 1".
	anotherChild2RemoteChild := t1.StartSpan("root2.child.remotechild2", tracing.WithRemoteParentFromSpanMeta(child2.Meta()))
	return root.TraceID(), root2.TraceID(), func() {
		for _, span := range []*tracing.Span{root, child, childRemoteChild, root2, child2,
			child2RemoteChild, anotherChild2RemoteChild} {
			span.Finish()
		}
	}
}

func TestTracingCollectorGetSpanRecordings(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	args := base.TestClusterArgs{}
	tc := testcluster.StartTestCluster(t, 2 /* nodes */, args)
	defer tc.Stopper().Stop(ctx)

	s0 := tc.Server(0).ApplicationLayer()
	s1 := tc.Server(1).ApplicationLayer()

	localTracer := s0.TracerI().(*tracing.Tracer)
	remoteTracer := s1.TracerI().(*tracing.Tracer)

	traceCollector := collector.New(
		localTracer,
		func(ctx context.Context) ([]sqlinstance.InstanceInfo, error) {
			instanceIDs := make([]sqlinstance.InstanceInfo, len(tc.Servers))
			for i := range tc.Servers {
				instanceIDs[i].InstanceID = tc.Server(i).ApplicationLayer().SQLInstanceID()
			}
			return instanceIDs, nil
		},
		s0.NodeDialer().(*nodedialer.Dialer))
	localTraceID, remoteTraceID, cleanup := setupTraces(localTracer, remoteTracer)
	defer cleanup()

	getSpansFromAllInstances := func(traceID tracingpb.TraceID) map[base.SQLInstanceID][]tracingpb.Recording {
		res := make(map[base.SQLInstanceID][]tracingpb.Recording)
		iter, err := traceCollector.StartIter(ctx, traceID)
		require.NoError(t, err)
		for ; iter.Valid(); iter.Next(ctx) {
			instanceID, recording := iter.Value()
			res[instanceID] = append(res[instanceID], recording)
		}
		return res
	}

	t.Run("fetch-local-recordings", func(t *testing.T) {
		nodeRecordings := getSpansFromAllInstances(localTraceID)
		node1Recordings := nodeRecordings[s0.SQLInstanceID()]
		require.Equal(t, 1, len(node1Recordings))
		require.NoError(t, tracing.CheckRecordedSpans(node1Recordings[0], `
				span: root
					tags: _unfinished=1 _verbose=1
					event: structured=root
					span: root.child
						tags: _unfinished=1 _verbose=1
						span: root.child.remotechilddone
							tags: _verbose=1
	`))
		node2Recordings := nodeRecordings[s1.SQLInstanceID()]
		require.Equal(t, 1, len(node2Recordings))
		require.NoError(t, tracing.CheckRecordedSpans(node2Recordings[0], `
				span: root.child.remotechild
					tags: _unfinished=1 _verbose=1
					event: structured=root.child.remotechild
	`))
	})

	// The traceCollector is running on node 1, so most of the recordings for this
	// subtest will be passed back by node 2 over RPC.
	t.Run("fetch-remote-recordings", func(t *testing.T) {
		nodeRecordings := getSpansFromAllInstances(remoteTraceID)
		node1Recordings := nodeRecordings[s0.SQLInstanceID()]
		require.Equal(t, 2, len(node1Recordings))
		require.NoError(t, tracing.CheckRecordedSpans(node1Recordings[0], `
				span: root2.child.remotechild
					tags: _unfinished=1 _verbose=1
	`))
		require.NoError(t, tracing.CheckRecordedSpans(node1Recordings[1], `
				span: root2.child.remotechild2
					tags: _unfinished=1 _verbose=1
	`))

		node2Recordings := nodeRecordings[s1.SQLInstanceID()]
		require.Equal(t, 1, len(node2Recordings))
		require.NoError(t, tracing.CheckRecordedSpans(node2Recordings[0], `
				span: root2
					tags: _unfinished=1 _verbose=1
					event: structured=root2
					span: root2.child
						tags: _unfinished=1 _verbose=1
	`))
	})
}

// Test that crdb_internal.cluster_inflight_traces works, both in tenants and in
// mixed nodes.
func TestClusterInflightTraces(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	args := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			DefaultTestTenant: base.TestControlsTenantsExplicitly,
		},
	}

	for _, config := range []string{
		"single-tenant",
		"shared-process",
		"separate-process",
	} {
		t.Run(config, func(t *testing.T) {
			tc := testcluster.StartTestCluster(t, 2 /* nodes */, args)
			defer tc.Stopper().Stop(ctx)

			systemServers := []serverutils.ApplicationLayerInterface{
				tc.SystemLayer(0),
				tc.SystemLayer(1),
			}
			systemDBs := make([]*gosql.DB, len(tc.Servers))
			for i, s := range tc.Servers {
				systemDBs[i] = s.SQLConn(t)
			}

			type testCase struct {
				name    string
				servers []serverutils.ApplicationLayerInterface
				dbs     []*gosql.DB
				// otherServers, if set, represents the servers corresponding to
				// other tenants (or to the system tenant) than the ones being
				// tested.
				otherServers []serverutils.ApplicationLayerInterface
			}
			var testCases []testCase
			switch config {
			case "single-tenant":
				testCases = []testCase{{
					name:    "system-tenant",
					servers: []serverutils.ApplicationLayerInterface{tc.Servers[0], tc.Servers[1]},
					dbs:     systemDBs,
				}}

			case "shared-process":
				tenants := make([]serverutils.ApplicationLayerInterface, len(tc.Servers))
				dbs := make([]*gosql.DB, len(tc.Servers))
				for i, s := range tc.Servers {
					tenant, db, err := s.TenantController().StartSharedProcessTenant(ctx, base.TestSharedProcessTenantArgs{TenantName: "app"})
					require.NoError(t, err)
					tenants[i] = tenant
					dbs[i] = db
				}
				testCases = []testCase{
					{
						name:         "application-tenant",
						servers:      tenants,
						dbs:          dbs,
						otherServers: systemServers,
					},
					{
						name:         "system-tenant",
						servers:      systemServers,
						dbs:          systemDBs,
						otherServers: tenants,
					},
				}

			case "separate-process":
				tenantID := roachpb.MustMakeTenantID(10)
				tenants := make([]serverutils.ApplicationLayerInterface, len(tc.Servers))
				dbs := make([]*gosql.DB, len(tc.Servers))
				for i := range tc.Servers {
					tenant, err := tc.Servers[i].TenantController().StartTenant(ctx, base.TestTenantArgs{TenantID: tenantID})
					require.NoError(t, err)
					tenants[i] = tenant
					dbs[i] = tenant.SQLConn(t)
				}
				testCases = []testCase{
					{
						name:         "application-tenant",
						servers:      tenants,
						dbs:          dbs,
						otherServers: systemServers,
					},
					{
						name:         "system-tenant",
						servers:      systemServers,
						dbs:          systemDBs,
						otherServers: tenants,
					},
				}
			}

			for _, tc := range testCases {
				t.Run(tc.name, func(t *testing.T) {
					// Set up the traces we're going to look for.
					localTraceID, _, cleanup := setupTraces(tc.servers[0].Tracer(), tc.servers[1].Tracer())
					defer cleanup()

					// Create some other spans on tc.otherServers, that we don't
					// expect to find.
					const otherServerSpanName = "other-server-span"
					for _, s := range tc.otherServers {
						sp := s.Tracer().StartSpan(otherServerSpanName)
						defer sp.Finish()
					}

					// We're going to query the cluster_inflight_traces through every SQL instance.
					// We use SucceedsSoon because sqlInstanceReader.GetAllInstances is powered by a
					// cache under the hood that isn't guaranteed to be consistent, so we give the
					// cache extra time to populate while the tenant startup process completes.
					testutils.SucceedsSoon(t, func() error {
						for _, db := range tc.dbs {
							rows, err := db.Query(
								"SELECT node_id, trace_str FROM crdb_internal.cluster_inflight_traces "+
									"WHERE trace_id=$1 ORDER BY node_id",
								localTraceID)
							if err != nil {
								return errors.Wrap(err, "failed to query crdb_internal.cluster_inflight_traces")
							}
							expSpans := map[int][]string{
								1: {"root", "root.child", "root.child.remotechilddone"},
								2: {"root.child.remotechild"},
							}
							for rows.Next() {
								var nodeID int
								var trace string
								if err := rows.Scan(&nodeID, &trace); err != nil {
									return errors.Wrap(err, "failed to scan row")
								}
								exp, ok := expSpans[nodeID]
								if !ok {
									return errors.Newf("no expected spans found for nodeID %d", nodeID)
								}
								delete(expSpans, nodeID) // Consume this entry; we'll check that they were all consumed.
								for _, span := range exp {
									spanName := "=== operation:" + span
									if !strings.Contains(trace, spanName) {
										return errors.Newf("failed to find span %q in trace %q", spanName, trace)
									}
								}
								otherServerSpanOp := "=== operation:" + otherServerSpanName
								if strings.Contains(trace, otherServerSpanOp) {
									return errors.Newf("unexpected span %q found in trace %q", otherServerSpanOp, trace)
								}
							}
							if len(expSpans) != 0 {
								return errors.Newf("didn't find all expected spans, remaining spans: %v", expSpans)
							}
						}
						return nil
					})
				})
			}
		})
	}
}
