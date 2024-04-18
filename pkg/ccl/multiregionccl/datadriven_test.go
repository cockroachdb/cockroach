// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package multiregionccl_test

import (
	"context"
	gosql "database/sql"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/allocatorimpl"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
)

// TestMultiRegionDataDriven is a data-driven test to test various multi-region
// invariants at a high level. This is accomplished by allowing custom cluster
// configurations when creating the test cluster and providing directives to
// assert expectations in query traces.
//
// It offers the following commands:
//
// "new-cluster localities=<localities>": creates a new cluster with
// len(localities) number of nodes. A locality entry may be repeated to
// create more than one node in a region. The order in which the localities are
// provided may later be used to index into a particular node to run a query.
//
// "exec-sql idx=server_number": executes the input SQL query on the target
// server.
//
// "trace-sql idx=server_number":
// Similar to exec-sql, but also traces the input statement and analyzes the
// trace. Currently, the trace analysis only works for "simple" queries which
// perform a single kv operation. The trace is analyzed for the following:
//   - served locally: prints true iff the query was routed to the local
//     replica.
//   - served via follower read: prints true iff the query was served using a
//     follower read. This is omitted completely if the query was not served
//     locally.
//
// This is because it the replica the query is routed to may or may not be the
// leaseholder.
//
// "wait-for-zone-config-changes idx=server_number table-name=tbName
// [num-voters=num] [num-non-voters=num] [leaseholder=idx] [voter=idx_list]
// [non-voter=idx_list] [not-present=idx_list]": finds the
// range belonging to the given tbName's prefix key and runs it through the
// split, replicate, and raftsnapshot queues. If the num-voters or
// num-non-voters arguments are provided, it then makes sure that the range
// conforms to those.
// It also takes the arguments leaseholder, voter, nonvoter, and not-present,
// each of which take a comma-separated list of server indexes to check against
// the provided table.
// Note that if a server idx is not listed, it is unconstrained. This is
// necessary for cases where we know how many replicas of a type we want but
// we can't deterministically know their placement.
// If the leaseholder does not match the argument provided, a lease
// transfer is attempted.
// All this is done in a succeeds soon as any of these steps may error out for
// completely legitimate reasons.
//
// NB: It's worth noting that num-voters and num-non-voters must be provided as
// arguments and cannot be asserted on return similar to the way a lot of
// data-driven tests would do. This is because zone config updates, which are
// gossiped, may not have been seen by the leaseholder node when the replica is
// run through the replicate queue. As such, it is completely reasonable to have
// this operation retry until the node has seen these updates, which is why this
// whole operation (including asserting num-voters num-non-voters) is wrapped
// inside a succeeds soon. The only way to do this is to accept these things as
// arguments, instead of returning the result.
//
// "refresh-range-descriptor-cache idx=server_number table-name=tbName": runs
// the given query to refresh the range descriptor cache. Then, using the
// table-name argument, the closed timestamp policy is returned.
//
// "sleep-for-follower-read": sleeps for 4,4 seconds, the target duration for
// follower reads according to our cluster settings.
//
// "cleanup-cluster": destroys the cluster. Must be done before creating a new
// cluster.
func TestMultiRegionDataDriven(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// This test speeds up replication changes by proactively enqueuing replicas
	// into various queues. This has the benefit of reducing the time taken after
	// zone config changes, however the downside of added overhead. Disable the
	// test under deadlock builds, as the test is slow and susceptible to
	// (legitimate) timing issues on a deadlock build.
	skip.UnderRace(t, "flaky test")
	skip.UnderDeadlock(t, "flaky test")
	ctx := context.Background()
	datadriven.Walk(t, datapathutils.TestDataPath(t), func(t *testing.T, path string) {
		ds := datadrivenTestState{}
		defer ds.cleanup(ctx)
		var mu syncutil.Mutex
		var traceStmt string
		var recCh chan tracingpb.Recording
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "skip":
				var issue int
				d.ScanArgs(t, "issue-num", &issue)
				skip.WithIssue(t, issue)
				return ""
			case "sleep-for-follower-read":
				time.Sleep(time.Second)
			case "new-cluster":
				if ds.tc != nil {
					t.Fatal("cluster already exists, cleanup cluster first")
				}
				var localities string
				mustHaveArgOrFatal(t, d, serverLocalities)
				d.ScanArgs(t, serverLocalities, &localities)
				if localities == "" {
					t.Fatalf("can't create a cluster without %s", serverLocalities)
				}
				serverArgs := make(map[int]base.TestServerArgs)
				localityNames := strings.Split(localities, ",")
				recCh = make(chan tracingpb.Recording, 1)
				for i, localityName := range localityNames {
					localityCfg := roachpb.Locality{
						Tiers: []roachpb.Tier{
							{Key: "region", Value: localityName},
							{Key: "zone", Value: localityName + "a"},
						},
					}
					st := cluster.MakeClusterSettings()
					// Prevent rebalancing from happening automatically (i.e., make it
					// exceedingly unlikely).
					// TODO(rafi): use more explicit cluster setting once it's available;
					// see https://github.com/cockroachdb/cockroach/issues/110740.
					allocatorimpl.LeaseRebalanceThreshold.Override(ctx, &st.SV, 10.0)
					serverArgs[i] = base.TestServerArgs{
						Locality: localityCfg,
						Settings: st,
						Knobs: base.TestingKnobs{
							JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(), // speeds up test
							SQLExecutor: &sql.ExecutorTestingKnobs{
								WithStatementTrace: func(trace tracingpb.Recording, stmt string) {
									mu.Lock()
									defer mu.Unlock()
									if stmt == traceStmt {
										recCh <- trace
									}
								},
							},
							// NB: This test is asserting on whether it reads from the leaseholder
							// or the follower first, so it has to route to leaseholder when
							// requested.
							KVClient: &kvcoord.ClientTestingKnobs{RouteToLeaseholderFirst: true},
						},
					}
				}
				numServers := len(localityNames)
				tc := testcluster.StartTestCluster(t, numServers, base.TestClusterArgs{
					ServerArgsPerNode: serverArgs,
					ServerArgs: base.TestServerArgs{
						// We need to disable the default test tenant here
						// because it appears as though operations like
						// "wait-for-zone-config-changes" only work correctly
						// when called from the system tenant. More
						// investigation is required (tracked with #76378).
						DefaultTestTenant: base.TODOTestTenantDisabled,
					},
				})
				ds.tc = tc

				sqlConn, err := ds.getSQLConn(0)
				if err != nil {
					return err.Error()
				}
				// Speed up closing of timestamps, in order to sleep less below before
				// we can use follower_read_timestamp(). follower_read_timestamp() uses
				// sum of the following settings. Also, disable all kvserver lease
				// transfers other than those required to satisfy a lease preference.
				// This prevents the lease shifting around too quickly, which leads to
				// concurrent replication changes being proposed by prior leaseholders.
				for _, stmt := range strings.Split(`
SET CLUSTER SETTING kv.closed_timestamp.target_duration = '0.4s';
SET CLUSTER SETTING kv.closed_timestamp.side_transport_interval = '0.1s';
SET CLUSTER SETTING kv.closed_timestamp.propagation_slack = '0.5s';
SET CLUSTER SETTING kv.allocator.load_based_rebalancing = 'off';
SET CLUSTER SETTING kv.allocator.load_based_lease_rebalancing.enabled = false;
SET CLUSTER SETTING kv.allocator.min_lease_transfer_interval = '5m'
`,
					";") {
					_, err = sqlConn.Exec(stmt)
					if err != nil {
						return err.Error()
					}
				}

			case "cleanup-cluster":
				ds.cleanup(ctx)

			case "stop-server":
				mustHaveArgOrFatal(t, d, serverIdx)
				var idx int
				d.ScanArgs(t, serverIdx, &idx)
				ds.tc.StopServer(idx)

			case "exec-sql":
				mustHaveArgOrFatal(t, d, serverIdx)
				var err error
				var idx int
				d.ScanArgs(t, serverIdx, &idx)
				sqlDB, err := ds.getSQLConn(idx)
				if err != nil {
					return err.Error()
				}

				_, err = sqlDB.Exec(d.Input)
				if err != nil {
					return err.Error()
				}

			case "trace-sql":
				mustHaveArgOrFatal(t, d, serverIdx)
				var rec tracingpb.Recording
				queryFunc := func() (localRead bool, followerRead bool, err error) {
					var idx int
					d.ScanArgs(t, serverIdx, &idx)
					sqlDB, err := ds.getSQLConn(idx)
					if err != nil {
						return false, false, err
					}

					// Setup tracing for the input.
					mu.Lock()
					traceStmt = d.Input
					mu.Unlock()
					defer func() {
						mu.Lock()
						traceStmt = ""
						mu.Unlock()
					}()

					_, err = sqlDB.Query(d.Input)
					if err != nil {
						return false, false, err
					}
					rec = <-recCh
					localRead, followerRead, err = checkReadServedLocallyInSimpleRecording(rec)
					if err != nil {
						return false, false, err
					}
					return localRead, followerRead, nil
				}
				localRead, followerRead, err := queryFunc()
				if err != nil {
					return err.Error()
				}
				var output strings.Builder
				output.WriteString(
					fmt.Sprintf("served locally: %s\n", strconv.FormatBool(localRead)))
				// Only print follower read information if the query was served locally.
				if localRead {
					output.WriteString(
						fmt.Sprintf("served via follower read: %s\n", strconv.FormatBool(followerRead)))
				}
				if d.Expected != output.String() {
					return errors.AssertionFailedf("not a match, trace:\n%s\n", rec).Error()
				}
				return output.String()

			case "refresh-range-descriptor-cache":
				mustHaveArgOrFatal(t, d, tableName)
				mustHaveArgOrFatal(t, d, serverIdx)

				var tbName string
				d.ScanArgs(t, tableName, &tbName)
				var idx int
				d.ScanArgs(t, serverIdx, &idx)
				sqlDB, err := ds.getSQLConn(idx)
				if err != nil {
					return err.Error()
				}
				// Execute the query that's supposed to populate the range descriptor
				// cache.
				_, err = sqlDB.Exec(d.Input)
				if err != nil {
					return err.Error()
				}
				// Ensure the range descriptor cache was indeed populated.
				var tableID uint32
				err = sqlDB.QueryRow(`SELECT id from system.namespace WHERE name=$1`,
					tbName).Scan(&tableID)
				if err != nil {
					return err.Error()
				}
				cache := ds.tc.Server(idx).DistSenderI().(*kvcoord.DistSender).RangeDescriptorCache()
				tablePrefix := keys.MustAddr(keys.SystemSQLCodec.TablePrefix(tableID))
				entry, err := cache.TestingGetCached(ctx, tablePrefix, false /* inverted */)
				if err != nil {
					return err.Error()
				}
				return entry.ClosedTimestampPolicy.String()

			case "wait-for-zone-config-changes":
				lookupKey, err := getRangeKeyForInput(t, d, ds.tc)
				if err != nil {
					return err.Error()
				}
				lookupRKey := keys.MustAddr(lookupKey)

				// There's a lot going on here and things can fail at various steps, for
				// completely legitimate reasons, which is why this thing needs to be
				// wrapped in a succeeds soon.
				if err := testutils.SucceedsWithinError(func() error {
					desc, err := ds.tc.LookupRange(lookupKey)
					if err != nil {
						return err
					}

					// Parse user-supplied expected replica types from input.
					expectedPlacement := parseReplicasFromInput(t, ds.tc, d)
					// Get current replica type info from range and validate against
					// user-specified replica types
					actualPlacement, err := parseReplicasFromRange(t, ds.tc, desc)
					if err != nil {
						return err
					}

					leaseHolderNode := ds.tc.Server(actualPlacement.getLeaseholder())
					store, err := leaseHolderNode.GetStores().(*kvserver.Stores).GetStore(
						leaseHolderNode.GetFirstStoreID())
					if err != nil {
						return err
					}
					repl := store.LookupReplica(lookupRKey)
					if repl == nil {
						return errors.New(`could not find replica`)
					}
					for _, queueName := range []string{"split", "replicate", "raftsnapshot"} {
						_, processErr, err := store.Enqueue(
							ctx, queueName, repl, true /* skipShouldQueue */, false, /* async */
						)
						if processErr != nil {
							return processErr
						}
						if err != nil {
							return err
						}
					}

					// If the user specified a leaseholder, transfer range's lease to the
					// that node.
					if expectedPlacement.hasLeaseholderInfo() {
						expectedLeaseIdx := expectedPlacement.getLeaseholder()
						actualLeaseIdx := actualPlacement.getLeaseholder()
						if expectedLeaseIdx != actualLeaseIdx {
							leaseErr := errors.Newf("expected leaseholder %d but got %d",
								expectedLeaseIdx, actualLeaseIdx)

							// We want to only initiate a lease transfer if our target replica
							// is a voter. Otherwise, TransferRangeLease will silently fail.
							newLeaseholderType, found := actualPlacement.getReplicaType(expectedLeaseIdx)
							if !found {
								return errors.CombineErrors(
									leaseErr,
									errors.Newf(
										"expected node %d to have a replica; it doesn't have one yet",
										expectedLeaseIdx),
								)
							}
							if newLeaseholderType != replicaTypeVoter {
								return errors.CombineErrors(
									leaseErr,
									errors.Newf(
										"expected node %d to be a voter but was %s",
										expectedLeaseIdx,
										newLeaseholderType.String()))
							}

							log.VEventf(
								ctx,
								2,
								"transferring lease from node %d to %d", actualLeaseIdx, expectedLeaseIdx)
							err = ds.tc.TransferRangeLease(desc, ds.tc.Target(expectedLeaseIdx))
							return errors.CombineErrors(
								leaseErr,
								err,
							)
						}
					}
					// Now that this range has gone through a bunch of changes, we lookup
					// the range and its leaseholder again to ensure we're comparing the
					// most up-to-date range state with the supplied expectation.
					desc, err = ds.tc.LookupRange(lookupKey)
					if err != nil {
						return err
					}
					actualPlacement, err = parseReplicasFromRange(t, ds.tc, desc)
					if err != nil {
						return err
					}
					err = actualPlacement.satisfiesExpectedPlacement(expectedPlacement)
					if err != nil {
						return err
					}

					if d.HasArg(numVoters) {
						var voters int
						d.ScanArgs(t, numVoters, &voters)
						if len(desc.Replicas().VoterDescriptors()) != voters {
							return errors.Newf("unexpected number of voters %d, expected %d",
								len(desc.Replicas().VoterDescriptors()), voters)
						}
					}
					if d.HasArg(numNonVoters) {
						var nonVoters int
						d.ScanArgs(t, numNonVoters, &nonVoters)
						if len(desc.Replicas().NonVoterDescriptors()) != nonVoters {
							return errors.Newf("unexpected number of non-voters %d, expected %d",
								len(desc.Replicas().NonVoterDescriptors()), nonVoters)
						}
					}

					return nil
				}, 5*time.Minute); err != nil {
					return err.Error()
				}

			default:
				return errors.Newf("unknown command").Error()
			}
			return ""
		})
	})
}

// Constants corresponding to command-options accepted by the data-driven test.
const (
	serverIdx        = "idx"
	serverLocalities = "localities"
	tableName        = "table-name"
	dbName           = "db-name"
	partitionName    = "partition-name"
	numVoters        = "num-voters"
	numNonVoters     = "num-non-voters"
)

type replicaType int

const (
	replicaTypeLeaseholder replicaType = iota
	replicaTypeVoter
	replicaTypeNonVoter
	replicaTypeNotPresent
)

type datadrivenTestState struct {
	tc serverutils.TestClusterInterface
}

var replicaTypes = map[string]replicaType{
	"leaseholder": replicaTypeLeaseholder,
	"voter":       replicaTypeVoter,
	"non-voter":   replicaTypeNonVoter,
	"not-present": replicaTypeNotPresent,
}
var replicaTypeString = map[replicaType]string{
	replicaTypeLeaseholder: "leaseholder",
	replicaTypeVoter:       "voter",
	replicaTypeNonVoter:    "non-voter",
	replicaTypeNotPresent:  "not-present",
}

func (r replicaType) String() string {
	return replicaTypeString[r]
}

func (d *datadrivenTestState) cleanup(ctx context.Context) {
	if d.tc != nil {
		d.tc.Stopper().Stop(ctx)
	}
	d.tc = nil
}

func (d *datadrivenTestState) getSQLConn(idx int) (*gosql.DB, error) {
	if d.tc == nil {
		return nil, errors.New("no cluster exists")
	}
	if idx < 0 || idx >= d.tc.NumServers() {
		return nil, errors.Newf("invalid idx, must be in range [0, %d)", d.tc.NumServers())
	}
	return d.tc.ServerConn(idx), nil
}

func mustHaveArgOrFatal(t *testing.T, d *datadriven.TestData, arg string) {
	if !d.HasArg(arg) {
		t.Fatalf("no %q provided", arg)
	}
}

func nodeIdToIdx(t *testing.T, tc serverutils.TestClusterInterface, id roachpb.NodeID) int {
	for i := 0; i < tc.NumServers(); i++ {
		if tc.Server(i).NodeID() == id {
			return i
		}
	}

	t.Fatalf("could not find idx for node id %d", id)

	return -1
}

// checkReadServedLocallyInSimpleRecording looks at a "simple" trace and returns
// if the query for served locally and if it was served via a follower read.
// A "simple" trace is defined as one that contains a single "dist sender send"
// message. An error is returned if more than one (or no) "dist sender send"
// messages are found in the recording.
func checkReadServedLocallyInSimpleRecording(
	rec tracingpb.Recording,
) (servedLocally bool, servedUsingFollowerReads bool, err error) {
	foundDistSenderSend := false
	for _, sp := range rec {
		if sp.Operation == "dist sender send" {
			if foundDistSenderSend {
				return false,
					false,
					errors.New("recording contains > 1 dist sender send messages")
			}
			foundDistSenderSend = true
			servedLocally = tracing.LogsContainMsg(sp, kvbase.RoutingRequestLocallyMsg)
			// Check the child span to find out if the query was served using a
			// follower read.
			for _, span := range rec {
				if span.ParentSpanID == sp.SpanID {
					if tracing.LogsContainMsg(span, kvbase.FollowerReadServingMsg) {
						servedUsingFollowerReads = true
					}
				}
			}
		}
	}
	if !foundDistSenderSend {
		return false,
			false,
			errors.New("recording contains no dist sender send messages")
	}
	return servedLocally, servedUsingFollowerReads, nil
}

// replicaPlacement keeps track of which nodes have what replica types for a
// given range.
type replicaPlacement struct {
	nodeToReplicaType  map[int]replicaType
	replicaTypeToNodes map[replicaType][]int
	leaseholder        int
}

// parseReplicasFromInput constructs a replicaPlacement from user input.
func parseReplicasFromInput(
	t *testing.T, tc serverutils.TestClusterInterface, d *datadriven.TestData,
) *replicaPlacement {
	ret := replicaPlacement{}
	ret.leaseholder = -1

	userReplicas := make(map[replicaType][]int)
	for replicaTypeName := range replicaTypes {
		if !d.HasArg(replicaTypeName) {
			continue
		}

		var rawReplicas string
		d.ScanArgs(t, replicaTypeName, &rawReplicas)

		rawReplicaList := strings.Split(rawReplicas, ",")
		newReplicas := make([]int, 0, len(rawReplicaList))
		for _, rawIndex := range rawReplicaList {
			ind, err := strconv.Atoi(rawIndex)
			if err != nil {
				t.Fatalf("could not parse replica type for index: %v", err)
			}
			if ind >= tc.NumServers() || ind < 0 {
				t.Fatalf("got server index %d but have %d servers", ind, tc.NumServers())
			}
			newReplicas = append(newReplicas, ind)
		}

		userReplicas[replicaTypes[replicaTypeName]] = newReplicas
	}

	if leaseholders := userReplicas[replicaTypeLeaseholder]; len(leaseholders) == 1 {
		ret.leaseholder = leaseholders[0]
	} else if len(leaseholders) > 1 {
		t.Fatalf("got more than one leaseholder: %d", len(leaseholders))
	}

	ret.replicaTypeToNodes = userReplicas

	reversed := make(map[int]replicaType)
	for rt, replicas := range userReplicas {
		for _, replica := range replicas {
			reversed[replica] = rt
		}
	}
	ret.nodeToReplicaType = reversed

	return &ret
}

// parseReplicasFromInput constructs a replicaPlacement from a range descriptor.
// It also ensures all replicas have the same view of who the leaseholder is.
func parseReplicasFromRange(
	t *testing.T, tc serverutils.TestClusterInterface, desc roachpb.RangeDescriptor,
) (*replicaPlacement, error) {
	ret := replicaPlacement{}

	replicaMap := make(map[int]replicaType)

	leaseHolder, err := tc.FindRangeLeaseHolder(desc, nil)
	if err != nil {
		return nil, errors.Wrap(err, "could not get leaseholder")
	}
	// This test performs lease transfers at various points and expects the
	// range's leaseholder to conform to what was specified in the test. However,
	// whenever the lease is transferred using methods on TestCluster, only the
	// outgoing leaseholder is guaranteed to have applied the lease. Given the
	// tracing assumptions these tests make, it's worth ensuring all replicas have
	// applied the lease, as it makes reasoning about these tests much easier.
	// To that effect, we loop through all replicas on the supplied descriptor and
	// ensure that is indeed the case.
	for _, repl := range desc.Replicas().VoterAndNonVoterDescriptors() {
		t := tc.Target(nodeIdToIdx(t, tc, repl.NodeID))
		lh, err := tc.FindRangeLeaseHolder(desc, &t)
		if err != nil {
			return nil, err
		}
		if lh.NodeID != leaseHolder.NodeID && lh.StoreID != leaseHolder.StoreID {
			return nil, errors.Newf(
				"all replicas do not have the same view of the lease; found %s and %s",
				lh, leaseHolder,
			)
		}
	}

	leaseHolderIdx := nodeIdToIdx(t, tc, leaseHolder.NodeID)
	replicaMap[leaseHolderIdx] = replicaTypeLeaseholder
	ret.leaseholder = leaseHolderIdx

	for _, replica := range desc.Replicas().VoterDescriptors() {
		idx := nodeIdToIdx(t, tc, replica.NodeID)
		if _, found := replicaMap[idx]; !found {
			replicaMap[idx] = replicaTypeVoter
		}
	}

	for _, replica := range desc.Replicas().NonVoterDescriptors() {
		idx := nodeIdToIdx(t, tc, replica.NodeID)
		if rt, found := replicaMap[idx]; !found {
			replicaMap[idx] = replicaTypeNonVoter
		} else {
			return nil,
				errors.Errorf("expected not to find non-voter %d in replica map but found %s",
					idx, rt.String())
		}
	}

	ret.nodeToReplicaType = replicaMap

	reversed := make(map[replicaType][]int)
	for replica, rt := range replicaMap {
		if val, found := reversed[rt]; !found {
			reversed[rt] = []int{replica}
		} else {
			reversed[rt] = append(val, replica)
		}
	}
	ret.replicaTypeToNodes = reversed

	return &ret, nil
}

// satisfiesExpectedPlacement returns nil if the expected replicaPlacement is a
// subset of the provided replicaPlacement, error otherwise.
func (r *replicaPlacement) satisfiesExpectedPlacement(expected *replicaPlacement) error {
	for node, expectedRt := range expected.nodeToReplicaType {
		actualRt, found := r.nodeToReplicaType[node]
		if expectedRt == replicaTypeNotPresent {
			// We hope to not be able to find replicas marked as not present, so
			// continue to the next replica if that's the case.
			if !found || actualRt == replicaTypeNotPresent {
				continue
			} else {
				return errors.Newf(
					"expected node %s to not be present but had replica type %s",
					node,
					actualRt.String())
			}
		}

		if !found {
			return errors.Newf(
				"expected node %s to have replica type %s but was not found",
				node,
				expectedRt.String())
		}

		if expectedRt != actualRt {
			return errors.Newf(
				"expected node %s to have replica type %s but was %s",
				node,
				expectedRt.String(),
				actualRt.String())
		}
	}

	if expected.hasLeaseholderInfo() && expected.getLeaseholder() != r.getLeaseholder() {
		return errors.Newf(
			"expected %s to be the leaseholder, but %s was instead",
			expected.getLeaseholder(), r.getLeaseholder(),
		)
	}
	return nil
}

func (r *replicaPlacement) hasLeaseholderInfo() bool {
	return r.leaseholder != -1
}

func (r *replicaPlacement) getLeaseholder() int {
	return r.leaseholder
}

func (r *replicaPlacement) getReplicaType(nodeIdx int) (_ replicaType, found bool) {
	rType, found := r.nodeToReplicaType[nodeIdx]
	return rType, found
}

func getRangeKeyForInput(
	t *testing.T, d *datadriven.TestData, tc serverutils.TestClusterInterface,
) (roachpb.Key, error) {
	mustHaveArgOrFatal(t, d, dbName)
	mustHaveArgOrFatal(t, d, tableName)

	var tbName string
	d.ScanArgs(t, tableName, &tbName)
	var db string
	d.ScanArgs(t, dbName, &db)

	execCfg := tc.Server(0).ExecutorConfig().(sql.ExecutorConfig)

	tableDesc, err := lookupTable(&execCfg, db, tbName)
	if err != nil {
		return nil, err
	}

	if !d.HasArg(partitionName) {
		return tableDesc.TableSpan(keys.SystemSQLCodec).Key, nil
	}

	var partition string
	d.ScanArgs(t, partitionName, &partition)

	primaryInd := tableDesc.GetPrimaryIndex()

	part := primaryInd.GetPartitioning()
	if part == nil {
		return []byte{},
			errors.Newf("could not get partitioning for primary index on table %s", tbName)
	}

	var listVal []byte
	for _, val := range part.PartitioningDesc().List {
		if val.Name == partition && len(val.Values) > 0 {
			listVal = val.Values[0]
		}
	}
	if listVal == nil {
		return nil, errors.Newf(
			"could not find list tuple for partition %s on table %s",
			partition,
			tbName,
		)
	}

	_, keyPrefix, err := rowenc.DecodePartitionTuple(
		&tree.DatumAlloc{},
		keys.SystemSQLCodec,
		tableDesc,
		primaryInd,
		part,
		listVal,
		tree.Datums{},
	)
	if err != nil {
		return nil, err
	}

	return keyPrefix, nil
}

func lookupTable(ec *sql.ExecutorConfig, database, table string) (catalog.TableDescriptor, error) {
	tbName, err := parser.ParseQualifiedTableName(database + ".public." + table)
	if err != nil {
		return nil, err
	}

	var tableDesc catalog.TableDescriptor

	err = sql.DescsTxn(
		context.Background(),
		ec,
		func(ctx context.Context, txn isql.Txn, col *descs.Collection) error {
			_, desc, err := descs.PrefixAndTable(ctx, col.ByNameWithLeased(txn.KV()).MaybeGet(), tbName)
			if err != nil {
				return err
			}
			tableDesc = desc
			return nil
		})

	if err != nil {
		return nil, err
	}

	if tableDesc == nil {
		return nil, errors.Newf("could not find table %s", table)
	}

	return tableDesc, nil
}
