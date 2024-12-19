// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package physical

import (
	"context"
	gosql "database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/backup"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdctest"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/kvccl/kvtenantccl" // To start tenants.
	"github.com/cockroachdb/cockroach/pkg/crosscluster/replicationtestutils"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/replicationutils"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/streamclient"
	_ "github.com/cockroachdb/cockroach/pkg/crosscluster/streamclient/randclient"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func getReplicatedTime(ingestionJobID int, sqlDB *gosql.DB) (hlc.Timestamp, error) {
	var progressBytes []byte
	if err := sqlDB.QueryRow(
		`SELECT progress FROM crdb_internal.system_jobs WHERE id = $1`, ingestionJobID,
	).Scan(&progressBytes); err != nil {
		return hlc.Timestamp{}, err
	}
	var progress jobspb.Progress
	if err := protoutil.Unmarshal(progressBytes, &progress); err != nil {
		return hlc.Timestamp{}, err
	}
	return replicationutils.ReplicatedTimeFromProgress(&progress), nil
}

func getTestRandomClientURI(tenantID roachpb.TenantID, tenantName roachpb.TenantName) string {
	kvsPerResolved := 200
	kvFrequency := 50 * time.Nanosecond
	numPartitions := 2
	dupProbability := 0.2
	return makeTestStreamURI(kvsPerResolved, numPartitions, kvFrequency,
		dupProbability, tenantID, tenantName)
}

// streamClientValidatorWrapper wraps a Validator and exposes additional methods
// used by stream ingestion to check for correctness.
type streamClientValidator struct {
	cdctest.StreamValidator
	rekeyer *backup.KeyRewriter

	mu syncutil.Mutex
}

// newStreamClientValidator returns a wrapped Validator, that can be used
// to validate the events emitted by the cluster to cluster streaming client.
// The wrapper currently only "wraps" an orderValidator, but can be built out
// to utilize other Validator's.
// The wrapper also allows querying the orderValidator to retrieve streamed
// events from an in-memory store.
func newStreamClientValidator(rekeyer *backup.KeyRewriter) *streamClientValidator {
	ov := cdctest.NewStreamOrderValidator()
	return &streamClientValidator{
		StreamValidator: ov,
		rekeyer:         rekeyer,
	}
}

func (sv *streamClientValidator) noteRow(
	partition string, key, value string, updated hlc.Timestamp,
) error {
	sv.mu.Lock()
	defer sv.mu.Unlock()
	return sv.NoteRow(partition, key, value, updated, "")
}

func (sv *streamClientValidator) noteResolved(partition string, resolved hlc.Timestamp) error {
	sv.mu.Lock()
	defer sv.mu.Unlock()
	return sv.NoteResolved(partition, resolved)
}

func (sv *streamClientValidator) failures() []string {
	sv.mu.Lock()
	defer sv.mu.Unlock()
	return sv.Failures()
}

func (sv *streamClientValidator) getValuesForKeyBelowTimestamp(
	key string, timestamp hlc.Timestamp,
) ([]roachpb.KeyValue, error) {
	sv.mu.Lock()
	defer sv.mu.Unlock()
	return sv.StreamValidator.GetValuesForKeyBelowTimestamp(key, timestamp)
}

// TestStreamIngestionJobWithRandomClient creates a stream ingestion job that is
// fed KVs from the random stream client. After receiving a certain number of
// resolved timestamp events the test completes the job to tear down the flow,
// and rollback to the latest resolved frontier timestamp.
// The test scans the KV store to compare all MVCC KVs against the relevant
// streamed KV Events, thereby ensuring that we end up in a consistent state.
func TestStreamIngestionJobWithRandomClient(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRaceWithIssue(t, 60710)

	ctx := context.Background()

	canBeCompletedCh := make(chan struct{})
	const threshold = 10
	mu := syncutil.Mutex{}
	completeJobAfterCheckpoints := makeCheckpointEventCounter(&mu, threshold, func() {
		canBeCompletedCh <- struct{}{}
	})

	// Register interceptors on the random stream client, which will be used by
	// the processors.
	const oldTenantID = 10
	oldTenantName := roachpb.TenantName("10")
	// The destination tenant is going to be assigned ID 2.
	// TODO(ssd,knz): This is a hack, we should really retrieve the tenant ID
	// from beyond the point CREATE TENANT has run below.
	const newTenantID = 2
	rekeyer, err := backup.MakeKeyRewriterFromRekeys(keys.MakeSQLCodec(roachpb.MustMakeTenantID(oldTenantID)),
		nil /* tableRekeys */, []execinfrapb.TenantRekey{{
			OldID: roachpb.MustMakeTenantID(oldTenantID),
			NewID: roachpb.MustMakeTenantID(newTenantID),
		}}, true /* restoreTenantFromStream */)
	require.NoError(t, err)
	streamValidator := newStreamClientValidator(rekeyer)
	client := streamclient.GetRandomStreamClientSingletonForTesting()
	defer func() {
		require.NoError(t, client.Close(ctx))
	}()

	client.ClearInterceptors()
	client.RegisterInterception(completeJobAfterCheckpoints)
	client.RegisterInterception(validateFnWithValidator(t, streamValidator))
	client.RegisterSSTableGenerator(func(keyValues []roachpb.KeyValue) kvpb.RangeFeedSSTable {
		return replicationtestutils.SSTMaker(t, keyValues)
	})

	var receivedRevertRequest chan struct{}
	var allowResponse chan struct{}
	var revertRangeTargetTime hlc.Timestamp
	params := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			// Test hangs with test tenant. More investigation is required.
			// Tracked with #76378.
			DefaultTestTenant: base.TestControlsTenantsExplicitly,
			Knobs: base.TestingKnobs{
				TenantTestingKnobs: &sql.TenantTestingKnobs{
					// Needed to pin down the ID of the replication target.
					EnableTenantIDReuse: true,
				},
			},
		},
	}
	params.ServerArgs.Knobs.Store = &kvserver.StoreTestingKnobs{
		TestingRequestFilter: func(_ context.Context, ba *kvpb.BatchRequest) *kvpb.Error {
			for _, req := range ba.Requests {
				switch r := req.GetInner().(type) {
				case *kvpb.RevertRangeRequest:
					revertRangeTargetTime = r.TargetTime
					<-receivedRevertRequest
				}
			}
			return nil
		},
		TestingResponseFilter: jobutils.BulkOpResponseFilter(&allowResponse),
	}
	params.ServerArgs.Knobs.JobsTestingKnobs = jobs.NewTestingKnobsWithShortIntervals()
	params.ServerArgs.Knobs.Streaming = &sql.StreamingTestingKnobs{
		SkipSpanConfigReplication: true,
	}

	numNodes := 3
	tc := testcluster.StartTestCluster(t, numNodes, params)
	defer tc.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	conn := tc.Conns[0]

	allowResponse = make(chan struct{})
	receivedRevertRequest = make(chan struct{})
	_, err = conn.Exec(`SET CLUSTER SETTING bulkio.stream_ingestion.minimum_flush_interval= '0.0005ms'`)
	require.NoError(t, err)
	_, err = conn.Exec(`SET CLUSTER SETTING bulkio.stream_ingestion.failover_signal_poll_interval='1s'`)
	require.NoError(t, err)
	streamAddr := getTestRandomClientURI(roachpb.MustMakeTenantID(oldTenantID), oldTenantName)
	query := fmt.Sprintf(`CREATE TENANT "30" FROM REPLICATION OF "10" ON '%s'`, streamAddr)

	_, err = conn.Exec(query)
	require.NoError(t, err)

	// Check that the tenant ID that was created is the one we expect.
	var tenantID int
	err = conn.QueryRow(`SELECT id FROM system.tenants WHERE name = '30'`).Scan(&tenantID)
	require.NoError(t, err)
	require.Equal(t, newTenantID, tenantID)

	_, ingestionJobID := replicationtestutils.GetStreamJobIds(t, ctx, sqlDB, "30")

	// Start the ingestion stream and wait (for up to a minute) for at least one
	// AddSSTable to ensure the job is running.
	select {
	case allowResponse <- struct{}{}:
	case <-time.After(time.Minute):
		t.Fatal("timed out waiting for stream ingestion to send an sst")
	}
	close(allowResponse)

	// Wait for the job to signal that it is ready to be cutover, after it has
	// received `threshold` resolved ts events.
	<-canBeCompletedCh
	close(canBeCompletedCh)

	// Ensure that the job has made some progress.
	var replicatedTime hlc.Timestamp
	testutils.SucceedsSoon(t, func() error {
		var err error
		replicatedTime, err = getReplicatedTime(ingestionJobID, conn)
		require.NoError(t, err)
		if replicatedTime.IsEmpty() {
			return errors.New("ReplicatedTime is unset, no progress has been reported")
		}
		return nil
	})

	// Cutting over the job should shutdown the ingestion processors via a context
	// cancellation, and subsequently rollback data above our frontier timestamp.
	//
	// Pick a cutover time just before the latest resolved timestamp.
	cutoverTime := timeutil.Unix(0, replicatedTime.WallTime).UTC().Add(-1 * time.Microsecond).Round(time.Microsecond)
	_, err = conn.Exec(`ALTER TENANT "30" COMPLETE REPLICATION TO SYSTEM TIME $1::string`, cutoverTime)
	require.NoError(t, err)

	// Wait for the job to issue a revert request.
	receivedRevertRequest <- struct{}{}
	close(receivedRevertRequest)
	require.True(t, !revertRangeTargetTime.IsEmpty())
	require.Equal(t, revertRangeTargetTime, hlc.Timestamp{WallTime: cutoverTime.UnixNano()})

	// Wait for the ingestion job to have been marked as succeeded.
	jobutils.WaitForJobToSucceed(t, sqlDB, jobspb.JobID(ingestionJobID))

	// Check the validator for any failures.
	for _, err := range streamValidator.failures() {
		t.Fatal(err)
	}

	tenantSpan := keys.MakeTenantSpan(roachpb.MustMakeTenantID(uint64(newTenantID)))
	t.Logf("counting kvs in span %v", tenantSpan)
	maxIngestedTS := assertExactlyEqualKVs(t, tc, streamValidator, revertRangeTargetTime, tenantSpan)
	// Sanity check that the max ts in the store is less than the revert range
	// target timestamp.
	require.True(t, maxIngestedTS.LessEq(revertRangeTargetTime))
}

// assertExactlyEqualKVs runs an incremental iterator on the underlying store.
// At every key the method polls the `streamValidator` to return the KVEvents
// for that particular key, and have a timestamp less than equal to the
// `frontierTimestamp`. The key and value must be identical between the two.
func assertExactlyEqualKVs(
	t *testing.T,
	tc *testcluster.TestCluster,
	streamValidator *streamClientValidator,
	frontierTimestamp hlc.Timestamp,
	tenantSpan roachpb.Span,
) hlc.Timestamp {
	// Iterate over the store.
	store := tc.GetFirstStoreFromServer(t, 0)
	it, err := store.TODOEngine().NewMVCCIterator(context.Background(), storage.MVCCKeyIterKind, storage.IterOptions{
		LowerBound: tenantSpan.Key,
		UpperBound: tenantSpan.EndKey,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()
	var prevKey roachpb.Key
	var valueTimestampTuples []roachpb.KeyValue
	var maxKVTimestampSeen hlc.Timestamp
	var matchingKVs int
	for it.SeekGE(storage.MVCCKey{}); ; it.Next() {
		if ok, err := it.Valid(); !ok {
			if err != nil {
				t.Fatal(err)
			}
			break
		}
		if maxKVTimestampSeen.Less(it.UnsafeKey().Timestamp) {
			maxKVTimestampSeen = it.UnsafeKey().Timestamp
		}
		newKey := (prevKey != nil && !it.UnsafeKey().Key.Equal(prevKey)) || prevKey == nil
		prevKey = it.UnsafeKey().Clone().Key

		if newKey {
			// All value ts should have been drained at this point, otherwise there is
			// a mismatch between the streamed and ingested data.
			require.Equal(t, 0, len(valueTimestampTuples))
			valueTimestampTuples, err = streamValidator.getValuesForKeyBelowTimestamp(
				string(it.UnsafeKey().Key), frontierTimestamp)
			require.NoError(t, err)
		}

		// If there are no values stored in the validator to match against the
		// current key then we skip to the next key.
		if len(valueTimestampTuples) == 0 {
			continue
		}

		require.Greater(t, len(valueTimestampTuples), 0)
		// Since the iterator goes from latest to older versions, we compare
		// starting from the end of the slice that is sorted by timestamp.
		latestVersionInChain := valueTimestampTuples[len(valueTimestampTuples)-1]
		v, err := it.Value()
		require.NoError(t, err)
		require.Equal(t, roachpb.KeyValue{
			Key: it.UnsafeKey().Key,
			Value: roachpb.Value{
				RawBytes:  v,
				Timestamp: it.UnsafeKey().Timestamp,
			},
		}, latestVersionInChain)
		matchingKVs++
		// Truncate the latest version which we just checked against in preparation
		// for the next iteration.
		valueTimestampTuples = valueTimestampTuples[0 : len(valueTimestampTuples)-1]
	}
	// Sanity check that we have compared a non-zero number of KVs.
	require.Greater(t, matchingKVs, 0)
	return maxKVTimestampSeen
}
