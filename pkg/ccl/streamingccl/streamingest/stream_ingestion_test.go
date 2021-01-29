// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package streamingest

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/kvccl/kvtenantccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
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
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestStreamIngestionJobWithRandomClient creates a stream ingestion job that is
// fed KVs from the random stream client. After receiving a certain number of
// resolved timestamp events the test cancels the job to tear down the flow, and
// rollback to the latest resolved frontier timestamp.
// The test scans the KV store to compare all MVCC KVs against the relevant
// streamed KV Events, thereby ensuring that we end up in a consistent state.
func TestStreamIngestionJobWithRandomClient(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRaceWithIssue(t, 60710)

	ctx := context.Background()
	defer jobs.TestingSetAdoptAndCancelIntervals(100*time.Millisecond, 100*time.Millisecond)

	cancelJobCh := make(chan struct{})
	threshold := 10
	mu := syncutil.Mutex{}
	cancelJobAfterCheckpoints := makeCheckpointEventCounter(&mu, threshold, func() {
		cancelJobCh <- struct{}{}
	})
	streamValidator := newStreamClientValidator()
	registerValidator := registerValidatorWithClient(streamValidator)
	knobs := base.TestingKnobs{
		DistSQL: &execinfra.TestingKnobs{StreamIngestionTestingKnobs: &sql.StreamIngestionTestingKnobs{
			Interceptors: []func(event streamingccl.Event, pa streamingccl.PartitionAddress){cancelJobAfterCheckpoints,
				registerValidator},
		},
		},
	}
	serverArgs := base.TestServerArgs{}
	serverArgs.Knobs = knobs

	var allowResponse chan struct{}
	params := base.TestClusterArgs{ServerArgs: serverArgs}
	params.ServerArgs.Knobs.Store = &kvserver.StoreTestingKnobs{
		TestingResponseFilter: jobutils.BulkOpResponseFilter(&allowResponse),
	}

	numNodes := 3
	tc := testcluster.StartTestCluster(t, numNodes, params)
	defer tc.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	conn := tc.Conns[0]

	tenantID := 10
	valueRange := 100
	kvsPerResolved := 200
	kvFrequency := 50 * time.Nanosecond
	numPartitions := 2
	dupProbability := 0.2
	streamAddr := makeTestStreamURI(valueRange, kvsPerResolved, numPartitions, tenantID, kvFrequency,
		dupProbability)

	// Start the ingestion stream and wait for at least one AddSSTable to ensure the job is running.
	allowResponse = make(chan struct{})
	errCh := make(chan error)
	defer close(errCh)
	_, err := conn.Exec(`SET CLUSTER SETTING bulkio.stream_ingestion.minimum_flush_interval= '0.0005ms'`)
	require.NoError(t, err)
	query := fmt.Sprintf(`RESTORE TENANT 10 FROM REPLICATION STREAM FROM '%s'`, streamAddr)
	go func() {
		_, err := conn.Exec(query)
		errCh <- err
	}()
	select {
	case allowResponse <- struct{}{}:
	case err := <-errCh:
		t.Fatalf("%s: query returned before expected: %s", err, query)
	}
	close(allowResponse)

	var streamJobID string
	testutils.SucceedsSoon(t, func() error {
		row := conn.QueryRow("SELECT id FROM system.jobs ORDER BY created DESC LIMIT 1")
		return row.Scan(&streamJobID)
	})

	// Wait for the job to signal that it is ready to be canceled.
	<-cancelJobCh
	close(cancelJobCh)

	// Canceling the job should shutdown the ingestion processors via a context
	// cancellation, and subsequently rollback data above our frontier
	// timestamp.
	// TODO(adityamaru): Change this to cutover once we have cutover logic in
	// place.
	_, err = conn.Exec(`CANCEL JOB $1`, streamJobID)
	require.NoError(t, err)
	// We expect the statement to fail.
	if err := <-errCh; err == nil {
		t.Fatal(err)
	}

	// Wait for the ingestion job to have been canceled.
	testutils.SucceedsSoon(t, func() error {
		var status string
		sqlDB.QueryRow(t, `SELECT status FROM system.jobs WHERE id = $1`, streamJobID).Scan(&status)
		if jobs.Status(status) != jobs.StatusCanceled {
			return errors.New("job not in canceled state")
		}
		return nil
	})

	progress := &jobspb.Progress{}
	var streamProgress []byte
	sqlDB.QueryRow(
		t, `SELECT progress FROM system.jobs WHERE id=$1`, streamJobID,
	).Scan(&streamProgress)

	if err := protoutil.Unmarshal(streamProgress, progress); err != nil {
		t.Fatal("cannot unmarshal job progress from system.jobs")
	}
	highWaterTimestamp := progress.GetHighWater()
	if highWaterTimestamp == nil {
		t.Fatal(errors.New("expected the highWaterTimestamp written to progress to be non-nil"))
	}
	ts := *highWaterTimestamp
	require.True(t, !ts.IsEmpty())

	// Check the validator for any failures.
	for _, err := range streamValidator.failures() {
		t.Fatal(err)
	}

	tenantPrefix := keys.MakeTenantPrefix(roachpb.MakeTenantID(uint64(tenantID)))
	maxIngestedTS := assertExactlyEqualKVs(t, tc, streamValidator, ts, tenantPrefix)

	//Sanity check that the max ts in the store is less than the ts stored in the
	//job progress.
	require.True(t, maxIngestedTS.LessEq(ts))
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
	tenantPrefix roachpb.Key,
) hlc.Timestamp {
	// Iterate over the store.
	store := tc.GetFirstStoreFromServer(t, 0)
	it := store.Engine().NewMVCCIterator(storage.MVCCKeyIterKind, storage.IterOptions{
		LowerBound: tenantPrefix,
		UpperBound: tenantPrefix.PrefixEnd(),
	})
	defer it.Close()
	var prevKey roachpb.Key
	var valueTimestampTuples []roachpb.KeyValue
	var err error
	var maxKVTimestampSeen hlc.Timestamp
	var matchingKVs int
	for it.SeekGE(storage.MVCCKey{}); ; it.Next() {
		if ok, err := it.Valid(); !ok {
			if err != nil {
				t.Fatal(err)
			}
			break
		}
		if maxKVTimestampSeen.Less(it.Key().Timestamp) {
			maxKVTimestampSeen = it.Key().Timestamp
		}
		newKey := (prevKey != nil && !it.Key().Key.Equal(prevKey)) || prevKey == nil
		prevKey = it.Key().Key

		if newKey {
			// All value ts should have been drained at this point, otherwise there is
			// a mismatch between the streamed and ingested data.
			require.Equal(t, 0, len(valueTimestampTuples))
			valueTimestampTuples, err = streamValidator.getValuesForKeyBelowTimestamp(
				string(it.Key().Key), frontierTimestamp)
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
		require.Equal(t, roachpb.KeyValue{
			Key: it.Key().Key,
			Value: roachpb.Value{
				RawBytes:  it.Value(),
				Timestamp: it.Key().Timestamp,
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
