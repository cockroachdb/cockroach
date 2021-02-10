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
	gosql "database/sql"
	"fmt"
	"strconv"
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

func getHighWaterMark(jobID int, sqlDB *gosql.DB) (*hlc.Timestamp, error) {
	var progressBytes []byte
	if err := sqlDB.QueryRow(
		`SELECT progress FROM system.jobs WHERE id = $1`, jobID,
	).Scan(&progressBytes); err != nil {
		return nil, err
	}
	var payload jobspb.Progress
	if err := protoutil.Unmarshal(progressBytes, &payload); err != nil {
		return nil, err
	}
	return payload.GetHighWater(), nil
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

	skip.WithIssue(t, 60789)
	skip.UnderRaceWithIssue(t, 60710)

	ctx := context.Background()
	defer jobs.TestingSetAdoptAndCancelIntervals(100*time.Millisecond, 100*time.Millisecond)

	canBeCompletedCh := make(chan struct{})
	threshold := 10
	mu := syncutil.Mutex{}
	completeJobAfterCheckpoints := makeCheckpointEventCounter(&mu, threshold, func() {
		canBeCompletedCh <- struct{}{}
	})
	streamValidator := newStreamClientValidator()
	registerValidator := registerValidatorWithClient(streamValidator)
	knobs := base.TestingKnobs{
		DistSQL: &execinfra.TestingKnobs{StreamIngestionTestingKnobs: &sql.StreamIngestionTestingKnobs{
			Interceptors: []func(event streamingccl.Event, pa streamingccl.PartitionAddress){completeJobAfterCheckpoints,
				registerValidator},
		},
		},
	}
	serverArgs := base.TestServerArgs{}
	serverArgs.Knobs = knobs

	var receivedRevertRequest chan struct{}
	var allowResponse chan struct{}
	var revertRangeTargetTime hlc.Timestamp
	params := base.TestClusterArgs{ServerArgs: serverArgs}
	params.ServerArgs.Knobs.Store = &kvserver.StoreTestingKnobs{
		TestingRequestFilter: func(_ context.Context, ba roachpb.BatchRequest) *roachpb.Error {
			for _, req := range ba.Requests {
				switch r := req.GetInner().(type) {
				case *roachpb.RevertRangeRequest:
					revertRangeTargetTime = r.TargetTime
					receivedRevertRequest <- struct{}{}
				}
			}
			return nil
		},
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
	receivedRevertRequest = make(chan struct{})
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

	var jobID string
	var streamJobID int
	testutils.SucceedsSoon(t, func() error {
		row := conn.QueryRow("SELECT id FROM system.jobs ORDER BY created DESC LIMIT 1")
		return row.Scan(&jobID)
	})
	streamJobID, err = strconv.Atoi(jobID)
	require.NoError(t, err)

	// Wait for the job to signal that it is ready to be cutover.
	<-canBeCompletedCh
	close(canBeCompletedCh)

	// Ensure that the job has made some progress.
	var highwater hlc.Timestamp
	testutils.SucceedsSoon(t, func() error {
		hw, err := getHighWaterMark(streamJobID, conn)
		require.NoError(t, err)
		if hw == nil {
			return errors.New("highwatermark is unset, no progress has been reported")
		}
		highwater = *hw
		return nil
	})

	// Cutting over the job should shutdown the ingestion processors via a context
	// cancellation, and subsequently rollback data above our frontier timestamp.
	tz := time.Unix(0, highwater.WallTime).UTC()
	_, err = conn.Exec(`SELECT crdb_internal.complete_stream_ingestion_job ($1, $2)`,
		streamJobID, tz)
	require.NoError(t, err)

	// Wait for the job to issue a revert request.
	<-receivedRevertRequest
	close(receivedRevertRequest)
	require.True(t, !revertRangeTargetTime.IsEmpty())
	require.Equal(t, revertRangeTargetTime, highwater)

	// Expect the job to complete without an error.
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}

	// Wait for the ingestion job to have been marked as succeeded.
	testutils.SucceedsSoon(t, func() error {
		var status string
		sqlDB.QueryRow(t, `SELECT status FROM system.jobs WHERE id = $1`, streamJobID).Scan(&status)
		if jobs.Status(status) != jobs.StatusSucceeded {
			return errors.New("job not in succeeded state")
		}
		return nil
	})

	// Check the validator for any failures.
	for _, err := range streamValidator.failures() {
		t.Fatal(err)
	}

	tenantPrefix := keys.MakeTenantPrefix(roachpb.MakeTenantID(uint64(tenantID)))
	maxIngestedTS := assertExactlyEqualKVs(t, tc, streamValidator, revertRangeTargetTime, tenantPrefix)

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
