// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package builtins_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils/fingerprintutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/storageutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

// TestFingerprint tests the `crdb_internal.fingerprint` builtin in the presence
// of rangekeys.
func TestFingerprint(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	var mu syncutil.Mutex
	var numExportResponses int
	var numSSTsInExportResponses int
	s, sqlDB, db := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Store: &kvserver.StoreTestingKnobs{
				TestingResponseFilter: func(ctx context.Context, ba *kvpb.BatchRequest, br *kvpb.BatchResponse) *kvpb.Error {
					mu.Lock()
					defer mu.Unlock()
					for i, ru := range br.Responses {
						if _, ok := ba.Requests[i].GetInner().(*kvpb.ExportRequest); ok {
							exportResponse := ru.GetInner().(*kvpb.ExportResponse)
							numExportResponses++
							numSSTsInExportResponses += len(exportResponse.Files)
						}
					}
					return nil
				},
			},
		},
	})
	defer s.Stopper().Stop(ctx)

	resetVars := func() {
		mu.Lock()
		defer mu.Unlock()
		numExportResponses = 0
		numSSTsInExportResponses = 0
	}

	returnPointAndRangeKeys := func(eng storage.Engine) ([]storage.MVCCKeyValue, []storage.MVCCRangeKey) {
		var rangeKeys []storage.MVCCRangeKey
		var pointKeys []storage.MVCCKeyValue
		for _, kvI := range storageutils.ScanKeySpan(t, eng, roachpb.Key("a"), roachpb.Key("z")) {
			switch kv := kvI.(type) {
			case storage.MVCCRangeKeyValue:
				rangeKeys = append(rangeKeys, kv.RangeKey)

			case storage.MVCCKeyValue:
				pointKeys = append(pointKeys, kv)

			default:
				t.Fatalf("unknown type %t", kvI)
			}
		}
		return pointKeys, rangeKeys
	}

	fingerprint := func(t *testing.T, startKey, endKey string, startTime, endTime hlc.Timestamp, allRevisions bool) int64 {
		aost := endTime.AsOfSystemTime()
		var fingerprint int64
		query := fmt.Sprintf(`SELECT * FROM crdb_internal.fingerprint(ARRAY[$1::BYTES, $2::BYTES],$3::DECIMAL, $4) AS OF SYSTEM TIME '%s'`, aost)
		require.NoError(t, sqlDB.QueryRow(query, roachpb.Key(startKey), roachpb.Key(endKey),
			startTime.AsOfSystemTime(), allRevisions).Scan(&fingerprint))
		return fingerprint
	}

	// Disable index recommendation so that >5 invocations of
	// `crdb_internal.fingerprint` does not result in an additional call for
	// generating index recommendations.
	_, err := sqlDB.Exec(`SET CLUSTER SETTING sql.metrics.statement_details.index_recommendation_collection.enabled = false`)
	require.NoError(t, err)

	t.Run("fingerprint-empty-store", func(t *testing.T) {
		fingerprint := fingerprint(t, "a", "z", hlc.Timestamp{WallTime: 0},
			hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}, true /* allRevisions */)
		require.Zero(t, fingerprint)
	})

	store, err := s.GetStores().(*kvserver.Stores).GetStore(s.GetFirstStoreID())
	require.NoError(t, err)
	eng := store.TODOEngine()

	// Insert some point keys.
	txn := db.NewTxn(ctx, "test-point-keys")
	pointKeysTS := hlc.Timestamp{WallTime: timeutil.Now().Round(time.Microsecond).UnixNano()}
	require.NoError(t, txn.SetFixedTimestamp(ctx, pointKeysTS))
	require.NoError(t, txn.Put(ctx, "a", "value"))
	require.NoError(t, txn.Put(ctx, "b", "value"))
	require.NoError(t, txn.Put(ctx, "c", "value"))
	require.NoError(t, txn.Put(ctx, "d", "value"))
	require.NoError(t, txn.Commit(ctx))

	// Run a scan to force intent resolution.
	_, err = db.Scan(ctx, "a", "z", 0)
	require.NoError(t, err)

	pointKeys, rangeKeys := returnPointAndRangeKeys(eng)
	require.Len(t, pointKeys, 4)
	require.Len(t, rangeKeys, 0)

	// The store will have:
	//
	// ts2 [----------- rt -------------)
	//
	// ts1	value		value		value		value
	//				a				b				c				d
	//
	// Fingerprint the point keys.
	fingerprintPointKeys := fingerprint(t, "a", "z",
		pointKeysTS.Add(int64(-time.Microsecond), 0), pointKeysTS, true /* allRevisions */)

	// The store will have:
	//
	// ts2 [------ rt --------)
	//
	// ts1	value		value		value		value
	//				a				b				c				d
	require.NoError(t, db.DelRangeUsingTombstone(ctx, "a", "c"))
	pointKeys, rangeKeys = returnPointAndRangeKeys(eng)
	require.Len(t, pointKeys, 4)
	require.Len(t, rangeKeys, 1)
	rangeKey1Timestamp := rangeKeys[0].Timestamp
	// Note, the timestamp comparison is a noop here but we need the timestamp for
	// future AOST fingerprint queries.
	require.Equal(t, []storage.MVCCRangeKey{
		storageutils.RangeKeyWithTS("a", "c", rangeKey1Timestamp),
	}, rangeKeys)

	// Fingerprint the point and range keys.
	fingerprintPointAndRangeKeys := fingerprint(t, "a", "z",
		pointKeysTS.Add(int64(-time.Microsecond), 0), rangeKey1Timestamp, true /* allRevisions */)
	require.NotEqual(t, int64(0), fingerprintPointAndRangeKeys)
	require.NotEqual(t, fingerprintPointKeys, fingerprintPointAndRangeKeys)

	// Fingerprint only the range key.
	fingerprintRangekeys := fingerprint(t, "a", "z",
		rangeKey1Timestamp.Add(int64(-time.Microsecond), 0), rangeKey1Timestamp, true /* allRevisions */)
	require.NotEqual(t, int64(0), fingerprintRangekeys)

	require.Equal(t, fingerprintPointAndRangeKeys, fingerprintPointKeys^fingerprintRangekeys)

	// The store now has:
	//
	// ts3 						 [------)[-------)
	//
	// ts2 [----------)[------)
	//
	// ts1	value		value		value		value
	//				a				b				c				d
	require.NoError(t, db.DelRangeUsingTombstone(ctx, "b", "d"))
	pointKeys, rangeKeys = returnPointAndRangeKeys(eng)
	require.Len(t, pointKeys, 4)
	require.Len(t, rangeKeys, 4)
	rangeKey2Timestamp := rangeKeys[1].Timestamp
	require.Equal(t, []storage.MVCCRangeKey{
		storageutils.RangeKeyWithTS("a", "b", rangeKey1Timestamp),
		storageutils.RangeKeyWithTS("b", "c", rangeKey2Timestamp),
		storageutils.RangeKeyWithTS("b", "c", rangeKey1Timestamp),
		storageutils.RangeKeyWithTS("c", "d", rangeKey2Timestamp),
	}, rangeKeys)

	// Even with the fragmentation of the first range key, our fingerprint for the
	// point keys and first range key should be the same as before.
	fingerprintFragmentedPointAndRangeKeys := fingerprint(t, "a", "z",
		pointKeysTS.Add(int64(-time.Microsecond), 0), rangeKey1Timestamp, true /* allRevisions */)
	require.Equal(t, fingerprintPointAndRangeKeys, fingerprintFragmentedPointAndRangeKeys)

	// Insert a split point so that we're returned 2 SSTs with rangekeys instead
	// of one. This should not affect the fingerprint.
	resetVars()
	fingerprintPreSplit := fingerprint(t, "a", "z", pointKeysTS.Add(int64(-time.Microsecond), 0),
		hlc.Timestamp{WallTime: timeutil.Now().Round(time.Microsecond).UnixNano()}, true /* allRevisions */)
	require.Equal(t, 1, numSSTsInExportResponses)
	require.Equal(t, 1, numExportResponses)

	require.NoError(t, db.AdminSplit(ctx, "c", hlc.MaxTimestamp))

	resetVars()
	fingerprintPostSplit := fingerprint(t, "a", "z", pointKeysTS.Add(int64(-time.Microsecond), 0),
		hlc.Timestamp{WallTime: timeutil.Now().Round(time.Microsecond).UnixNano()}, true /* allRevisions */)
	require.Equal(t, 2, numSSTsInExportResponses)
	require.Equal(t, 2, numExportResponses)

	require.Equal(t, fingerprintPreSplit, fingerprintPostSplit)
}

func TestFingerprintConcurrentTransactions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	db := sqlutils.MakeSQLRunner(sqlDB)
	db.Exec(t, "CREATE DATABASE IF NOT EXISTS test")
	db.Exec(t, "CREATE TABLE test.test (k PRIMARY KEY) AS SELECT generate_series(1, 10)")

	startTs := s.Clock().Now()

	fingerprintQuery := `
SELECT *
FROM
	crdb_internal.fingerprint(
		crdb_internal.table_span((SELECT id FROM system.namespace WHERE name = 'test' AND "parentID" != 0)),
		0::TIMESTAMPTZ,
		false
	)
`

	txn, err := sqlDB.Begin()
	require.NoError(t, err)

	var fingerprint1 int
	require.NoError(t, txn.QueryRow(fingerprintQuery).Scan(&fingerprint1))

	// Write a row in a different transaction.
	db.Exec(t, "INSERT INTO test.test (k) VALUES (42)")

	// Another fingerprint taken inside the transaction should
	// yield the same result.
	var fingerprint2 int
	require.NoError(t, txn.QueryRow(fingerprintQuery).Scan(&fingerprint2))
	require.Equal(t, fingerprint1, fingerprint2)

	require.NoError(t, txn.Commit())

	// AOST query back to the start should give the same as value
	// as those taken during the transaction that can't see the
	// new write.
	var fingerprint3 int
	aost := fmt.Sprintf("%s AS OF SYSTEM TIME %s", fingerprintQuery, startTs.AsOfSystemTime())
	db.QueryRow(t, aost).Scan(&fingerprint3)
	require.Equal(t, fingerprint1, fingerprint3)

	// Just to be sure we aren't fooling ourselves, the
	// fingerprint should be different after the new writes.
	var fingerprint4 int
	db.QueryRow(t, fingerprintQuery).Scan(&fingerprint4)
	require.NotEqual(t, fingerprint1, fingerprint4)
}

func TestFingerprintStripped(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	db := sqlutils.MakeSQLRunner(sqlDB)
	db.Exec(t, "CREATE DATABASE IF NOT EXISTS test")
	db.Exec(t, "CREATE TABLE test.test (k PRIMARY KEY) AS SELECT generate_series(1, 10)")

	// Create the same sql rows in a different table, committed at a different timestamp.
	db.Exec(t, "CREATE TABLE test.test2 (k PRIMARY KEY) AS SELECT generate_series(1, 10)")

	fingerprints, err := fingerprintutils.FingerprintDatabase(ctx, sqlDB, "test",
		fingerprintutils.Stripped())
	require.NoError(t, err)

	require.Equal(t, fingerprints["test"], fingerprints["test2"])
}
