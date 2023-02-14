// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package builtins_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
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
	serv, sqlDB, db := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Store: &kvserver.StoreTestingKnobs{
				TestingResponseFilter: func(ctx context.Context, ba *roachpb.BatchRequest, br *roachpb.BatchResponse) *roachpb.Error {
					mu.Lock()
					defer mu.Unlock()
					for i, ru := range br.Responses {
						if _, ok := ba.Requests[i].GetInner().(*roachpb.ExportRequest); ok {
							exportResponse := ru.GetInner().(*roachpb.ExportResponse)
							numExportResponses++
							numSSTsInExportResponses += len(exportResponse.Files)
						}
					}
					return nil
				},
			},
		},
	})

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
		decimal := eval.TimestampToDecimal(endTime)
		var fingerprint int64
		query := fmt.Sprintf(`SELECT * FROM crdb_internal.fingerprint(ARRAY[$1::BYTES, $2::BYTES], $3, $4) AS OF SYSTEM TIME %s`, decimal.String())
		require.NoError(t, sqlDB.QueryRow(query, roachpb.Key(startKey), roachpb.Key(endKey), startTime.GoTime(), allRevisions).Scan(&fingerprint))
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

	s := serv.(*server.TestServer)
	defer s.Stopper().Stop(ctx)

	store, err := s.Stores().GetStore(s.GetFirstStoreID())
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
