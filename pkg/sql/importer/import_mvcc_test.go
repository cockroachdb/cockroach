// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package importer_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestMVCCValueHeaderImportEpoch tests that the import job ID is properly
// stored in the MVCCValueHeader in an imported key's MVCCValue.
func TestMVCCValueHeaderImportEpoch(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(db)

	sqlDB.Exec(t, `CREATE DATABASE d`)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			fmt.Fprint(w, "1")
		}
	}))
	defer srv.Close()

	// Create a table where the first row ( in sort order) comes from an IMPORT
	// while the second comes from an INSERT.
	sqlDB.Exec(t, `CREATE TABLE d.t (a INT8)`)
	sqlDB.Exec(t, `INSERT INTO d.t VALUES ('2')`)
	sqlDB.Exec(t, `IMPORT INTO d.t CSV DATA ($1)`, srv.URL)

	// Conduct an export request to iterate over the keys in the table.
	var tableID uint32
	sqlDB.QueryRow(t, `SELECT id FROM system.namespace WHERE name = $1`,
		"t").Scan(&tableID)

	startKey := keys.SystemSQLCodec.TablePrefix(tableID)
	endKey := startKey.PrefixEnd()

	req := &roachpb.ExportRequest{
		RequestHeader: roachpb.RequestHeader{
			Key:    startKey,
			EndKey: endKey,
		},
		MVCCFilter: roachpb.MVCCFilter_All,
		StartTime:  hlc.Timestamp{},
		ReturnSST:  true,
	}

	header := roachpb.Header{Timestamp: s.Clock().Now()}
	resp, roachErr := kv.SendWrappedWith(ctx,
		s.DistSenderI().(*kvcoord.DistSender), header, req)
	require.NoError(t, roachErr.GoError())

	iterOpts := storage.IterOptions{
		KeyTypes:   storage.IterKeyTypePointsOnly,
		LowerBound: startKey,
		UpperBound: endKey,
	}

	// Ensure there are 2 keys in the span, and only the first one contains job ID metadata
	keyCount := 0
	for _, file := range resp.(*roachpb.ExportResponse).Files {
		it, err := storage.NewPebbleMemSSTIterator(file.SST, false /* verify */, iterOpts)
		require.NoError(t, err)
		defer it.Close()
		for it.SeekGE(storage.NilKey); ; it.Next() {
			ok, err := it.Valid()
			require.NoError(t, err)
			if !ok {
				break
			}
			val, err := storage.DecodeMVCCValue(it.UnsafeValue())
			require.NoError(t, err)
			if keyCount == 0 {
				require.NotEqual(t, uint32(0), val.ImportEpoch)
			} else if keyCount == 1 {
				require.Equal(t, uint32(0), val.ImportEpoch)
			} else {
				t.Fatal("more than 2 keys in the table")
			}
			require.Equal(t, hlc.ClockTimestamp{}, val.LocalTimestamp)
			keyCount++
		}
	}
}
