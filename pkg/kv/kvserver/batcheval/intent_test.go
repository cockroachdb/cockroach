// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package batcheval

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// instrumentedEngine wraps a storage.Engine and allows for various methods in
// the interface to be instrumented for testing purposes.
type instrumentedEngine struct {
	storage.Engine

	onNewIterator func(storage.IterOptions)
	// ... can be extended ...
}

func (ie *instrumentedEngine) NewIterator(opts storage.IterOptions) storage.Iterator {
	if ie.onNewIterator != nil {
		ie.onNewIterator(opts)
	}
	return ie.Engine.NewIterator(opts)
}

// TestCollectIntentsUsesSameIterator tests that all uses of CollectIntents
// (currently only by READ_UNCOMMITTED Gets, Scans, and ReverseScans) use the
// same cached iterator (prefix or non-prefix) for their initial read and their
// provisional value collection for any intents they find.
func TestCollectIntentsUsesSameIterator(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	key := roachpb.Key("key")
	ts := hlc.Timestamp{WallTime: 123}
	header := roachpb.Header{
		Timestamp:       ts,
		ReadConsistency: roachpb.READ_UNCOMMITTED,
	}

	testCases := []struct {
		name              string
		run               func(*testing.T, storage.ReadWriter) (intent *roachpb.Value, _ error)
		expPrefixIters    int
		expNonPrefixIters int
	}{
		{
			name: "get",
			run: func(t *testing.T, db storage.ReadWriter) (*roachpb.Value, error) {
				req := &roachpb.GetRequest{
					RequestHeader: roachpb.RequestHeader{Key: key},
				}
				var resp roachpb.GetResponse
				if _, err := Get(ctx, db, CommandArgs{Args: req, Header: header}, &resp); err != nil {
					return nil, err
				}
				return resp.IntentValue, nil
			},
			expPrefixIters:    2,
			expNonPrefixIters: 0,
		},
		{
			name: "scan",
			run: func(t *testing.T, db storage.ReadWriter) (*roachpb.Value, error) {
				req := &roachpb.ScanRequest{
					RequestHeader: roachpb.RequestHeader{Key: key, EndKey: key.Next()},
				}
				var resp roachpb.ScanResponse
				if _, err := Scan(ctx, db, CommandArgs{Args: req, Header: header}, &resp); err != nil {
					return nil, err
				}
				if len(resp.IntentRows) != 1 {
					return nil, nil
				}
				return &resp.IntentRows[0].Value, nil
			},
			expPrefixIters:    0,
			expNonPrefixIters: 2,
		},
		{
			name: "reverse scan",
			run: func(t *testing.T, db storage.ReadWriter) (*roachpb.Value, error) {
				req := &roachpb.ReverseScanRequest{
					RequestHeader: roachpb.RequestHeader{Key: key, EndKey: key.Next()},
				}
				var resp roachpb.ReverseScanResponse
				if _, err := ReverseScan(ctx, db, CommandArgs{Args: req, Header: header}, &resp); err != nil {
					return nil, err
				}
				if len(resp.IntentRows) != 1 {
					return nil, nil
				}
				return &resp.IntentRows[0].Value, nil
			},
			expPrefixIters:    0,
			expNonPrefixIters: 2,
		},
	}
	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			db := &instrumentedEngine{Engine: storage.NewDefaultInMem()}
			defer db.Close()

			// Write an intent.
			val := roachpb.MakeValueFromBytes([]byte("val"))
			txn := roachpb.MakeTransaction("test", key, roachpb.NormalUserPriority, ts, 0)
			err := storage.MVCCPut(ctx, db, nil, key, ts, val, &txn)
			require.NoError(t, err)

			// Instrument iterator creation, count prefix vs. non-prefix iters.
			var prefixIters, nonPrefixIters int
			db.onNewIterator = func(opts storage.IterOptions) {
				if opts.Prefix {
					prefixIters++
				} else {
					nonPrefixIters++
				}
			}

			intentVal, err := c.run(t, db)
			require.NoError(t, err)

			// Assert proper intent value.
			expIntentVal := val
			expIntentVal.Timestamp = ts
			require.NotNil(t, intentVal)
			require.Equal(t, expIntentVal, *intentVal)

			// Assert proper iterator use.
			require.Equal(t, c.expPrefixIters, prefixIters)
			require.Equal(t, c.expNonPrefixIters, nonPrefixIters)
		})
	}
}
