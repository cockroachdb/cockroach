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
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestScanReverseScanTargetBytes(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Sanity checks for the TargetBytes scan option. We're not checking the specifics here, just
	// that the plumbing works. TargetBytes is tested in-depth via TestMVCCHistories.

	const (
		tbNeg  = -1     // hard limit, should return no kv pairs
		tbNone = 0      // no limit, i.e. should return all kv pairs
		tbOne  = 1      // one byte = return first key only
		tbMid  = 50     // between first and second key, don't return second if avoidExcess
		tbLots = 100000 // de facto ditto tbNone
	)
	testutils.RunTrueAndFalse(t, "reverse", func(t *testing.T, reverse bool) {
		testutils.RunTrueAndFalse(t, "avoidExcess", func(t *testing.T, avoidExcess bool) {
			testutils.RunTrueAndFalse(t, "allowEmpty", func(t *testing.T, allowEmpty bool) {
				testutils.RunTrueAndFalse(t, "requireNextBytes", func(t *testing.T, requireNextBytes bool) {
					for _, tb := range []int64{tbNeg, tbNone, tbOne, tbMid, tbLots} {
						t.Run(fmt.Sprintf("targetBytes=%d", tb), func(t *testing.T) {
							// allowEmpty takes precedence over avoidExcess at the RPC
							// level, since callers have no control over avoidExcess.
							expN := 2
							if tb == tbNeg {
								expN = 0
							} else if tb == tbOne {
								if allowEmpty {
									expN = 0
								} else {
									expN = 1
								}
							} else if tb == tbMid && (allowEmpty || avoidExcess) {
								expN = 1
							}
							for _, sf := range []roachpb.ScanFormat{roachpb.KEY_VALUES, roachpb.BATCH_RESPONSE} {
								t.Run(fmt.Sprintf("format=%s", sf), func(t *testing.T) {
									testScanReverseScanInner(t, tb, sf, reverse, avoidExcess, allowEmpty, expN)
								})
							}
						})
					}
				})
			})
		})
	})
}

func testScanReverseScanInner(
	t *testing.T,
	tb int64,
	sf roachpb.ScanFormat,
	reverse bool,
	avoidExcess bool,
	allowEmpty bool,
	expN int,
) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	k1, k2 := roachpb.Key("a"), roachpb.Key("b")
	ts := hlc.Timestamp{WallTime: 1}

	eng := storage.NewDefaultInMemForTesting()
	defer eng.Close()

	// Write to k1 and k2.
	for _, k := range []roachpb.Key{k1, k2} {
		err := storage.MVCCPut(ctx, eng, nil, k, ts, roachpb.MakeValueFromString("value-"+string(k)), nil)
		require.NoError(t, err)
	}

	var req roachpb.Request
	var resp roachpb.Response
	if !reverse {
		req = &roachpb.ScanRequest{ScanFormat: sf}
		resp = &roachpb.ScanResponse{}
	} else {
		req = &roachpb.ReverseScanRequest{ScanFormat: sf}
		resp = &roachpb.ReverseScanResponse{}
	}
	req.SetHeader(roachpb.RequestHeader{Key: k1, EndKey: roachpb.KeyMax})

	version := clusterversion.TestingBinaryVersion
	if !avoidExcess {
		version = clusterversion.ByKey(clusterversion.TargetBytesAvoidExcess - 1)
	}
	settings := cluster.MakeTestingClusterSettingsWithVersions(version, clusterversion.TestingBinaryMinSupportedVersion, true)

	cArgs := CommandArgs{
		Args: req,
		Header: roachpb.Header{
			Timestamp:   ts,
			TargetBytes: tb,
			AllowEmpty:  allowEmpty,
		},
		EvalCtx: (&MockEvalCtx{ClusterSettings: settings}).EvalContext(),
	}

	if !reverse {
		_, err := Scan(ctx, eng, cArgs, resp)
		require.NoError(t, err)
	} else {
		_, err := ReverseScan(ctx, eng, cArgs, resp)
		require.NoError(t, err)
	}

	require.EqualValues(t, expN, resp.Header().NumKeys)
	if allowEmpty && tb > 0 {
		require.LessOrEqual(t, resp.Header().NumBytes, tb)
	} else if tb >= 0 {
		require.NotZero(t, resp.Header().NumBytes)
	}

	expSpan := &roachpb.Span{Key: k1, EndKey: roachpb.KeyMax}
	switch expN {
	case 0:
		if tb >= 0 && reverse {
			expSpan.EndKey = append(k2, 0)
		}
	case 1:
		if reverse {
			expSpan.EndKey = append(k1, 0)
		} else {
			expSpan.Key = k2
		}
	default:
		expSpan = nil
	}

	require.Equal(t, expSpan, resp.Header().ResumeSpan)
	if expSpan != nil {
		require.NotZero(t, resp.Header().ResumeReason)
		if tb < 0 {
			require.Zero(t, resp.Header().ResumeNextBytes)
		} else {
			require.NotZero(t, resp.Header().ResumeNextBytes)
		}
	}

	var rows []roachpb.KeyValue
	if !reverse {
		rows = resp.(*roachpb.ScanResponse).Rows
	} else {
		rows = resp.(*roachpb.ReverseScanResponse).Rows
	}

	if rows != nil {
		require.Len(t, rows, expN)
	}
}
