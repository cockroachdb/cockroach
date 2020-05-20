// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/abortspan"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestEvaluateBatch(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ts := hlc.Timestamp{WallTime: 1}
	txn := roachpb.MakeTransaction("test", roachpb.Key("a"), 0, ts, 0)

	tcs := []testCase{
		//
		// Test suite for MaxRequestSpans.
		//
		{
			// We should never evaluate empty batches, but here's what would happen
			// if we did.
			name:  "all empty",
			setup: func(t *testing.T, d *data) {},
			check: func(t *testing.T, r resp) {
				require.Nil(t, r.pErr)
				require.NotNil(t, r.br)
				require.Empty(t, r.br.Responses)
			},
		}, {
			// Scanning without limit should return everything.
			name: "scan without MaxSpanRequestKeys",
			setup: func(t *testing.T, d *data) {
				writeABCDEF(t, d)
				req := scanArgsString("a", "z")
				d.ba.Add(req)
			},
			check: func(t *testing.T, r resp) {
				verifyScanResult(t, r, []string{"a", "b", "c", "d", "e", "f"})
				verifyResumeSpans(t, r, "")
			},
		}, {
			// Ditto in reverse.
			name: "reverse scan without MaxSpanRequestKeys",
			setup: func(t *testing.T, d *data) {
				writeABCDEF(t, d)
				req := revScanArgsString("a", "z")
				d.ba.Add(req)
			},
			check: func(t *testing.T, r resp) {
				verifyScanResult(t, r, []string{"f", "e", "d", "c", "b", "a"})
				verifyResumeSpans(t, r, "")
			},
		}, {
			// Scanning with "giant" limit should return everything.
			name: "scan with giant MaxSpanRequestKeys",
			setup: func(t *testing.T, d *data) {
				writeABCDEF(t, d)
				req := scanArgsString("a", "z")
				d.ba.Add(req)
				d.ba.MaxSpanRequestKeys = 100000
			},
			check: func(t *testing.T, r resp) {
				verifyScanResult(t, r, []string{"a", "b", "c", "d", "e", "f"})
				verifyResumeSpans(t, r, "")
			},
		}, {
			// Ditto in reverse.
			name: "reverse scan with giant MaxSpanRequestKeys",
			setup: func(t *testing.T, d *data) {
				writeABCDEF(t, d)
				req := revScanArgsString("a", "z")
				d.ba.Add(req)
				d.ba.MaxSpanRequestKeys = 100000
			},
			check: func(t *testing.T, r resp) {
				verifyScanResult(t, r, []string{"f", "e", "d", "c", "b", "a"})
				verifyResumeSpans(t, r, "")
			},
		}, {
			// Similar to above, just two scans.
			name: "scans with giant MaxSpanRequestKeys",
			setup: func(t *testing.T, d *data) {
				writeABCDEF(t, d)
				d.ba.Add(scanArgsString("a", "c"))
				d.ba.Add(scanArgsString("d", "g"))
				d.ba.MaxSpanRequestKeys = 100000
			},
			check: func(t *testing.T, r resp) {
				verifyScanResult(t, r, []string{"a", "b"}, []string{"d", "e", "f"})
				verifyResumeSpans(t, r, "", "")
			},
		}, {
			// Ditto in reverse.
			name: "reverse scans with giant MaxSpanRequestKeys",
			setup: func(t *testing.T, d *data) {
				writeABCDEF(t, d)
				d.ba.Add(revScanArgsString("d", "g"))
				d.ba.Add(revScanArgsString("a", "c"))
				d.ba.MaxSpanRequestKeys = 100000
			},
			check: func(t *testing.T, r resp) {
				verifyScanResult(t, r, []string{"f", "e", "d"}, []string{"b", "a"})
				verifyResumeSpans(t, r, "", "")
			},
		}, {
			// A batch limited to return only one key. Throw in a Get which is
			// not subject to limitation and should thus have returned a value.
			// However, the second scan comes up empty because there's no quota left.
			//
			// Note that there is currently a lot of undesirable behavior in the KV
			// API for pretty much any batch that's not a nonoverlapping sorted run
			// of only scans or only reverse scans. For example, in the example
			// below, one would get a response for get(f) even though the resume
			// span on the first scan is `[c,...)`. The higher layers of KV don't
			// handle that correctly. Right now we just trust that nobody will
			// send such requests.
			name: "scans with MaxSpanRequestKeys=1",
			setup: func(t *testing.T, d *data) {
				writeABCDEF(t, d)
				d.ba.Add(scanArgsString("a", "c"))
				d.ba.Add(getArgsString("f"))
				d.ba.Add(scanArgsString("d", "f"))
				d.ba.MaxSpanRequestKeys = 1
			},
			check: func(t *testing.T, r resp) {
				verifyScanResult(t, r, []string{"a"}, []string{"f"}, nil)
				verifyResumeSpans(t, r, "b-c", "", "d-f")
				b, err := r.br.Responses[1].GetGet().Value.GetBytes()
				require.NoError(t, err)
				require.Equal(t, "value-f", string(b))
			},
		}, {
			// Ditto in reverse.
			name: "reverse scans with MaxSpanRequestKeys=1",
			setup: func(t *testing.T, d *data) {
				writeABCDEF(t, d)
				d.ba.Add(revScanArgsString("d", "f"))
				d.ba.Add(getArgsString("f"))
				d.ba.Add(revScanArgsString("a", "c"))
				d.ba.MaxSpanRequestKeys = 1
			},
			check: func(t *testing.T, r resp) {
				verifyScanResult(t, r, []string{"e"}, []string{"f"}, nil)
				verifyResumeSpans(t, r, "d-d\x00", "", "a-c")
				b, err := r.br.Responses[1].GetGet().Value.GetBytes()
				require.NoError(t, err)
				require.Equal(t, "value-f", string(b))
			},
		}, {
			// Similar, but this time the request allows the second scan to
			// return one (but not more) remaining key. Again there's a Get
			// that isn't counted against the limit.
			name: "scans with MaxSpanRequestKeys=3",
			setup: func(t *testing.T, d *data) {
				writeABCDEF(t, d)
				d.ba.Add(scanArgsString("a", "c"))
				d.ba.Add(getArgsString("e"))
				d.ba.Add(scanArgsString("c", "e"))
				d.ba.MaxSpanRequestKeys = 3
			},
			check: func(t *testing.T, r resp) {
				verifyScanResult(t, r, []string{"a", "b"}, []string{"e"}, []string{"c"})
				verifyResumeSpans(t, r, "", "", "d-e")
				b, err := r.br.Responses[1].GetGet().Value.GetBytes()
				require.NoError(t, err)
				require.Equal(t, "value-e", string(b))
			},
		}, {
			// Ditto in reverse.
			name: "reverse scans with MaxSpanRequestKeys=3",
			setup: func(t *testing.T, d *data) {
				writeABCDEF(t, d)
				d.ba.Add(revScanArgsString("c", "e"))
				d.ba.Add(getArgsString("e"))
				d.ba.Add(revScanArgsString("a", "c"))
				d.ba.MaxSpanRequestKeys = 3
			},
			check: func(t *testing.T, r resp) {
				verifyScanResult(t, r, []string{"d", "c"}, []string{"e"}, []string{"b"})
				verifyResumeSpans(t, r, "", "", "a-a\x00")
				b, err := r.br.Responses[1].GetGet().Value.GetBytes()
				require.NoError(t, err)
				require.Equal(t, "value-e", string(b))
			},
		},
		//
		// Test suite for TargetBytes.
		//
		{
			// Two scans and a target bytes limit that saturates during the
			// first.
			name: "scans with TargetBytes=1",
			setup: func(t *testing.T, d *data) {
				writeABCDEF(t, d)
				d.ba.Add(scanArgsString("a", "c"))
				d.ba.Add(getArgsString("e"))
				d.ba.Add(scanArgsString("c", "e"))
				d.ba.TargetBytes = 1
				// Also set a nontrivial MaxSpanRequestKeys, just to make sure
				// there's no weird interaction (like it overriding TargetBytes).
				// The stricter one ought to win.
				d.ba.MaxSpanRequestKeys = 3
			},
			check: func(t *testing.T, r resp) {
				verifyScanResult(t, r, []string{"a"}, []string{"e"}, nil)
				verifyResumeSpans(t, r, "b-c", "", "c-e")
				b, err := r.br.Responses[1].GetGet().Value.GetBytes()
				require.NoError(t, err)
				require.Equal(t, "value-e", string(b))
			},
		}, {
			// Ditto in reverse.
			name: "reverse scans with TargetBytes=1",
			setup: func(t *testing.T, d *data) {
				writeABCDEF(t, d)
				d.ba.Add(revScanArgsString("c", "e"))
				d.ba.Add(getArgsString("e"))
				d.ba.Add(revScanArgsString("a", "c"))
				d.ba.TargetBytes = 1
				d.ba.MaxSpanRequestKeys = 3
			},
			check: func(t *testing.T, r resp) {
				verifyScanResult(t, r, []string{"d"}, []string{"e"}, nil)
				verifyResumeSpans(t, r, "c-c\x00", "", "a-c")
				b, err := r.br.Responses[1].GetGet().Value.GetBytes()
				require.NoError(t, err)
				require.Equal(t, "value-e", string(b))
			},
		},
		//
		// Test suite for KeyLocking.
		//
		{
			// Three scans that observe 3, 1, and 0 keys, respectively. An
			// unreplicated lock should be acquired on each key that is scanned.
			name: "scans with key locking",
			setup: func(t *testing.T, d *data) {
				writeABCDEF(t, d)
				scanAD := scanArgsString("a", "d")
				scanAD.KeyLocking = lock.Exclusive
				d.ba.Add(scanAD)
				scanEF := scanArgsString("e", "f")
				scanEF.KeyLocking = lock.Exclusive
				d.ba.Add(scanEF)
				scanHJ := scanArgsString("h", "j")
				scanHJ.KeyLocking = lock.Exclusive
				d.ba.Add(scanHJ)
				d.ba.Txn = &txn
			},
			check: func(t *testing.T, r resp) {
				verifyScanResult(t, r, []string{"a", "b", "c"}, []string{"e"}, nil)
				verifyAcquiredLocks(t, r, lock.Unreplicated, "a", "b", "c", "e")
				verifyAcquiredLocks(t, r, lock.Replicated, []string(nil)...)
			},
		},
		{
			// Ditto in reverse.
			name: "reverse scans with key locking",
			setup: func(t *testing.T, d *data) {
				writeABCDEF(t, d)
				scanAD := revScanArgsString("a", "d")
				scanAD.KeyLocking = lock.Exclusive
				d.ba.Add(scanAD)
				scanEF := revScanArgsString("e", "f")
				scanEF.KeyLocking = lock.Exclusive
				d.ba.Add(scanEF)
				scanHJ := revScanArgsString("h", "j")
				scanHJ.KeyLocking = lock.Exclusive
				d.ba.Add(scanHJ)
				d.ba.Txn = &txn
			},
			check: func(t *testing.T, r resp) {
				verifyScanResult(t, r, []string{"c", "b", "a"}, []string{"e"}, nil)
				verifyAcquiredLocks(t, r, lock.Unreplicated, "c", "b", "a", "e")
				verifyAcquiredLocks(t, r, lock.Replicated, []string(nil)...)
			},
		},
		{
			// Three scans that observe 3, 1, and 0 keys, respectively. No
			// transaction set, so no locks should be acquired.
			name: "scans with key locking without txn",
			setup: func(t *testing.T, d *data) {
				writeABCDEF(t, d)
				scanAD := scanArgsString("a", "d")
				scanAD.KeyLocking = lock.Exclusive
				d.ba.Add(scanAD)
				scanEF := scanArgsString("e", "f")
				scanEF.KeyLocking = lock.Exclusive
				d.ba.Add(scanEF)
				scanHJ := scanArgsString("h", "j")
				scanHJ.KeyLocking = lock.Exclusive
				d.ba.Add(scanHJ)
			},
			check: func(t *testing.T, r resp) {
				verifyScanResult(t, r, []string{"a", "b", "c"}, []string{"e"}, nil)
				verifyAcquiredLocks(t, r, lock.Unreplicated, []string(nil)...)
				verifyAcquiredLocks(t, r, lock.Replicated, []string(nil)...)
			},
		},
		{
			// Ditto in reverse.
			name: "reverse scans with key locking without txn",
			setup: func(t *testing.T, d *data) {
				writeABCDEF(t, d)
				scanAD := revScanArgsString("a", "d")
				scanAD.KeyLocking = lock.Exclusive
				d.ba.Add(scanAD)
				scanEF := revScanArgsString("e", "f")
				scanEF.KeyLocking = lock.Exclusive
				d.ba.Add(scanEF)
				scanHJ := revScanArgsString("h", "j")
				scanHJ.KeyLocking = lock.Exclusive
				d.ba.Add(scanHJ)
			},
			check: func(t *testing.T, r resp) {
				verifyScanResult(t, r, []string{"c", "b", "a"}, []string{"e"}, nil)
				verifyAcquiredLocks(t, r, lock.Unreplicated, []string(nil)...)
				verifyAcquiredLocks(t, r, lock.Replicated, []string(nil)...)
			},
		},
		{

			// Scanning with key locking and a MaxSpanRequestKeys limit should
			// acquire an unreplicated lock on each key returned and no locks on
			// keys past the limit.
			name: "scan with key locking and MaxSpanRequestKeys=3",
			setup: func(t *testing.T, d *data) {
				writeABCDEF(t, d)
				scanAE := scanArgsString("a", "e")
				scanAE.KeyLocking = lock.Exclusive
				d.ba.Add(scanAE)
				d.ba.Txn = &txn
				d.ba.MaxSpanRequestKeys = 3
			},
			check: func(t *testing.T, r resp) {
				verifyScanResult(t, r, []string{"a", "b", "c"})
				verifyResumeSpans(t, r, "d-e")
				verifyAcquiredLocks(t, r, lock.Unreplicated, "a", "b", "c")
				verifyAcquiredLocks(t, r, lock.Replicated, []string(nil)...)
			},
		},
		{
			// Ditto in reverse.
			name: "reverse scan with key locking and MaxSpanRequestKeys=3",
			setup: func(t *testing.T, d *data) {
				writeABCDEF(t, d)
				scanAE := revScanArgsString("a", "e")
				scanAE.KeyLocking = lock.Exclusive
				d.ba.Add(scanAE)
				d.ba.Txn = &txn
				d.ba.MaxSpanRequestKeys = 3
			},
			check: func(t *testing.T, r resp) {
				verifyScanResult(t, r, []string{"d", "c", "b"})
				verifyResumeSpans(t, r, "a-a\x00")
				verifyAcquiredLocks(t, r, lock.Unreplicated, "d", "c", "b")
				verifyAcquiredLocks(t, r, lock.Replicated, []string(nil)...)
			},
		},
		{

			// Scanning with key locking and a MaxSpanRequestKeys limit should
			// acquire an unreplicated lock on each key returned and no locks on
			// keys past the limit. One the batch's limit is exhausted, no more
			// rows are scanner nor locks acquired.
			name: "scans with key locking and MaxSpanRequestKeys=3",
			setup: func(t *testing.T, d *data) {
				writeABCDEF(t, d)
				scanAE := scanArgsString("a", "e")
				scanAE.KeyLocking = lock.Exclusive
				d.ba.Add(scanAE)
				scanHJ := scanArgsString("h", "j")
				scanHJ.KeyLocking = lock.Exclusive
				d.ba.Add(scanHJ)
				d.ba.Txn = &txn
				d.ba.MaxSpanRequestKeys = 3
			},
			check: func(t *testing.T, r resp) {
				verifyScanResult(t, r, []string{"a", "b", "c"}, nil)
				verifyResumeSpans(t, r, "d-e", "h-j")
				verifyAcquiredLocks(t, r, lock.Unreplicated, "a", "b", "c")
				verifyAcquiredLocks(t, r, lock.Replicated, []string(nil)...)
			},
		},
		{
			// Ditto in reverse.
			name: "reverse scans with key locking and MaxSpanRequestKeys=3",
			setup: func(t *testing.T, d *data) {
				writeABCDEF(t, d)
				scanAE := revScanArgsString("a", "e")
				scanAE.KeyLocking = lock.Exclusive
				d.ba.Add(scanAE)
				scanHJ := scanArgsString("h", "j")
				scanHJ.KeyLocking = lock.Exclusive
				d.ba.Add(scanHJ)
				d.ba.Txn = &txn
				d.ba.MaxSpanRequestKeys = 3
			},
			check: func(t *testing.T, r resp) {
				verifyScanResult(t, r, []string{"d", "c", "b"}, nil)
				verifyResumeSpans(t, r, "a-a\x00", "h-j")
				verifyAcquiredLocks(t, r, lock.Unreplicated, "d", "c", "b")
				verifyAcquiredLocks(t, r, lock.Replicated, []string(nil)...)
			},
		},
		{
			// Scanning with key locking and a TargetBytes limit should acquire
			// an unreplicated lock on each key returned and no locks on keys
			// past the limit.
			name: "scan with key locking and TargetBytes=1",
			setup: func(t *testing.T, d *data) {
				writeABCDEF(t, d)
				scanAE := scanArgsString("a", "e")
				scanAE.KeyLocking = lock.Exclusive
				d.ba.Add(scanAE)
				d.ba.Txn = &txn
				d.ba.TargetBytes = 1
			},
			check: func(t *testing.T, r resp) {
				verifyScanResult(t, r, []string{"a"})
				verifyResumeSpans(t, r, "b-e")
				verifyAcquiredLocks(t, r, lock.Unreplicated, "a")
				verifyAcquiredLocks(t, r, lock.Replicated, []string(nil)...)
			},
		},
		{
			// Ditto in reverse.
			name: "reverse scan with key locking and TargetBytes=1",
			setup: func(t *testing.T, d *data) {
				writeABCDEF(t, d)
				scanAE := revScanArgsString("a", "e")
				scanAE.KeyLocking = lock.Exclusive
				d.ba.Add(scanAE)
				d.ba.Txn = &txn
				d.ba.TargetBytes = 1
			},
			check: func(t *testing.T, r resp) {
				verifyScanResult(t, r, []string{"d"})
				verifyResumeSpans(t, r, "a-c\x00")
				verifyAcquiredLocks(t, r, lock.Unreplicated, "d")
				verifyAcquiredLocks(t, r, lock.Replicated, []string(nil)...)
			},
		},
		{
			// Scanning with key locking and a TargetBytes limit should acquire
			// an unreplicated lock on each key returned and no locks on keys
			// past the limit. One the batch's limit is exhausted, no more rows
			// are scanner nor locks acquired.
			name: "scans with key locking and TargetBytes=1",
			setup: func(t *testing.T, d *data) {
				writeABCDEF(t, d)
				scanAE := scanArgsString("a", "e")
				scanAE.KeyLocking = lock.Exclusive
				d.ba.Add(scanAE)
				scanHJ := scanArgsString("h", "j")
				scanHJ.KeyLocking = lock.Exclusive
				d.ba.Add(scanHJ)
				d.ba.Txn = &txn
				d.ba.TargetBytes = 1
			},
			check: func(t *testing.T, r resp) {
				verifyScanResult(t, r, []string{"a"}, nil)
				verifyResumeSpans(t, r, "b-e", "h-j")
				verifyAcquiredLocks(t, r, lock.Unreplicated, "a")
				verifyAcquiredLocks(t, r, lock.Replicated, []string(nil)...)
			},
		},
		{
			// Ditto in reverse.
			name: "reverse scans with key locking and TargetBytes=1",
			setup: func(t *testing.T, d *data) {
				writeABCDEF(t, d)
				scanAE := revScanArgsString("a", "e")
				scanAE.KeyLocking = lock.Exclusive
				d.ba.Add(scanAE)
				scanHJ := scanArgsString("h", "j")
				scanHJ.KeyLocking = lock.Exclusive
				d.ba.Add(scanHJ)
				d.ba.Txn = &txn
				d.ba.TargetBytes = 1
			},
			check: func(t *testing.T, r resp) {
				verifyScanResult(t, r, []string{"d"}, nil)
				verifyResumeSpans(t, r, "a-c\x00", "h-j")
				verifyAcquiredLocks(t, r, lock.Unreplicated, "d")
				verifyAcquiredLocks(t, r, lock.Replicated, []string(nil)...)
			},
		},
		//
		// Test suite for ResolveIntentRange with and without limits.
		//
		{
			// Three range intent resolutions that observe 3, 1, and 0 intent,
			// respectively. All intents should be resolved.
			name: "ranged intent resolution",
			setup: func(t *testing.T, d *data) {
				writeABCDEFIntents(t, d, &txn)
				d.ba.Add(resolveIntentRangeArgsString("a", "d", txn.TxnMeta, roachpb.COMMITTED))
				d.ba.Add(resolveIntentRangeArgsString("e", "f", txn.TxnMeta, roachpb.COMMITTED))
				d.ba.Add(resolveIntentRangeArgsString("h", "j", txn.TxnMeta, roachpb.COMMITTED))
			},
			check: func(t *testing.T, r resp) {
				verifyNumKeys(t, r, 3, 1, 0)
				verifyResumeSpans(t, r, "", "", "")
			},
		},
		{
			// Resolving intents with a giant limit should resolve everything.
			name: "ranged intent resolution with giant MaxSpanRequestKeys",
			setup: func(t *testing.T, d *data) {
				writeABCDEFIntents(t, d, &txn)
				d.ba.Add(resolveIntentRangeArgsString("a", "d", txn.TxnMeta, roachpb.COMMITTED))
				d.ba.Add(resolveIntentRangeArgsString("e", "f", txn.TxnMeta, roachpb.COMMITTED))
				d.ba.Add(resolveIntentRangeArgsString("h", "j", txn.TxnMeta, roachpb.COMMITTED))
				d.ba.MaxSpanRequestKeys = 100000
			},
			check: func(t *testing.T, r resp) {
				verifyNumKeys(t, r, 3, 1, 0)
				verifyResumeSpans(t, r, "", "", "")
			},
		},
		{
			// A batch limited to resolve only up to 3 keys should respect that
			// limit. The limit is saturated by the first request in the batch.
			name: "ranged intent resolution with MaxSpanRequestKeys=3",
			setup: func(t *testing.T, d *data) {
				writeABCDEFIntents(t, d, &txn)
				d.ba.Add(resolveIntentRangeArgsString("a", "d", txn.TxnMeta, roachpb.COMMITTED))
				d.ba.Add(resolveIntentRangeArgsString("e", "f", txn.TxnMeta, roachpb.COMMITTED))
				d.ba.Add(resolveIntentRangeArgsString("h", "j", txn.TxnMeta, roachpb.COMMITTED))
				d.ba.MaxSpanRequestKeys = 3
			},
			check: func(t *testing.T, r resp) {
				verifyNumKeys(t, r, 3, 0, 0)
				verifyResumeSpans(t, r, "c\x00-d", "e-f", "h-j")
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			eng := storage.NewDefaultInMem()
			defer eng.Close()

			d := &data{
				idKey: kvserverbase.CmdIDKey("testing"),
				eng:   eng,
			}
			d.AbortSpan = abortspan.New(1)
			d.ba.Header.Timestamp = ts

			tc.setup(t, d)

			var r resp
			r.d = d
			r.br, r.res, r.pErr = evaluateBatch(
				ctx,
				d.idKey,
				d.eng,
				d.MockEvalCtx.EvalContext(),
				&d.ms,
				&d.ba,
				d.readOnly,
			)

			tc.check(t, r)
		})
	}
}

type data struct {
	batcheval.MockEvalCtx
	ba       roachpb.BatchRequest
	idKey    kvserverbase.CmdIDKey
	eng      storage.Engine
	ms       enginepb.MVCCStats
	readOnly bool
}

type resp struct {
	d    *data
	br   *roachpb.BatchResponse
	res  result.Result
	pErr *roachpb.Error
}

type testCase struct {
	name  string
	setup func(*testing.T, *data)
	check func(*testing.T, resp)
}

func writeABCDEF(t *testing.T, d *data) {
	writeABCDEFIntents(t, d, nil /* txn */)
}

func writeABCDEFIntents(t *testing.T, d *data, txn *roachpb.Transaction) {
	for _, k := range []string{"a", "b", "c", "d", "e", "f"} {
		require.NoError(t, storage.MVCCPut(
			context.Background(), d.eng, nil /* ms */, roachpb.Key(k), d.ba.Timestamp,
			roachpb.MakeValueFromString("value-"+k), txn))
	}
}

func verifyScanResult(t *testing.T, r resp, keysPerResp ...[]string) {
	require.Nil(t, r.pErr)
	require.NotNil(t, r.br)
	require.Len(t, r.br.Responses, len(keysPerResp))
	for i, keys := range keysPerResp {
		var isGet bool
		scan := r.br.Responses[i].GetInner()
		var rows []roachpb.KeyValue
		switch req := scan.(type) {
		case *roachpb.ScanResponse:
			rows = req.Rows
		case *roachpb.ReverseScanResponse:
			rows = req.Rows
		case *roachpb.GetResponse:
			isGet = true
			rows = []roachpb.KeyValue{{
				Key:   r.d.ba.Requests[i].GetGet().Key,
				Value: *req.Value,
			}}
		default:
		}

		if !isGet {
			require.EqualValues(t, len(keys), scan.Header().NumKeys, "in response #%d", i+1)
		} else {
			require.Zero(t, scan.Header().NumKeys, "in response #%d", i+1)
		}
		var actKeys []string
		for _, row := range rows {
			actKeys = append(actKeys, string(row.Key))
		}
		require.Equal(t, keys, actKeys, "in response #%i", i+1)
	}
}

func verifyNumKeys(t *testing.T, r resp, keysPerResp ...int) {
	require.Nil(t, r.pErr)
	require.NotNil(t, r.br)
	require.Len(t, r.br.Responses, len(keysPerResp))
	for i, keys := range keysPerResp {
		actKeys := int(r.br.Responses[i].GetInner().Header().NumKeys)
		require.Equal(t, keys, actKeys, "in response #%i", i+1)
	}
}

func verifyResumeSpans(t *testing.T, r resp, resumeSpans ...string) {
	for i, span := range resumeSpans {
		rs := r.br.Responses[i].GetInner().Header().ResumeSpan
		if span == "" {
			require.Nil(t, rs)
		} else {
			require.NotNil(t, rs)
			act := fmt.Sprintf("%s-%s", string(rs.Key), string(rs.EndKey))
			require.Equal(t, span, act, "#%d", i+1)
		}
	}
}

func verifyAcquiredLocks(t *testing.T, r resp, dur lock.Durability, lockedKeys ...string) {
	var foundLocked []string
	for _, l := range r.res.Local.AcquiredLocks {
		if l.Durability == dur {
			foundLocked = append(foundLocked, string(l.Key))
		}
	}
	require.Equal(t, lockedKeys, foundLocked)
}
