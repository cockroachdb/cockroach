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
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestEvaluateBatch(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

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
			// A batch limited to return only one key. Throw in a Get which will come
			// up empty because there's no quota left. The second scan will also
			// return nothing due to lack of quota.
			name: "scans with MaxSpanRequestKeys=1",
			setup: func(t *testing.T, d *data) {
				writeABCDEF(t, d)
				d.ba.Add(scanArgsString("a", "c"))
				d.ba.Add(getArgsString("f"))
				d.ba.Add(scanArgsString("d", "f"))
				d.ba.MaxSpanRequestKeys = 1
			},
			check: func(t *testing.T, r resp) {
				verifyScanResult(t, r, []string{"a"}, nil, nil)
				verifyResumeSpans(t, r, "b-c", "f-", "d-f")
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
				verifyScanResult(t, r, []string{"e"}, nil, nil)
				verifyResumeSpans(t, r, "d-d\x00", "f-", "a-c")
			},
		}, {
			// Similar, but this time the request allows the second scan to
			// return one (but not more) remaining key. Again there's a Get, which
			// uses up one quota.
			name: "scans with MaxSpanRequestKeys=4",
			setup: func(t *testing.T, d *data) {
				writeABCDEF(t, d)
				d.ba.Add(scanArgsString("a", "c"))
				d.ba.Add(getArgsString("e"))
				d.ba.Add(scanArgsString("c", "e"))
				d.ba.MaxSpanRequestKeys = 4
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
			name: "reverse scans with MaxSpanRequestKeys=4",
			setup: func(t *testing.T, d *data) {
				writeABCDEF(t, d)
				d.ba.Add(revScanArgsString("c", "e"))
				d.ba.Add(getArgsString("e"))
				d.ba.Add(revScanArgsString("a", "c"))
				d.ba.MaxSpanRequestKeys = 4
			},
			check: func(t *testing.T, r resp) {
				verifyScanResult(t, r, []string{"d", "c"}, []string{"e"}, []string{"b"})
				verifyResumeSpans(t, r, "", "", "a-a\x00")
				b, err := r.br.Responses[1].GetGet().Value.GetBytes()
				require.NoError(t, err)
				require.Equal(t, "value-e", string(b))
			},
		}, {
			// GetRequests that come before scans.
			name: "gets and scans with MaxSpanRequestKeys=2, the second uses up the limit.",
			setup: func(t *testing.T, d *data) {
				writeABCDEF(t, d)
				d.ba.Add(getArgsString("a"))
				d.ba.Add(getArgsString("b"))
				d.ba.Add(getArgsString("c"))
				d.ba.Add(scanArgsString("d", "e"))
				d.ba.MaxSpanRequestKeys = 2
			},
			check: func(t *testing.T, r resp) {
				verifyScanResult(t, r, []string{"a"}, []string{"b"}, nil, nil)
				verifyResumeSpans(t, r, "", "", "c-", "d-e")
			},
		}, {
			// GetRequests that come before revscans.
			name: "gets and revscans with MaxSpanRequestKeys=2, the second uses up the limit.",
			setup: func(t *testing.T, d *data) {
				writeABCDEF(t, d)
				d.ba.Add(getArgsString("a"))
				d.ba.Add(getArgsString("b"))
				d.ba.Add(getArgsString("c"))
				d.ba.Add(revScanArgsString("d", "e"))
				d.ba.MaxSpanRequestKeys = 2
			},
			check: func(t *testing.T, r resp) {
				verifyScanResult(t, r, []string{"a"}, []string{"b"}, nil, nil)
				verifyResumeSpans(t, r, "", "", "c-", "d-e")
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
				verifyScanResult(t, r, []string{"a"}, nil, nil)
				verifyResumeSpans(t, r, "b-c", "e-", "c-e")
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
				verifyScanResult(t, r, []string{"d"}, nil, nil)
				verifyResumeSpans(t, r, "c-c\x00", "e-", "a-c")
			},
		}, {
			// GetRequests that come before scans.
			name: "gets and scans with TargetBytes=1, the first uses up the limit.",
			setup: func(t *testing.T, d *data) {
				writeABCDEF(t, d)
				d.ba.Add(getArgsString("a"))
				d.ba.Add(getArgsString("b"))
				d.ba.Add(scanArgsString("c", "e"))
				d.ba.TargetBytes = 1
			},
			check: func(t *testing.T, r resp) {
				verifyScanResult(t, r, []string{"a"}, nil, nil)
				verifyResumeSpans(t, r, "", "b-", "c-e")
			},
		}, {
			// GetRequests that come before revscans.
			name: "gets and revscans with TargetBytes=1, the first uses up the limit.",
			setup: func(t *testing.T, d *data) {
				writeABCDEF(t, d)
				d.ba.Add(getArgsString("a"))
				d.ba.Add(getArgsString("b"))
				d.ba.Add(revScanArgsString("c", "e"))
				d.ba.TargetBytes = 1
			},
			check: func(t *testing.T, r resp) {
				verifyScanResult(t, r, []string{"a"}, nil, nil)
				verifyResumeSpans(t, r, "", "b-", "c-e")
			},
		},
		//
		// Test suite for KeyLocking.
		//
		{
			// Two gets, one of which finds a key, one of which does not. An
			// unreplicated lock should be acquired on the key that existed.
			name: "gets with key locking",
			setup: func(t *testing.T, d *data) {
				writeABCDEFAt(t, d, ts.Prev())
				scanA := getArgsString("a")
				scanA.KeyLocking = lock.Exclusive
				d.ba.Add(scanA)
				scanG := getArgsString("g")
				scanG.KeyLocking = lock.Exclusive
				d.ba.Add(scanG)
				d.ba.Txn = &txn
			},
			check: func(t *testing.T, r resp) {
				verifyScanResult(t, r, []string{"a"}, nil)
				verifyAcquiredLocks(t, r, lock.Unreplicated, "a")
				verifyAcquiredLocks(t, r, lock.Replicated, []string(nil)...)
			},
		},
		{
			// Two gets, one of which finds a key, one of which does not. No
			// transaction set, so no locks should be acquired.
			name: "gets with key locking without txn",
			setup: func(t *testing.T, d *data) {
				writeABCDEFAt(t, d, ts.Prev())
				scanA := getArgsString("a")
				scanA.KeyLocking = lock.Exclusive
				d.ba.Add(scanA)
				scanG := getArgsString("g")
				scanG.KeyLocking = lock.Exclusive
				d.ba.Add(scanG)
			},
			check: func(t *testing.T, r resp) {
				verifyScanResult(t, r, []string{"a"}, nil)
				verifyAcquiredLocks(t, r, lock.Unreplicated, []string(nil)...)
				verifyAcquiredLocks(t, r, lock.Replicated, []string(nil)...)
			},
		},
		{
			// Three scans that observe 3, 1, and 0 keys, respectively. An
			// unreplicated lock should be acquired on each key that is scanned.
			name: "scans with key locking",
			setup: func(t *testing.T, d *data) {
				writeABCDEFAt(t, d, ts.Prev())
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
				writeABCDEFAt(t, d, ts.Prev())
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
				writeABCDEFAt(t, d, ts.Prev())
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
				writeABCDEFAt(t, d, ts.Prev())
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
				writeABCDEFAt(t, d, ts.Prev())
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
				writeABCDEFAt(t, d, ts.Prev())
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
				writeABCDEFAt(t, d, ts.Prev())
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
				writeABCDEFAt(t, d, ts.Prev())
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
				writeABCDEFAt(t, d, ts.Prev())
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
				writeABCDEFAt(t, d, ts.Prev())
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
				writeABCDEFAt(t, d, ts.Prev())
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
				writeABCDEFAt(t, d, ts.Prev())
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
			eng := storage.NewDefaultInMemForTesting()
			defer eng.Close()

			d := &data{
				idKey: kvserverbase.CmdIDKey("testing"),
				eng:   eng,
			}
			d.AbortSpan = abortspan.New(1)
			d.ba.Header.Timestamp = ts
			d.ClusterSettings = cluster.MakeClusterSettings()

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
				hlc.Timestamp{},
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
	writeABCDEFAt(t, d, d.ba.Timestamp)
}

func writeABCDEFAt(t *testing.T, d *data, ts hlc.Timestamp) {
	writeABCDEFWith(t, d.eng, ts, nil /* txn */)
}

func writeABCDEFIntents(t *testing.T, d *data, txn *roachpb.Transaction) {
	writeABCDEFWith(t, d.eng, txn.WriteTimestamp, txn)
}

func writeABCDEFWith(t *testing.T, eng storage.Engine, ts hlc.Timestamp, txn *roachpb.Transaction) {
	for _, k := range []string{"a", "b", "c", "d", "e", "f"} {
		require.NoError(t, storage.MVCCPut(
			context.Background(), eng, nil /* ms */, roachpb.Key(k), ts,
			roachpb.MakeValueFromString("value-"+k), txn))
	}
}

func verifyScanResult(t *testing.T, r resp, keysPerResp ...[]string) {
	t.Helper()
	require.Nil(t, r.pErr)
	require.NotNil(t, r.br)
	require.Len(t, r.br.Responses, len(keysPerResp))
	for i, keys := range keysPerResp {
		scan := r.br.Responses[i].GetInner()
		var rows []roachpb.KeyValue
		switch req := scan.(type) {
		case *roachpb.ScanResponse:
			rows = req.Rows
		case *roachpb.ReverseScanResponse:
			rows = req.Rows
		case *roachpb.GetResponse:
			if req.Value != nil {
				rows = []roachpb.KeyValue{{
					Key:   r.d.ba.Requests[i].GetGet().Key,
					Value: *req.Value,
				}}
			}
		default:
		}

		require.EqualValues(t, len(keys), scan.Header().NumKeys, "in response #%d", i+1)
		var actKeys []string
		for _, row := range rows {
			actKeys = append(actKeys, string(row.Key))
		}
		require.Equal(t, keys, actKeys, "in response #%i", i+1)
	}
}

func verifyNumKeys(t *testing.T, r resp, keysPerResp ...int) {
	t.Helper()
	require.Nil(t, r.pErr)
	require.NotNil(t, r.br)
	require.Len(t, r.br.Responses, len(keysPerResp))
	for i, keys := range keysPerResp {
		actKeys := int(r.br.Responses[i].GetInner().Header().NumKeys)
		require.Equal(t, keys, actKeys, "in response #%i", i+1)
	}
}

func verifyResumeSpans(t *testing.T, r resp, resumeSpans ...string) {
	t.Helper()
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
	t.Helper()
	var foundLocked []string
	for _, l := range r.res.Local.AcquiredLocks {
		if l.Durability == dur {
			foundLocked = append(foundLocked, string(l.Key))
		}
	}
	require.Equal(t, lockedKeys, foundLocked)
}
