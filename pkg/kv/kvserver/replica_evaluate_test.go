// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/abortspan"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/uncertainty"
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
	txn := roachpb.MakeTransaction("test", roachpb.Key("a"), 0, 0, ts, 0, 0, 0, false /* omitInRangefeeds */)

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
				verifyAcquiredLocks(t, r, acquiredLocks{})
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
				verifyReadResult(t, r, []string{"a", "b", "c", "d", "e", "f"})
				verifyResumeSpans(t, r, "")
				verifyAcquiredLocks(t, r, acquiredLocks{})
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
				verifyReadResult(t, r, []string{"f", "e", "d", "c", "b", "a"})
				verifyResumeSpans(t, r, "")
				verifyAcquiredLocks(t, r, acquiredLocks{})
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
				verifyReadResult(t, r, []string{"a", "b", "c", "d", "e", "f"})
				verifyResumeSpans(t, r, "")
				verifyAcquiredLocks(t, r, acquiredLocks{})
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
				verifyReadResult(t, r, []string{"f", "e", "d", "c", "b", "a"})
				verifyResumeSpans(t, r, "")
				verifyAcquiredLocks(t, r, acquiredLocks{})
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
				verifyReadResult(t, r, []string{"a", "b"}, []string{"d", "e", "f"})
				verifyResumeSpans(t, r, "", "")
				verifyAcquiredLocks(t, r, acquiredLocks{})
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
				verifyReadResult(t, r, []string{"f", "e", "d"}, []string{"b", "a"})
				verifyResumeSpans(t, r, "", "")
				verifyAcquiredLocks(t, r, acquiredLocks{})
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
				verifyReadResult(t, r, []string{"a"}, nil, nil)
				verifyResumeSpans(t, r, "b-c", "f-", "d-f")
				verifyAcquiredLocks(t, r, acquiredLocks{})
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
				verifyReadResult(t, r, []string{"e"}, nil, nil)
				verifyResumeSpans(t, r, "d-d\x00", "f-", "a-c")
				verifyAcquiredLocks(t, r, acquiredLocks{})
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
				verifyReadResult(t, r, []string{"a", "b"}, []string{"e"}, []string{"c"})
				verifyResumeSpans(t, r, "", "", "d-e")
				verifyAcquiredLocks(t, r, acquiredLocks{})
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
				verifyReadResult(t, r, []string{"d", "c"}, []string{"e"}, []string{"b"})
				verifyResumeSpans(t, r, "", "", "a-a\x00")
				verifyAcquiredLocks(t, r, acquiredLocks{})
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
				verifyReadResult(t, r, []string{"a"}, []string{"b"}, nil, nil)
				verifyResumeSpans(t, r, "", "", "c-", "d-e")
				verifyAcquiredLocks(t, r, acquiredLocks{})
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
				verifyReadResult(t, r, []string{"a"}, []string{"b"}, nil, nil)
				verifyResumeSpans(t, r, "", "", "c-", "d-e")
				verifyAcquiredLocks(t, r, acquiredLocks{})
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
				verifyReadResult(t, r, []string{"a"}, nil, nil)
				verifyResumeSpans(t, r, "b-c", "e-", "c-e")
				verifyAcquiredLocks(t, r, acquiredLocks{})
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
				verifyReadResult(t, r, []string{"d"}, nil, nil)
				verifyResumeSpans(t, r, "c-c\x00", "e-", "a-c")
				verifyAcquiredLocks(t, r, acquiredLocks{})
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
				verifyReadResult(t, r, []string{"a"}, nil, nil)
				verifyResumeSpans(t, r, "", "b-", "c-e")
				verifyAcquiredLocks(t, r, acquiredLocks{})
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
				verifyReadResult(t, r, []string{"a"}, nil, nil)
				verifyResumeSpans(t, r, "", "b-", "c-e")
				verifyAcquiredLocks(t, r, acquiredLocks{})
			},
		},
		//
		// Test suite for KeyLockingStrength.
		//
		{
			// Two gets, one of which finds a key, one of which does not. An
			// unreplicated lock should be acquired on the key that existed.
			name: "gets with key locking (exclusive, unreplicated)",
			setup: func(t *testing.T, d *data) {
				writeABCDEFAt(t, d, ts.Prev())
				getA := getArgsString("a")
				getA.KeyLockingStrength = lock.Exclusive
				getA.KeyLockingDurability = lock.Unreplicated
				d.ba.Add(getA)
				getG := getArgsString("g")
				getG.KeyLockingStrength = lock.Exclusive
				getG.KeyLockingDurability = lock.Unreplicated
				d.ba.Add(getG)
				d.ba.Txn = &txn
			},
			check: func(t *testing.T, r resp) {
				verifyReadResult(t, r, []string{"a"}, nil)
				verifyAcquiredLocks(t, r, acquiredLocks{
					lock.Exclusive: {lock.Unreplicated: []string{"a"}},
				})
			},
		},
		{
			// Same as above, but with shared locks.
			name: "gets with key locking (shared, unreplicated)",
			setup: func(t *testing.T, d *data) {
				writeABCDEFAt(t, d, ts.Prev())
				getA := getArgsString("a")
				getA.KeyLockingStrength = lock.Shared
				getA.KeyLockingDurability = lock.Unreplicated
				d.ba.Add(getA)
				getG := getArgsString("g")
				getG.KeyLockingStrength = lock.Shared
				getG.KeyLockingDurability = lock.Unreplicated
				d.ba.Add(getG)
				d.ba.Txn = &txn
			},
			check: func(t *testing.T, r resp) {
				verifyReadResult(t, r, []string{"a"}, nil)
				verifyAcquiredLocks(t, r, acquiredLocks{
					lock.Shared: {lock.Unreplicated: []string{"a"}},
				})
			},
		},
		{
			// Same as above, but with replicated exclusive locks.
			name: "gets with key locking (exclusive, replicated)",
			setup: func(t *testing.T, d *data) {
				writeABCDEFAt(t, d, ts.Prev())
				getA := getArgsString("a")
				getA.KeyLockingStrength = lock.Exclusive
				getA.KeyLockingDurability = lock.Replicated
				d.ba.Add(getA)
				getG := getArgsString("g")
				getG.KeyLockingStrength = lock.Exclusive
				getG.KeyLockingDurability = lock.Replicated
				d.ba.Add(getG)
				d.ba.Txn = &txn
			},
			check: func(t *testing.T, r resp) {
				verifyReadResult(t, r, []string{"a"}, nil)
				verifyAcquiredLocks(t, r, acquiredLocks{
					lock.Exclusive: {lock.Replicated: []string{"a"}},
				})
			},
		},
		{
			// Same as above, but with replicated shared locks.
			name: "gets with key locking (shared, replicated)",
			setup: func(t *testing.T, d *data) {
				writeABCDEFAt(t, d, ts.Prev())
				getA := getArgsString("a")
				getA.KeyLockingStrength = lock.Shared
				getA.KeyLockingDurability = lock.Replicated
				d.ba.Add(getA)
				getG := getArgsString("g")
				getG.KeyLockingStrength = lock.Shared
				getG.KeyLockingDurability = lock.Replicated
				d.ba.Add(getG)
				d.ba.Txn = &txn
			},
			check: func(t *testing.T, r resp) {
				verifyReadResult(t, r, []string{"a"}, nil)
				verifyAcquiredLocks(t, r, acquiredLocks{
					lock.Shared: {lock.Replicated: []string{"a"}},
				})
			},
		},
		{
			// Two gets, one of which finds a key, one of which does not. No
			// transaction set, so no locks should be acquired.
			name: "gets with key locking without txn",
			setup: func(t *testing.T, d *data) {
				writeABCDEFAt(t, d, ts.Prev())
				getA := getArgsString("a")
				getA.KeyLockingStrength = lock.Exclusive
				d.ba.Add(getA)
				getG := getArgsString("g")
				getG.KeyLockingStrength = lock.Exclusive
				d.ba.Add(getG)
			},
			check: func(t *testing.T, r resp) {
				verifyReadResult(t, r, []string{"a"}, nil)
				verifyAcquiredLocks(t, r, acquiredLocks{})
			},
		},
		{
			// Three scans that observe 3, 1, and 0 keys, respectively. An
			// unreplicated lock should be acquired on each key that is scanned.
			name: "scans with key locking (exclusive, unreplicated)",
			setup: func(t *testing.T, d *data) {
				writeABCDEFAt(t, d, ts.Prev())
				scanAD := scanArgsString("a", "d")
				scanAD.KeyLockingStrength = lock.Exclusive
				scanAD.KeyLockingDurability = lock.Unreplicated
				d.ba.Add(scanAD)
				scanEF := scanArgsString("e", "f")
				scanEF.KeyLockingStrength = lock.Exclusive
				scanEF.KeyLockingDurability = lock.Unreplicated
				d.ba.Add(scanEF)
				scanHJ := scanArgsString("h", "j")
				scanHJ.KeyLockingStrength = lock.Exclusive
				scanHJ.KeyLockingDurability = lock.Unreplicated
				d.ba.Add(scanHJ)
				d.ba.Txn = &txn
			},
			check: func(t *testing.T, r resp) {
				verifyReadResult(t, r, []string{"a", "b", "c"}, []string{"e"}, nil)
				verifyAcquiredLocks(t, r, acquiredLocks{
					lock.Exclusive: {lock.Unreplicated: []string{"a", "b", "c", "e"}},
				})
			},
		},
		{
			// Same as above, but with shared locks.
			name: "scans with key locking (shared, unreplicated)",
			setup: func(t *testing.T, d *data) {
				writeABCDEFAt(t, d, ts.Prev())
				scanAD := scanArgsString("a", "d")
				scanAD.KeyLockingStrength = lock.Shared
				scanAD.KeyLockingDurability = lock.Unreplicated
				d.ba.Add(scanAD)
				scanEF := scanArgsString("e", "f")
				scanEF.KeyLockingStrength = lock.Shared
				scanEF.KeyLockingDurability = lock.Unreplicated
				d.ba.Add(scanEF)
				scanHJ := scanArgsString("h", "j")
				scanHJ.KeyLockingStrength = lock.Shared
				scanHJ.KeyLockingDurability = lock.Unreplicated
				d.ba.Add(scanHJ)
				d.ba.Txn = &txn
			},
			check: func(t *testing.T, r resp) {
				verifyReadResult(t, r, []string{"a", "b", "c"}, []string{"e"}, nil)
				verifyAcquiredLocks(t, r, acquiredLocks{
					lock.Shared: {lock.Unreplicated: []string{"a", "b", "c", "e"}},
				})
			},
		},
		{
			// Same as above, but with replicated exclusive locks.
			name: "scans with key locking (exclusive, replicated)",
			setup: func(t *testing.T, d *data) {
				writeABCDEFAt(t, d, ts.Prev())
				scanAD := scanArgsString("a", "d")
				scanAD.KeyLockingStrength = lock.Exclusive
				scanAD.KeyLockingDurability = lock.Replicated
				d.ba.Add(scanAD)
				scanEF := scanArgsString("e", "f")
				scanEF.KeyLockingStrength = lock.Exclusive
				scanEF.KeyLockingDurability = lock.Replicated
				d.ba.Add(scanEF)
				scanHJ := scanArgsString("h", "j")
				scanHJ.KeyLockingStrength = lock.Exclusive
				scanHJ.KeyLockingDurability = lock.Replicated
				d.ba.Add(scanHJ)
				d.ba.Txn = &txn
			},
			check: func(t *testing.T, r resp) {
				verifyReadResult(t, r, []string{"a", "b", "c"}, []string{"e"}, nil)
				verifyAcquiredLocks(t, r, acquiredLocks{
					lock.Exclusive: {lock.Replicated: []string{"a", "b", "c", "e"}},
				})
			},
		},
		{
			// Same as above, but with replicated shared locks.
			name: "scans with key locking (shared, replicated)",
			setup: func(t *testing.T, d *data) {
				writeABCDEFAt(t, d, ts.Prev())
				scanAD := scanArgsString("a", "d")
				scanAD.KeyLockingStrength = lock.Shared
				scanAD.KeyLockingDurability = lock.Replicated
				d.ba.Add(scanAD)
				scanEF := scanArgsString("e", "f")
				scanEF.KeyLockingStrength = lock.Shared
				scanEF.KeyLockingDurability = lock.Replicated
				d.ba.Add(scanEF)
				scanHJ := scanArgsString("h", "j")
				scanHJ.KeyLockingStrength = lock.Shared
				scanHJ.KeyLockingDurability = lock.Replicated
				d.ba.Add(scanHJ)
				d.ba.Txn = &txn
			},
			check: func(t *testing.T, r resp) {
				verifyReadResult(t, r, []string{"a", "b", "c"}, []string{"e"}, nil)
				verifyAcquiredLocks(t, r, acquiredLocks{
					lock.Shared: {lock.Replicated: []string{"a", "b", "c", "e"}},
				})
			},
		},
		{
			// Ditto in reverse with exclusive locks.
			name: "reverse scans with key locking (exclusive, unreplicated)",
			setup: func(t *testing.T, d *data) {
				writeABCDEFAt(t, d, ts.Prev())
				scanAD := revScanArgsString("a", "d")
				scanAD.KeyLockingStrength = lock.Exclusive
				scanAD.KeyLockingDurability = lock.Unreplicated
				d.ba.Add(scanAD)
				scanEF := revScanArgsString("e", "f")
				scanEF.KeyLockingStrength = lock.Exclusive
				scanEF.KeyLockingDurability = lock.Unreplicated
				d.ba.Add(scanEF)
				scanHJ := revScanArgsString("h", "j")
				scanHJ.KeyLockingStrength = lock.Exclusive
				scanHJ.KeyLockingDurability = lock.Unreplicated
				d.ba.Add(scanHJ)
				d.ba.Txn = &txn
			},
			check: func(t *testing.T, r resp) {
				verifyReadResult(t, r, []string{"c", "b", "a"}, []string{"e"}, nil)
				verifyAcquiredLocks(t, r, acquiredLocks{
					lock.Exclusive: {lock.Unreplicated: []string{"c", "b", "a", "e"}},
				})
			},
		},
		{
			// Same as above, but with shared locks.
			name: "reverse scans with key locking (shared, unreplicated)",
			setup: func(t *testing.T, d *data) {
				writeABCDEFAt(t, d, ts.Prev())
				scanAD := revScanArgsString("a", "d")
				scanAD.KeyLockingStrength = lock.Shared
				scanAD.KeyLockingDurability = lock.Unreplicated
				d.ba.Add(scanAD)
				scanEF := revScanArgsString("e", "f")
				scanEF.KeyLockingStrength = lock.Shared
				scanEF.KeyLockingDurability = lock.Unreplicated
				d.ba.Add(scanEF)
				scanHJ := revScanArgsString("h", "j")
				scanHJ.KeyLockingStrength = lock.Shared
				scanHJ.KeyLockingDurability = lock.Unreplicated
				d.ba.Add(scanHJ)
				d.ba.Txn = &txn
			},
			check: func(t *testing.T, r resp) {
				verifyReadResult(t, r, []string{"c", "b", "a"}, []string{"e"}, nil)
				verifyAcquiredLocks(t, r, acquiredLocks{
					lock.Shared: {lock.Unreplicated: []string{"c", "b", "a", "e"}},
				})
			},
		},
		{
			// Same as above, but with replicated exclusive locks.
			name: "reverse scans with key locking (exclusive, replicated)",
			setup: func(t *testing.T, d *data) {
				writeABCDEFAt(t, d, ts.Prev())
				scanAD := revScanArgsString("a", "d")
				scanAD.KeyLockingStrength = lock.Exclusive
				scanAD.KeyLockingDurability = lock.Replicated
				d.ba.Add(scanAD)
				scanEF := revScanArgsString("e", "f")
				scanEF.KeyLockingStrength = lock.Exclusive
				scanEF.KeyLockingDurability = lock.Replicated
				d.ba.Add(scanEF)
				scanHJ := revScanArgsString("h", "j")
				scanHJ.KeyLockingStrength = lock.Exclusive
				scanHJ.KeyLockingDurability = lock.Replicated
				d.ba.Add(scanHJ)
				d.ba.Txn = &txn
			},
			check: func(t *testing.T, r resp) {
				verifyReadResult(t, r, []string{"c", "b", "a"}, []string{"e"}, nil)
				verifyAcquiredLocks(t, r, acquiredLocks{
					lock.Exclusive: {lock.Replicated: []string{"c", "b", "a", "e"}},
				})
			},
		},
		{
			// Same as above, but with replicated shared locks.
			name: "reverse scans with key locking (shared, replicated)",
			setup: func(t *testing.T, d *data) {
				writeABCDEFAt(t, d, ts.Prev())
				scanAD := revScanArgsString("a", "d")
				scanAD.KeyLockingStrength = lock.Shared
				scanAD.KeyLockingDurability = lock.Replicated
				d.ba.Add(scanAD)
				scanEF := revScanArgsString("e", "f")
				scanEF.KeyLockingStrength = lock.Shared
				scanEF.KeyLockingDurability = lock.Replicated
				d.ba.Add(scanEF)
				scanHJ := revScanArgsString("h", "j")
				scanHJ.KeyLockingStrength = lock.Shared
				scanHJ.KeyLockingDurability = lock.Replicated
				d.ba.Add(scanHJ)
				d.ba.Txn = &txn
			},
			check: func(t *testing.T, r resp) {
				verifyReadResult(t, r, []string{"c", "b", "a"}, []string{"e"}, nil)
				verifyAcquiredLocks(t, r, acquiredLocks{
					lock.Shared: {lock.Replicated: []string{"c", "b", "a", "e"}},
				})
			},
		},
		{
			// Three scans that observe 3, 1, and 0 keys, respectively. No
			// transaction set, so no locks should be acquired.
			name: "scans with key locking without txn",
			setup: func(t *testing.T, d *data) {
				writeABCDEFAt(t, d, ts.Prev())
				scanAD := scanArgsString("a", "d")
				scanAD.KeyLockingStrength = lock.Exclusive
				d.ba.Add(scanAD)
				scanEF := scanArgsString("e", "f")
				scanEF.KeyLockingStrength = lock.Exclusive
				d.ba.Add(scanEF)
				scanHJ := scanArgsString("h", "j")
				scanHJ.KeyLockingStrength = lock.Exclusive
				d.ba.Add(scanHJ)
			},
			check: func(t *testing.T, r resp) {
				verifyReadResult(t, r, []string{"a", "b", "c"}, []string{"e"}, nil)
				verifyAcquiredLocks(t, r, acquiredLocks{})
			},
		},
		{
			// Ditto in reverse.
			name: "reverse scans with key locking without txn",
			setup: func(t *testing.T, d *data) {
				writeABCDEFAt(t, d, ts.Prev())
				scanAD := revScanArgsString("a", "d")
				scanAD.KeyLockingStrength = lock.Exclusive
				d.ba.Add(scanAD)
				scanEF := revScanArgsString("e", "f")
				scanEF.KeyLockingStrength = lock.Exclusive
				d.ba.Add(scanEF)
				scanHJ := revScanArgsString("h", "j")
				scanHJ.KeyLockingStrength = lock.Exclusive
				d.ba.Add(scanHJ)
			},
			check: func(t *testing.T, r resp) {
				verifyReadResult(t, r, []string{"c", "b", "a"}, []string{"e"}, nil)
				verifyAcquiredLocks(t, r, acquiredLocks{})
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
				scanAE.KeyLockingStrength = lock.Exclusive
				d.ba.Add(scanAE)
				d.ba.Txn = &txn
				d.ba.MaxSpanRequestKeys = 3
			},
			check: func(t *testing.T, r resp) {
				verifyReadResult(t, r, []string{"a", "b", "c"})
				verifyResumeSpans(t, r, "d-e")
				verifyAcquiredLocks(t, r, acquiredLocks{
					lock.Exclusive: {lock.Unreplicated: []string{"a", "b", "c"}},
				})
			},
		},
		{
			// Ditto in reverse.
			name: "reverse scan with key locking and MaxSpanRequestKeys=3",
			setup: func(t *testing.T, d *data) {
				writeABCDEFAt(t, d, ts.Prev())
				scanAE := revScanArgsString("a", "e")
				scanAE.KeyLockingStrength = lock.Exclusive
				d.ba.Add(scanAE)
				d.ba.Txn = &txn
				d.ba.MaxSpanRequestKeys = 3
			},
			check: func(t *testing.T, r resp) {
				verifyReadResult(t, r, []string{"d", "c", "b"})
				verifyResumeSpans(t, r, "a-a\x00")
				verifyAcquiredLocks(t, r, acquiredLocks{
					lock.Exclusive: {lock.Unreplicated: []string{"d", "c", "b"}},
				})
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
				scanAE.KeyLockingStrength = lock.Exclusive
				d.ba.Add(scanAE)
				scanHJ := scanArgsString("h", "j")
				scanHJ.KeyLockingStrength = lock.Exclusive
				d.ba.Add(scanHJ)
				d.ba.Txn = &txn
				d.ba.MaxSpanRequestKeys = 3
			},
			check: func(t *testing.T, r resp) {
				verifyReadResult(t, r, []string{"a", "b", "c"}, nil)
				verifyResumeSpans(t, r, "d-e", "h-j")
				verifyAcquiredLocks(t, r, acquiredLocks{
					lock.Exclusive: {lock.Unreplicated: []string{"a", "b", "c"}},
				})
			},
		},
		{
			// Ditto in reverse.
			name: "reverse scans with key locking and MaxSpanRequestKeys=3",
			setup: func(t *testing.T, d *data) {
				writeABCDEFAt(t, d, ts.Prev())
				scanAE := revScanArgsString("a", "e")
				scanAE.KeyLockingStrength = lock.Exclusive
				d.ba.Add(scanAE)
				scanHJ := scanArgsString("h", "j")
				scanHJ.KeyLockingStrength = lock.Exclusive
				d.ba.Add(scanHJ)
				d.ba.Txn = &txn
				d.ba.MaxSpanRequestKeys = 3
			},
			check: func(t *testing.T, r resp) {
				verifyReadResult(t, r, []string{"d", "c", "b"}, nil)
				verifyResumeSpans(t, r, "a-a\x00", "h-j")
				verifyAcquiredLocks(t, r, acquiredLocks{
					lock.Exclusive: {lock.Unreplicated: []string{"d", "c", "b"}},
				})
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
				scanAE.KeyLockingStrength = lock.Exclusive
				d.ba.Add(scanAE)
				d.ba.Txn = &txn
				d.ba.TargetBytes = 1
			},
			check: func(t *testing.T, r resp) {
				verifyReadResult(t, r, []string{"a"})
				verifyResumeSpans(t, r, "b-e")
				verifyAcquiredLocks(t, r, acquiredLocks{
					lock.Exclusive: {lock.Unreplicated: []string{"a"}},
				})
			},
		},
		{
			// Ditto in reverse.
			name: "reverse scan with key locking and TargetBytes=1",
			setup: func(t *testing.T, d *data) {
				writeABCDEFAt(t, d, ts.Prev())
				scanAE := revScanArgsString("a", "e")
				scanAE.KeyLockingStrength = lock.Exclusive
				d.ba.Add(scanAE)
				d.ba.Txn = &txn
				d.ba.TargetBytes = 1
			},
			check: func(t *testing.T, r resp) {
				verifyReadResult(t, r, []string{"d"})
				verifyResumeSpans(t, r, "a-c\x00")
				verifyAcquiredLocks(t, r, acquiredLocks{
					lock.Exclusive: {lock.Unreplicated: []string{"d"}},
				})
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
				scanAE.KeyLockingStrength = lock.Exclusive
				d.ba.Add(scanAE)
				scanHJ := scanArgsString("h", "j")
				scanHJ.KeyLockingStrength = lock.Exclusive
				d.ba.Add(scanHJ)
				d.ba.Txn = &txn
				d.ba.TargetBytes = 1
			},
			check: func(t *testing.T, r resp) {
				verifyReadResult(t, r, []string{"a"}, nil)
				verifyResumeSpans(t, r, "b-e", "h-j")
				verifyAcquiredLocks(t, r, acquiredLocks{
					lock.Exclusive: {lock.Unreplicated: []string{"a"}},
				})
			},
		},
		{
			// Ditto in reverse.
			name: "reverse scans with key locking and TargetBytes=1",
			setup: func(t *testing.T, d *data) {
				writeABCDEFAt(t, d, ts.Prev())
				scanAE := revScanArgsString("a", "e")
				scanAE.KeyLockingStrength = lock.Exclusive
				d.ba.Add(scanAE)
				scanHJ := scanArgsString("h", "j")
				scanHJ.KeyLockingStrength = lock.Exclusive
				d.ba.Add(scanHJ)
				d.ba.Txn = &txn
				d.ba.TargetBytes = 1
			},
			check: func(t *testing.T, r resp) {
				verifyReadResult(t, r, []string{"d"}, nil)
				verifyResumeSpans(t, r, "a-c\x00", "h-j")
				verifyAcquiredLocks(t, r, acquiredLocks{
					lock.Exclusive: {lock.Unreplicated: []string{"d"}},
				})
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
				verifyAcquiredLocks(t, r, acquiredLocks{})
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
				verifyAcquiredLocks(t, r, acquiredLocks{})
			},
		},
		{
			// A batch limited to resolve only up to 2 keys should respect that
			// limit. The limit is saturated by the first request in the batch.
			name: "ranged intent resolution with MaxSpanRequestKeys=2",
			setup: func(t *testing.T, d *data) {
				writeABCDEFIntents(t, d, &txn)
				d.ba.Add(resolveIntentRangeArgsString("a", "d", txn.TxnMeta, roachpb.COMMITTED))
				d.ba.Add(resolveIntentRangeArgsString("e", "f", txn.TxnMeta, roachpb.COMMITTED))
				d.ba.Add(resolveIntentRangeArgsString("h", "j", txn.TxnMeta, roachpb.COMMITTED))
				d.ba.MaxSpanRequestKeys = 2
			},
			check: func(t *testing.T, r resp) {
				verifyNumKeys(t, r, 2, 0, 0)
				verifyResumeSpans(t, r, "b\x00-d", "e-f", "h-j")
				verifyAcquiredLocks(t, r, acquiredLocks{})
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
				verifyResumeSpans(t, r, "", "e-f", "h-j")
				verifyAcquiredLocks(t, r, acquiredLocks{})
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
			d.ClusterSettings = cluster.MakeTestingClusterSettings()

			tc.setup(t, d)

			var r resp
			r.d = d
			evalPath := readWrite
			if d.readOnly {
				evalPath = readOnlyDefault
			}
			r.br, r.res, r.pErr = evaluateBatch(
				ctx,
				d.idKey,
				d.eng,
				d.MockEvalCtx.EvalContext(),
				&d.ms,
				&d.ba,
				nil,
				nil,
				uncertainty.Interval{},
				evalPath,
				false, /* omitInRangefeeds */
			)

			tc.check(t, r)
		})
	}
}

type data struct {
	batcheval.MockEvalCtx
	ba       kvpb.BatchRequest
	idKey    kvserverbase.CmdIDKey
	eng      storage.Engine
	ms       enginepb.MVCCStats
	readOnly bool
}

type resp struct {
	d    *data
	br   *kvpb.BatchResponse
	res  result.Result
	pErr *kvpb.Error
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
		_, err := storage.MVCCPut(
			context.Background(), eng, roachpb.Key(k), ts,
			roachpb.MakeValueFromString("value-"+k), storage.MVCCWriteOptions{Txn: txn})
		require.NoError(t, err)
	}
}

func verifyReadResult(t *testing.T, r resp, keysPerResp ...[]string) {
	t.Helper()
	require.Nil(t, r.pErr)
	require.NotNil(t, r.br)
	require.Len(t, r.br.Responses, len(keysPerResp))
	for i, keys := range keysPerResp {
		scan := r.br.Responses[i].GetInner()
		var rows []roachpb.KeyValue
		switch req := scan.(type) {
		case *kvpb.ScanResponse:
			rows = req.Rows
		case *kvpb.ReverseScanResponse:
			rows = req.Rows
		case *kvpb.GetResponse:
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

type acquiredLocks map[lock.Strength]map[lock.Durability][]string

func verifyAcquiredLocks(t *testing.T, r resp, expLocked acquiredLocks) {
	t.Helper()
	foundLocked := make(acquiredLocks)
	for _, l := range r.res.Local.AcquiredLocks {
		if foundLocked[l.Strength] == nil {
			foundLocked[l.Strength] = make(map[lock.Durability][]string)
		}
		cur := foundLocked[l.Strength][l.Durability]
		foundLocked[l.Strength][l.Durability] = append(cur, string(l.Key))
	}
	require.Equal(t, expLocked, foundLocked)
}
