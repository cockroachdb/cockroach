// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package roachpb

import (
	"bytes"
	"math"
	"math/rand"
	"reflect"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/apd/v2"
	"github.com/cockroachdb/cockroach/pkg/cli/exit"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils/zerofields"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/bitarray"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/kr/pretty"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

func makeClockTS(walltime int64, logical int32) hlc.ClockTimestamp {
	return hlc.ClockTimestamp{
		WallTime: walltime,
		Logical:  logical,
	}
}

func makeTS(walltime int64, logical int32) hlc.Timestamp {
	return hlc.Timestamp{
		WallTime: walltime,
		Logical:  logical,
	}
}

func makeSynTS(walltime int64, logical int32) hlc.Timestamp {
	return hlc.Timestamp{
		WallTime:  walltime,
		Logical:   logical,
		Synthetic: true,
	}
}

// TestKeyNext tests that the method for creating lexicographic
// successors to byte slices works as expected.
func TestKeyNext(t *testing.T) {
	a := Key("a")
	aNext := a.Next()
	if a.Equal(aNext) {
		t.Errorf("expected key not equal to next")
	}
	if bytes.Compare(a, aNext) >= 0 {
		t.Errorf("expected next key to be greater")
	}

	extraCap := make([]byte, 2, 4)
	extraCap[0] = 'x'
	extraCap[1] = 'o'

	noExtraCap := make([]byte, 2)
	noExtraCap[0] = 'x'
	noExtraCap[1] = 'o'

	testCases := []struct {
		key           Key
		next          Key
		expReallocate int // 0 for don't test, -1 for not expected, 1 for expected.
	}{
		{nil, Key("\x00"), 0},
		{Key(""), Key("\x00"), 0},
		{Key("test key"), Key("test key\x00"), 0},
		{Key("\xff"), Key("\xff\x00"), 0},
		{Key("xoxo\x00"), Key("xoxo\x00\x00"), 0},
		{Key(extraCap), Key("xo\x00"), -1},
		{Key(noExtraCap), Key("xo\x00"), 1},
	}
	for i, c := range testCases {
		next := c.key.Next()
		if !bytes.Equal(next, c.next) {
			t.Errorf("%d: unexpected next bytes for %q: %q", i, c.key, next)
		}
		if c.expReallocate != 0 {
			if expect, reallocated := c.expReallocate > 0, (&next[0] != &c.key[0]); expect != reallocated {
				t.Errorf("%d: unexpected next reallocation = %t, found reallocation = %t", i, expect, reallocated)
			}
		}
	}
}

func TestKeyPrefixEnd(t *testing.T) {
	a := Key("a1")
	aNext := a.Next()
	aEnd := a.PrefixEnd()
	if bytes.Compare(a, aEnd) >= 0 {
		t.Errorf("expected end key to be greater")
	}
	if bytes.Compare(aNext, aEnd) >= 0 {
		t.Errorf("expected end key to be greater than next")
	}

	testCases := []struct {
		key Key
		end Key
	}{
		{Key{}, KeyMax},
		{Key{0}, Key{0x01}},
		{Key{0xff}, Key{0xff}},
		{Key{0xff, 0xff}, Key{0xff, 0xff}},
		{KeyMax, KeyMax},
		{Key{0xff, 0xfe}, Key{0xff, 0xff}},
		{Key{0x00, 0x00}, Key{0x00, 0x01}},
		{Key{0x00, 0xff}, Key{0x01}},
		{Key{0x00, 0xff, 0xff}, Key{0x01}},
	}
	for i, c := range testCases {
		if !bytes.Equal(c.key.PrefixEnd(), c.end) {
			t.Errorf("%d: unexpected prefix end bytes for %q: %q", i, c.key, c.key.PrefixEnd())
		}
	}
}

func TestKeyEqual(t *testing.T) {
	a1 := Key("a1")
	a2 := Key("a2")
	if !a1.Equal(a1) {
		t.Errorf("expected keys equal")
	}
	if a1.Equal(a2) {
		t.Errorf("expected different keys not equal")
	}
}

func TestKeyLess(t *testing.T) {
	testCases := []struct {
		a, b Key
		less bool
	}{
		{nil, Key("\x00"), true},
		{Key(""), Key("\x00"), true},
		{Key("a"), Key("b"), true},
		{Key("a\x00"), Key("a"), false},
		{Key("a\x00"), Key("a\x01"), true},
	}
	for i, c := range testCases {
		if (bytes.Compare(c.a, c.b) < 0) != c.less {
			t.Fatalf("%d: unexpected %q < %q: %t", i, c.a, c.b, c.less)
		}
	}
}

func TestKeyCompare(t *testing.T) {
	testCases := []struct {
		a, b    Key
		compare int
	}{
		{nil, nil, 0},
		{nil, Key("\x00"), -1},
		{Key("\x00"), Key("\x00"), 0},
		{Key(""), Key("\x00"), -1},
		{Key("a"), Key("b"), -1},
		{Key("a\x00"), Key("a"), 1},
		{Key("a\x00"), Key("a\x01"), -1},
	}
	for i, c := range testCases {
		if c.a.Compare(c.b) != c.compare {
			t.Fatalf("%d: unexpected %q.Compare(%q): %d", i, c.a, c.b, c.compare)
		}
	}
}

// TestNextKey tests that the method for creating successors of a Key
// works as expected.
func TestNextKey(t *testing.T) {
	testCases := []struct {
		key  Key
		next Key
	}{
		{nil, Key("\x00")},
		{Key(""), Key("\x00")},
		{Key("test key"), Key("test key\x00")},
		{Key("\xff\xff"), Key("\xff\xff\x00")},
		{Key("xoxo\x00"), Key("xoxo\x00\x00")},
	}
	for i, c := range testCases {
		if !c.key.Next().Equal(c.next) {
			t.Fatalf("%d: unexpected next key for %q: %s", i, c.key, c.key.Next())
		}
	}
}

func TestIsPrev(t *testing.T) {
	for i, tc := range []struct {
		k, m Key
		ok   bool
	}{
		{k: Key(""), m: Key{0}, ok: true},
		{k: nil, m: nil, ok: false},
		{k: Key("a"), m: Key{'a', 0, 0}, ok: false},
		{k: Key{'z', 'a', 0}, m: Key{'z', 'a'}, ok: false},
		{k: Key("bro"), m: Key{'b', 'r', 'o', 0}, ok: true},
		{k: Key("foo"), m: Key{'b', 'a', 'r', 0}, ok: false},
	} {
		if tc.ok != tc.k.IsPrev(tc.m) {
			t.Errorf("%d: wanted %t", i, tc.ok)
		}
	}
}

func TestValueDataEquals(t *testing.T) {
	strVal := func(s string) *Value {
		var v Value
		v.SetString(s)
		return &v
	}

	a := strVal("val1")

	b := strVal("val1")
	b.InitChecksum([]byte("key1"))

	c := strVal("val1")
	c.InitChecksum([]byte("key2"))

	// Different values.
	d := strVal("val2")

	e := strVal("val2")
	e.InitChecksum([]byte("key1"))

	// Different tags.
	f := strVal("val1")
	f.setTag(ValueType_INT)

	g := strVal("val1")
	g.setTag(ValueType_INT)
	g.InitChecksum([]byte("key1"))

	for i, tc := range []struct {
		v1, v2 *Value
		eq     bool
	}{
		{v1: a, v2: b, eq: true},
		{v1: a, v2: c, eq: true},
		{v1: a, v2: d, eq: false},
		{v1: a, v2: e, eq: false},
		{v1: a, v2: f, eq: false},
		{v1: a, v2: g, eq: false},
		{v1: b, v2: c, eq: true},
		{v1: b, v2: d, eq: false},
		{v1: b, v2: e, eq: false},
		{v1: b, v2: f, eq: false},
		{v1: b, v2: g, eq: false},
		{v1: c, v2: d, eq: false},
		{v1: c, v2: e, eq: false},
		{v1: c, v2: f, eq: false},
		{v1: c, v2: g, eq: false},
		{v1: d, v2: e, eq: true},
		{v1: d, v2: f, eq: false},
		{v1: d, v2: g, eq: false},
		{v1: e, v2: f, eq: false},
		{v1: e, v2: g, eq: false},
		{v1: f, v2: g, eq: true},
	} {
		if tc.eq != tc.v1.EqualTagAndData(*tc.v2) {
			t.Errorf("%d: wanted eq=%t", i, tc.eq)
		}
		// Test symmetry.
		if tc.eq != tc.v2.EqualTagAndData(*tc.v1) {
			t.Errorf("%d: wanted eq=%t", i, tc.eq)
		}
	}
}

func TestValueChecksumEmpty(t *testing.T) {
	k := []byte("key")
	v := Value{}
	// Before initializing checksum, always works.
	if err := v.Verify(k); err != nil {
		t.Error(err)
	}
	if err := v.Verify([]byte("key2")); err != nil {
		t.Error(err)
	}
	v.InitChecksum(k)
	if err := v.Verify(k); err != nil {
		t.Error(err)
	}
}

func TestValueChecksumWithBytes(t *testing.T) {
	k := []byte("key")
	v := MakeValueFromString("abc")
	v.InitChecksum(k)
	if err := v.Verify(k); err != nil {
		t.Error(err)
	}
	// Try a different key; should fail.
	if err := v.Verify([]byte("key2")); err == nil {
		t.Error("expected checksum verification failure on different key")
	}
	// Mess with the value. In order to corrupt the data for testing purposes we
	// have to ensure we overwrite the data without touching the checksum.
	copy(v.RawBytes[headerSize:], "cba")
	if err := v.Verify(k); err == nil {
		t.Error("expected checksum verification failure on different value")
	}
	// Test ClearChecksum and reinitialization of checksum.
	v.ClearChecksum()
	v.InitChecksum(k)
	if err := v.Verify(k); err != nil {
		t.Error(err)
	}
}

func TestSetGetChecked(t *testing.T) {
	v := Value{}

	v.SetBytes(nil)
	if _, err := v.GetBytes(); err != nil {
		t.Fatal(err)
	}

	f := 1.1
	v.SetFloat(f)
	if r, err := v.GetFloat(); err != nil {
		t.Fatal(err)
	} else if f != r {
		t.Errorf("set %f on a value and extracted it, expected %f back, but got %f", f, f, r)
	}

	b := true
	v.SetBool(b)
	if r, err := v.GetBool(); err != nil {
		t.Fatal(err)
	} else if b != r {
		t.Errorf("set %t on a value and extracted it, expected %t back, but got %t", b, b, r)
	}

	i := int64(1)
	v.SetInt(i)
	if r, err := v.GetInt(); err != nil {
		t.Fatal(err)
	} else if i != r {
		t.Errorf("set %d on a value and extracted it, expected %d back, but got %d", i, i, r)
	}

	dec := apd.New(11, -1)
	if err := v.SetDecimal(dec); err != nil {
		t.Fatal(err)
	}
	if r, err := v.GetDecimal(); err != nil {
		t.Fatal(err)
	} else if dec.Cmp(&r) != 0 {
		t.Errorf("set %s on a value and extracted it, expected %s back, but got %s", dec, dec, &r)
	}

	if err := v.SetProto(&Value{}); err != nil {
		t.Fatal(err)
	}
	if err := v.GetProto(&Value{}); err != nil {
		t.Fatal(err)
	}
	if _, err := v.GetBytes(); err != nil {
		t.Fatal(err)
	}

	if err := v.SetProto(&InternalTimeSeriesData{}); err != nil {
		t.Fatal(err)
	}
	if _, err := v.GetTimeseries(); err != nil {
		t.Fatal(err)
	}

	ti := time.Time{}
	v.SetTime(ti)
	if r, err := v.GetTime(); err != nil {
		t.Fatal(err)
	} else if !ti.Equal(r) {
		t.Errorf("set %s on a value and extracted it, expected %s back, but got %s", ti, ti, r)
	}
}

func TestTransactionBumpEpoch(t *testing.T) {
	origNow := makeTS(10, 1)
	txn := MakeTransaction("test", Key("a"), 1, origNow, 0)
	// Advance the txn timestamp.
	txn.WriteTimestamp = txn.WriteTimestamp.Add(10, 2)
	txn.BumpEpoch()
	if a, e := txn.Epoch, enginepb.TxnEpoch(1); a != e {
		t.Errorf("expected epoch %d; got %d", e, a)
	}
}

// TestTransactionObservedTimestamp verifies that txn.{Get,Update}ObservedTimestamp work as
// advertised.
func TestTransactionObservedTimestamp(t *testing.T) {
	var txn Transaction
	rng, seed := randutil.NewPseudoRand()
	t.Logf("running with seed %d", seed)
	ids := append([]int{109, 104, 102, 108, 1000}, rand.Perm(100)...)
	timestamps := make(map[NodeID]hlc.ClockTimestamp, len(ids))
	for i := 0; i < len(ids); i++ {
		timestamps[NodeID(i)] = hlc.ClockTimestamp{WallTime: rng.Int63()}
	}
	for i, n := range ids {
		nodeID := NodeID(n)
		if ts, ok := txn.GetObservedTimestamp(nodeID); ok {
			t.Fatalf("%d: false positive hit %s in %v", nodeID, ts, ids[:i+1])
		}
		txn.UpdateObservedTimestamp(nodeID, timestamps[nodeID])
		txn.UpdateObservedTimestamp(nodeID, hlc.MaxClockTimestamp) // should be noop
		if exp, act := i+1, len(txn.ObservedTimestamps); act != exp {
			t.Fatalf("%d: expected %d entries, got %d: %v", nodeID, exp, act, txn.ObservedTimestamps)
		}
	}
	for _, m := range ids {
		checkID := NodeID(m)
		expTS := timestamps[checkID]
		if actTS, _ := txn.GetObservedTimestamp(checkID); actTS != expTS {
			t.Fatalf("%d: expected %s, got %s", checkID, expTS, actTS)
		}
	}

	var emptyTxn Transaction
	ts := hlc.ClockTimestamp{WallTime: 1, Logical: 2}
	emptyTxn.UpdateObservedTimestamp(NodeID(1), ts)
	if actTS, _ := emptyTxn.GetObservedTimestamp(NodeID(1)); actTS != ts {
		t.Fatalf("unexpected: %s (wanted %s)", actTS, ts)
	}
}

func TestFastPathObservedTimestamp(t *testing.T) {
	var txn Transaction
	nodeID := NodeID(1)
	if _, ok := txn.GetObservedTimestamp(nodeID); ok {
		t.Errorf("fetched observed timestamp where none should exist")
	}
	expTS := hlc.ClockTimestamp{WallTime: 10}
	txn.UpdateObservedTimestamp(nodeID, expTS)
	if ts, ok := txn.GetObservedTimestamp(nodeID); !ok || !ts.Equal(expTS) {
		t.Errorf("expected %s; got %s", expTS, ts)
	}
	expTS = hlc.ClockTimestamp{WallTime: 9}
	txn.UpdateObservedTimestamp(nodeID, expTS)
	if ts, ok := txn.GetObservedTimestamp(nodeID); !ok || !ts.Equal(expTS) {
		t.Errorf("expected %s; got %s", expTS, ts)
	}
}

var nonZeroTxn = Transaction{
	TxnMeta: enginepb.TxnMeta{
		Key:            Key("foo"),
		ID:             uuid.MakeV4(),
		Epoch:          2,
		WriteTimestamp: makeSynTS(20, 21),
		MinTimestamp:   makeSynTS(10, 11),
		Priority:       957356782,
		Sequence:       123,
	},
	Name:                   "name",
	Status:                 COMMITTED,
	LastHeartbeat:          makeSynTS(1, 2),
	ReadTimestamp:          makeSynTS(20, 22),
	GlobalUncertaintyLimit: makeSynTS(40, 41),
	ObservedTimestamps: []ObservedTimestamp{{
		NodeID: 1,
		Timestamp: hlc.ClockTimestamp{
			WallTime:  1,
			Logical:   2,
			Synthetic: true, // normally not set, but needed for zerofields.NoZeroField
		},
	}},
	WriteTooOld:          true,
	LockSpans:            []Span{{Key: []byte("a"), EndKey: []byte("b")}},
	InFlightWrites:       []SequencedWrite{{Key: []byte("c"), Sequence: 1}},
	CommitTimestampFixed: true,
	IgnoredSeqNums:       []enginepb.IgnoredSeqNumRange{{Start: 888, End: 999}},
}

func TestTransactionUpdate(t *testing.T) {
	txn := nonZeroTxn
	if err := zerofields.NoZeroField(txn); err != nil {
		t.Fatal(err)
	}

	// Updating an empty Transaction copies all fields.
	var txn2 Transaction
	txn2.Update(&txn)

	expTxn2 := txn
	require.Equal(t, expTxn2, txn2)

	// Updating a Transaction at an earlier epoch replaces all epoch-scoped fields.
	var txn3 Transaction
	txn3.ID = txn.ID
	txn3.Epoch = txn.Epoch - 1
	txn3.Status = STAGING
	txn3.Name = "carl"
	txn3.Priority = 123
	txn3.Update(&txn)

	expTxn3 := txn
	expTxn3.Name = "carl"
	require.Equal(t, expTxn3, txn3)

	// Updating a Transaction at the same epoch forwards all epoch-scoped fields.
	var txn4 Transaction
	txn4.ID = txn.ID
	txn4.Epoch = txn.Epoch
	txn4.Status = STAGING
	txn4.Sequence = txn.Sequence + 10
	txn4.Name = "carl"
	txn4.Priority = 123
	txn4.Update(&txn)

	expTxn4 := txn
	expTxn4.Name = "carl"
	expTxn4.Sequence = txn.Sequence + 10
	require.Equal(t, expTxn4, txn4)

	// Test the updates to the WriteTooOld field. The WriteTooOld field is
	// supposed to be dictated by the transaction with the higher ReadTimestamp,
	// or it's cumulative when the ReadTimestamps are equal.
	{
		txn2 := txn
		txn2.ReadTimestamp = txn2.ReadTimestamp.Add(-1, 0)
		txn2.WriteTooOld = false
		txn2.Update(&txn)
		require.True(t, txn2.WriteTooOld)
	}
	{
		txn2 := txn
		txn2.WriteTooOld = false
		txn2.Update(&txn)
		require.True(t, txn2.WriteTooOld)
	}
	{
		txn2 := txn
		txn2.ReadTimestamp = txn2.ReadTimestamp.Add(1, 0)
		txn2.WriteTooOld = false
		txn2.Update(&txn)
		require.False(t, txn2.WriteTooOld)
	}

	// Updating a Transaction at a future epoch ignores all epoch-scoped fields.
	var txn5 Transaction
	txn5.ID = txn.ID
	txn5.Epoch = txn.Epoch + 1
	txn5.Status = PENDING
	txn5.Sequence = txn.Sequence - 10
	txn5.Name = "carl"
	txn5.Priority = 123
	txn5.Update(&txn)

	expTxn5 := txn
	expTxn5.Name = "carl"
	expTxn5.Epoch = txn.Epoch + 1
	expTxn5.Status = PENDING
	expTxn5.Sequence = txn.Sequence - 10
	expTxn5.LockSpans = nil
	expTxn5.InFlightWrites = nil
	expTxn5.IgnoredSeqNums = nil
	expTxn5.WriteTooOld = false
	expTxn5.CommitTimestampFixed = false
	require.Equal(t, expTxn5, txn5)

	// Updating a different transaction fatals.
	var exited bool
	log.SetExitFunc(true /* hideStack */, func(exit.Code) { exited = true })
	defer log.ResetExitFunc()

	var txn6 Transaction
	txn6.ID = uuid.MakeV4()
	origTxn6 := txn6
	txn6.Update(&txn)

	require.Equal(t, origTxn6, txn6)
	require.True(t, exited)
}

func TestTransactionUpdateMinTimestamp(t *testing.T) {
	txn := nonZeroTxn
	var txn2 Transaction
	txn2.Update(&txn)

	if a, e := txn2.MinTimestamp, txn.MinTimestamp; a != e {
		t.Errorf("expected min timestamp %s; got %s", e, a)
	}

	txn3 := nonZeroTxn
	txn3.MinTimestamp = nonZeroTxn.MinTimestamp.Prev()
	txn.Update(&txn3)

	if a, e := txn.MinTimestamp, txn3.MinTimestamp; a != e {
		t.Errorf("expected min timestamp %s; got %s", e, a)
	}
}

func TestTransactionUpdateStaging(t *testing.T) {
	txn := nonZeroTxn
	txn.Status = PENDING

	txn2 := nonZeroTxn
	txn2.Status = STAGING

	// In same epoch, PENDING < STAGING.
	txn.Update(&txn2)
	if a, e := txn.Status, STAGING; a != e {
		t.Errorf("expected status %s; got %s", e, a)
	}

	txn2.Status = PENDING
	txn.Update(&txn2)
	if a, e := txn.Status, STAGING; a != e {
		t.Errorf("expected status %s; got %s", e, a)
	}

	// In later epoch, PENDING > STAGING.
	txn2.Epoch++
	txn.Update(&txn2)
	if a, e := txn.Status, PENDING; a != e {
		t.Errorf("expected status %s; got %s", e, a)
	}

	txn2.Status = STAGING
	txn.Update(&txn2)
	if a, e := txn.Status, STAGING; a != e {
		t.Errorf("expected status %s; got %s", e, a)
	}

	txn2.Status = COMMITTED
	txn.Update(&txn2)
	if a, e := txn.Status, COMMITTED; a != e {
		t.Errorf("expected status %s; got %s", e, a)
	}
}

// TestTransactionUpdateAbortedOldEpoch tests that Transaction.Update propagates
// an ABORTED status even when that status comes from a proto with an old epoch.
// Once a transaction is ABORTED, it will stay aborted, even if its coordinator
// doesn't know this at the time that it increments its epoch and retries.
func TestTransactionUpdateAbortedOldEpoch(t *testing.T) {
	txn := nonZeroTxn
	txn.Status = ABORTED

	txnRestart := txn
	txnRestart.Epoch++
	txnRestart.Status = PENDING
	txnRestart.Update(&txn)

	expTxn := txn
	expTxn.Epoch++
	expTxn.Status = ABORTED
	require.Equal(t, expTxn, txnRestart)
}

func TestTransactionClone(t *testing.T) {
	txnPtr := nonZeroTxn.Clone()
	txn := *txnPtr

	fields := util.EqualPtrFields(reflect.ValueOf(nonZeroTxn), reflect.ValueOf(txn), "")
	sort.Strings(fields)

	// Verify that the only equal pointer fields after cloning are the ones
	// listed below. If this test fails, please update the list below and/or
	// Transaction.Clone().
	expFields := []string{
		"IgnoredSeqNums",
		"InFlightWrites",
		"InFlightWrites.Key",
		"LockSpans",
		"LockSpans.EndKey",
		"LockSpans.Key",
		"ObservedTimestamps",
		"TxnMeta.Key",
	}
	if !reflect.DeepEqual(expFields, fields) {
		t.Fatalf("%s != %s", expFields, fields)
	}
	if !reflect.DeepEqual(nonZeroTxn, txn) {
		t.Fatalf("e = %v, v = %v", nonZeroTxn, txn)
	}
}

func TestTransactionRestart(t *testing.T) {
	txn := nonZeroTxn
	txn.Restart(1, 1, makeTS(25, 1))

	expTxn := nonZeroTxn
	expTxn.Epoch++
	expTxn.Sequence = 0
	expTxn.WriteTimestamp = makeTS(25, 1)
	expTxn.ReadTimestamp = makeTS(25, 1)
	expTxn.WriteTooOld = false
	expTxn.CommitTimestampFixed = false
	expTxn.LockSpans = nil
	expTxn.InFlightWrites = nil
	expTxn.IgnoredSeqNums = nil
	require.Equal(t, expTxn, txn)
}

func TestTransactionRefresh(t *testing.T) {
	txn := nonZeroTxn
	txn.Refresh(makeTS(25, 1))

	expTxn := nonZeroTxn
	expTxn.WriteTimestamp = makeTS(25, 1)
	expTxn.ReadTimestamp = makeTS(25, 1)
	expTxn.WriteTooOld = false
	require.Equal(t, expTxn, txn)
}

// TestTransactionRecordRoundtrips tests a few properties about Transaction
// and TransactionRecord protos. Remember that the latter is wire compatible
// with the former and contains a subset of its protos.
//
// Assertions:
// 1. Transaction->TransactionRecord->Transaction is lossless for the fields
//    in TransactionRecord. It drops all other fields.
// 2. TransactionRecord->Transaction->TransactionRecord is lossless.
//    Fields not in TransactionRecord are set as zero values.
// 3. Transaction messages can be decoded as TransactionRecord messages.
//    Fields not in TransactionRecord are dropped.
// 4. TransactionRecord messages can be decoded as Transaction messages.
//    Fields not in TransactionRecord are decoded as zero values.
func TestTransactionRecordRoundtrips(t *testing.T) {
	// Verify that converting from a Transaction to a TransactionRecord
	// strips out fields but is lossless for the desired fields.
	txn := nonZeroTxn
	txnRecord := txn.AsRecord()
	if err := zerofields.NoZeroField(txnRecord); err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(txnRecord.TxnMeta, txn.TxnMeta) {
		t.Errorf("txnRecord.TxnMeta = %v, txn.TxnMeta = %v", txnRecord.TxnMeta, txn.TxnMeta)
	}
	if !reflect.DeepEqual(txnRecord.Status, txn.Status) {
		t.Errorf("txnRecord.Status = %v, txn.Status = %v", txnRecord.Status, txn.Status)
	}
	if !reflect.DeepEqual(txnRecord.LastHeartbeat, txn.LastHeartbeat) {
		t.Errorf("txnRecord.LastHeartbeat = %v, txn.LastHeartbeat = %v", txnRecord.LastHeartbeat, txn.LastHeartbeat)
	}
	if !reflect.DeepEqual(txnRecord.LockSpans, txn.LockSpans) {
		t.Errorf("txnRecord.LockSpans = %v, txn.LockSpans = %v", txnRecord.LockSpans, txn.LockSpans)
	}
	if !reflect.DeepEqual(txnRecord.InFlightWrites, txn.InFlightWrites) {
		t.Errorf("txnRecord.InFlightWrites = %v, txn.InFlightWrites = %v", txnRecord.InFlightWrites, txn.InFlightWrites)
	}
	if !reflect.DeepEqual(txnRecord.IgnoredSeqNums, txn.IgnoredSeqNums) {
		t.Errorf("txnRecord.IgnoredSeqNums = %v, txn.IgnoredSeqNums = %v", txnRecord.IgnoredSeqNums, txn.IgnoredSeqNums)
	}

	// Verify that converting through a Transaction message and back
	// to a TransactionRecord is a lossless round trip.
	txn2 := txnRecord.AsTransaction()
	txnRecord2 := txn2.AsRecord()
	if !reflect.DeepEqual(txnRecord, txnRecord2) {
		t.Errorf("txnRecord = %v, txnRecord2 = %v", txnRecord, txnRecord2)
	}

	// Verify that encoded Transaction messages can be decoded as
	// TransactionRecord messages.
	txnBytes, err := protoutil.Marshal(&txn)
	if err != nil {
		t.Fatal(err)
	}
	var txnRecord3 TransactionRecord
	if err := protoutil.Unmarshal(txnBytes, &txnRecord3); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(txnRecord, txnRecord3) {
		t.Errorf("txnRecord = %v, txnRecord3 = %v", txnRecord, txnRecord3)
	}

	// Verify that encoded TransactionRecord messages can be decoded
	// as Transaction messages.
	txnRecordBytes, err := protoutil.Marshal(&txnRecord)
	if err != nil {
		t.Fatal(err)
	}
	var txn3 Transaction
	if err := protoutil.Unmarshal(txnRecordBytes, &txn3); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(txn2, txn3) {
		t.Errorf("txn2 = %v, txn3 = %v", txn2, txn3)
	}
}

// checkVal verifies if a value is close to an expected value, within a fraction (e.g. if
// fraction=0.1, it checks if val is within 10% of expected).
func checkVal(val, expected, errFraction float64) bool {
	return val > expected*(1-errFraction) && val < expected*(1+errFraction)
}

// TestMakePriority verifies that setting user priority of P results
// in MakePriority returning priorities that are P times more likely
// to be higher than a priority with user priority = 1.
func TestMakePriority(t *testing.T) {
	// Verify min & max.
	if a, e := MakePriority(MinUserPriority), enginepb.MinTxnPriority; a != e {
		t.Errorf("expected min txn priority %d; got %d", e, a)
	}
	if a, e := MakePriority(MaxUserPriority), enginepb.MaxTxnPriority; a != e {
		t.Errorf("expected max txn priority %d; got %d", e, a)
	}

	userPs := []UserPriority{
		0.0011,
		0.01,
		0.1,
		0.5,
		0, // Same as 1.0 (special cased below)
		1.0,
		2.0,
		10.0,
		100.0,
		999.0,
	}

	// Generate values for all priorities.
	const trials = 100000
	values := make([][trials]enginepb.TxnPriority, len(userPs))
	for i, userPri := range userPs {
		for tr := 0; tr < trials; tr++ {
			p := MakePriority(userPri)
			if p == enginepb.MinTxnPriority {
				t.Fatalf("unexpected min txn priority")
			}
			if p == enginepb.MaxTxnPriority {
				t.Fatalf("unexpected max txn priority")
			}
			values[i][tr] = p
		}
	}

	// Check win-vs-loss ratios for all pairs
	for i, p1 := range userPs {
		for j, p2 := range userPs {
			if i >= j {
				continue
			}

			// Special case to verify that specifying 0 has same effect as specifying 1.
			if p1 == 0 {
				p1 = 1
			}
			if p2 == 0 {
				p2 = 1
			}
			priRatio := float64(p1 / p2)

			// Don't verify extreme ratios (we don't have enough resolution or trials)
			if math.Max(priRatio, 1/priRatio) >= 1000 {
				continue
			}
			wins := 0
			for t := 0; t < trials; t++ {
				if values[i][t] > values[j][t] {
					wins++
				}
			}

			winRatio := float64(wins) / float64(trials-wins)

			t.Logf("%f vs %f: %d wins, %d losses (winRatio=%f, priRatio=%f)",
				p1, p2, wins, trials-wins, winRatio, priRatio)
			if !checkVal(winRatio, priRatio, 0.2) {
				t.Errorf("Error (%f vs %f: %d wins, %d losses): winRatio=%f not "+
					"close to priRatio=%f", p1, p2, wins, trials-wins, winRatio, priRatio)
			}
		}
	}
}

// TestMakePriorityExplicit verifies that setting priority to a negative
// value sets it exactly.
func TestMakePriorityExplicit(t *testing.T) {
	explicitPs := []struct {
		userPri UserPriority
		expPri  enginepb.TxnPriority
	}{
		{-math.MaxInt32, math.MaxInt32},
		{-math.MaxInt32 + 1, math.MaxInt32 - 1},
		{-2, 2},
		{-1, 1},
	}
	for i, p := range explicitPs {
		if pri := MakePriority(p.userPri); pri != p.expPri {
			t.Errorf("%d: explicit priority %d doesn't match expected %d", i, pri, p.expPri)
		}
	}
}

// TestMakePriorityLimits verifies that min & max priorities are
// enforced and yield txn priority limits.
func TestMakePriorityLimits(t *testing.T) {
	userPs := []UserPriority{
		0.000000001,
		0.00001,
		0.00009,
		10001,
		100000,
		math.MaxFloat64,
	}
	for _, userPri := range userPs {
		expected := enginepb.MinTxnPriority
		if userPri > 1 {
			expected = enginepb.MaxTxnPriority
		}
		if actual := MakePriority(userPri); actual != expected {
			t.Errorf("%f: expected txn priority %d; got %d", userPri, expected, actual)
		}
	}
}

func TestLeaseEquivalence(t *testing.T) {
	r1 := ReplicaDescriptor{NodeID: 1, StoreID: 1, ReplicaID: 1}
	r2 := ReplicaDescriptor{NodeID: 2, StoreID: 2, ReplicaID: 2}
	ts1 := makeClockTS(1, 1)
	ts2 := makeClockTS(2, 1)
	ts3 := makeClockTS(3, 1)

	epoch1 := Lease{Replica: r1, Start: ts1, Epoch: 1}
	epoch2 := Lease{Replica: r1, Start: ts1, Epoch: 2}
	expire1 := Lease{Replica: r1, Start: ts1, Expiration: ts2.ToTimestamp().Clone()}
	expire2 := Lease{Replica: r1, Start: ts1, Expiration: ts3.ToTimestamp().Clone()}
	epoch2TS2 := Lease{Replica: r2, Start: ts2, Epoch: 2}
	expire2TS2 := Lease{Replica: r2, Start: ts2, Expiration: ts3.ToTimestamp().Clone()}

	proposed1 := Lease{Replica: r1, Start: ts1, Epoch: 1, ProposedTS: &ts1}
	proposed2 := Lease{Replica: r1, Start: ts1, Epoch: 2, ProposedTS: &ts1}
	proposed3 := Lease{Replica: r1, Start: ts1, Epoch: 1, ProposedTS: &ts2}

	stasis1 := Lease{Replica: r1, Start: ts1, Epoch: 1, DeprecatedStartStasis: ts1.ToTimestamp().Clone()}
	stasis2 := Lease{Replica: r1, Start: ts1, Epoch: 1, DeprecatedStartStasis: ts2.ToTimestamp().Clone()}

	r1Voter, r1Learner := r1, r1
	r1Voter.Type = ReplicaTypeVoterFull()
	r1Learner.Type = ReplicaTypeLearner()
	epoch1Voter := Lease{Replica: r1Voter, Start: ts1, Epoch: 1}
	epoch1Learner := Lease{Replica: r1Learner, Start: ts1, Epoch: 1}

	testCases := []struct {
		l, ol      Lease
		expSuccess bool
	}{
		{epoch1, epoch1, true},             // same epoch lease
		{expire1, expire1, true},           // same expiration lease
		{epoch1, epoch2, false},            // different epoch leases
		{epoch1, epoch2TS2, false},         // different epoch leases
		{expire1, expire2TS2, false},       // different expiration leases
		{expire1, expire2, true},           // same expiration lease, extended
		{expire2, expire1, false},          // same expiration lease, extended but backwards
		{epoch1, expire1, false},           // epoch and expiration leases
		{expire1, epoch1, false},           // expiration and epoch leases
		{proposed1, proposed1, true},       // exact leases with identical timestamps
		{proposed1, proposed2, false},      // same proposed timestamps, but diff epochs
		{proposed1, proposed3, true},       // different proposed timestamps, same lease
		{stasis1, stasis2, true},           // same lease, different stasis timestamps
		{epoch1, epoch1Voter, true},        // same epoch lease, different replica type
		{epoch1, epoch1Learner, true},      // same epoch lease, different replica type
		{epoch1Voter, epoch1Learner, true}, // same epoch lease, different replica type
	}

	for i, tc := range testCases {
		if ok := tc.l.Equivalent(tc.ol); tc.expSuccess != ok {
			t.Errorf("%d: expected success? %t; got %t", i, tc.expSuccess, ok)
		}
	}

	// #18689 changed the nullability of the DeprecatedStartStasis, ProposedTS, and Expiration
	// field. It introduced a bug whose regression is caught below where a zero Expiration and a nil
	// Expiration in an epoch-based lease led to mistakenly considering leases non-equivalent.
	prePRLease := Lease{
		Start: hlc.ClockTimestamp{WallTime: 10},
		Epoch: 123,

		// The bug-trigger.
		Expiration: new(hlc.Timestamp),

		// Similar potential bug triggers, but these were actually handled correctly.
		DeprecatedStartStasis: new(hlc.Timestamp),
		ProposedTS:            &hlc.ClockTimestamp{WallTime: 10},
	}
	postPRLease := prePRLease
	postPRLease.DeprecatedStartStasis = nil
	postPRLease.Expiration = nil

	if !postPRLease.Equivalent(prePRLease) || !prePRLease.Equivalent(postPRLease) {
		t.Fatalf("leases not equivalent but should be despite diff(pre,post) = %s", pretty.Diff(prePRLease, postPRLease))
	}
}

func TestLeaseEqual(t *testing.T) {
	type expectedLease struct {
		Start                 hlc.ClockTimestamp
		Expiration            *hlc.Timestamp
		Replica               ReplicaDescriptor
		DeprecatedStartStasis *hlc.Timestamp
		ProposedTS            *hlc.ClockTimestamp
		Epoch                 int64
		Sequence              LeaseSequence
		AcquisitionType       LeaseAcquisitionType
	}
	// Verify that the lease structure does not change unexpectedly. If a compile
	// error occurs on the following line of code, update the expectedLease
	// structure AND update Lease.Equal.
	var _ = expectedLease(Lease{})

	// Verify that nil == &hlc.Timestamp{} for the Expiration and
	// DeprecatedStartStasis fields. See #19843.
	a := Lease{}
	b := Lease{
		Expiration:            &hlc.Timestamp{},
		DeprecatedStartStasis: &hlc.Timestamp{},
	}
	if !a.Equal(b) {
		t.Fatalf("unexpectedly did not compare equal: %s", pretty.Diff(a, b))
	}

	if !(*Lease)(nil).Equal(nil) {
		t.Fatalf("unexpectedly did not compare equal")
	}
	if !(*Lease)(nil).Equal((*Lease)(nil)) {
		t.Fatalf("unexpectedly did not compare equal")
	}
	if (*Lease)(nil).Equal(Lease{}) {
		t.Fatalf("expectedly compared equal")
	}
	if (*Lease)(nil).Equal(&Lease{}) {
		t.Fatalf("expectedly compared equal")
	}
	if (&Lease{}).Equal(nil) {
		t.Fatalf("expectedly compared equal")
	}
	if (&Lease{}).Equal((*Lease)(nil)) {
		t.Fatalf("expectedly compared equal")
	}

	clockTS := hlc.ClockTimestamp{Logical: 1}
	ts := clockTS.ToTimestamp()
	testCases := []Lease{
		{Start: clockTS},
		{Expiration: &ts},
		{Replica: ReplicaDescriptor{NodeID: 1}},
		{DeprecatedStartStasis: &ts},
		{ProposedTS: &clockTS},
		{Epoch: 1},
		{Sequence: 1},
	}
	for _, c := range testCases {
		t.Run("", func(t *testing.T) {
			if c.Equal(Lease{}) {
				t.Fatalf("unexpected equality: %s", pretty.Diff(c, Lease{}))
			}
		})
	}
}

func TestSpanOverlaps(t *testing.T) {
	sA := Span{Key: []byte("a")}
	sD := Span{Key: []byte("d")}
	sAtoC := Span{Key: []byte("a"), EndKey: []byte("c")}
	sBtoD := Span{Key: []byte("b"), EndKey: []byte("d")}
	// Invalid spans.
	sCtoA := Span{Key: []byte("c"), EndKey: []byte("a")}
	sDtoB := Span{Key: []byte("d"), EndKey: []byte("b")}

	testData := []struct {
		s1, s2   Span
		overlaps bool
	}{
		{sA, sA, true},
		{sA, sD, false},
		{sA, sBtoD, false},
		{sBtoD, sA, false},
		{sD, sBtoD, false},
		{sBtoD, sD, false},
		{sA, sAtoC, true},
		{sAtoC, sA, true},
		{sAtoC, sAtoC, true},
		{sAtoC, sBtoD, true},
		{sBtoD, sAtoC, true},
		// Invalid spans.
		{sAtoC, sDtoB, false},
		{sDtoB, sAtoC, false},
		{sBtoD, sCtoA, false},
		{sCtoA, sBtoD, false},
	}
	for i, test := range testData {
		if o := test.s1.Overlaps(test.s2); o != test.overlaps {
			t.Errorf("%d: expected overlap %t; got %t between %s vs. %s", i, test.overlaps, o, test.s1, test.s2)
		}
	}
}

func TestSpanIntersect(t *testing.T) {
	sA := Span{Key: []byte("a")}
	sD := Span{Key: []byte("d")}
	sAtoC := Span{Key: []byte("a"), EndKey: []byte("c")}
	sAtoD := Span{Key: []byte("a"), EndKey: []byte("d")}
	sBtoC := Span{Key: []byte("b"), EndKey: []byte("c")}
	sBtoD := Span{Key: []byte("b"), EndKey: []byte("d")}
	sCtoD := Span{Key: []byte("c"), EndKey: []byte("d")}
	// Invalid spans.
	sCtoA := Span{Key: []byte("c"), EndKey: []byte("a")}
	sDtoB := Span{Key: []byte("d"), EndKey: []byte("b")}

	testData := []struct {
		s1, s2 Span
		expect Span
	}{
		{sA, sA, sA},
		{sA, sAtoC, sA},
		{sAtoC, sA, sA},
		{sAtoC, sAtoC, sAtoC},
		{sAtoC, sAtoD, sAtoC},
		{sAtoD, sAtoC, sAtoC},
		{sAtoC, sBtoC, sBtoC},
		{sBtoC, sAtoC, sBtoC},
		{sAtoC, sBtoD, sBtoC},
		{sBtoD, sAtoC, sBtoC},
		{sAtoD, sBtoC, sBtoC},
		{sBtoC, sAtoD, sBtoC},
		// Empty intersections.
		{sA, sD, Span{}},
		{sA, sBtoD, Span{}},
		{sBtoD, sA, Span{}},
		{sD, sBtoD, Span{}},
		{sBtoD, sD, Span{}},
		{sAtoC, sCtoD, Span{}},
		{sCtoD, sAtoC, Span{}},
		// Invalid spans.
		{sAtoC, sDtoB, Span{}},
		{sDtoB, sAtoC, Span{}},
		{sBtoD, sCtoA, Span{}},
		{sCtoA, sBtoD, Span{}},
	}
	for _, test := range testData {
		in := test.s1.Intersect(test.s2)
		if test.expect.Valid() {
			require.True(t, in.Valid())
			require.Equal(t, test.expect, in)
		} else {
			require.False(t, in.Valid())
		}
	}
}

func TestSpanCombine(t *testing.T) {
	sA := Span{Key: []byte("a")}
	sD := Span{Key: []byte("d")}
	sAtoC := Span{Key: []byte("a"), EndKey: []byte("c")}
	sAtoD := Span{Key: []byte("a"), EndKey: []byte("d")}
	sAtoDNext := Span{Key: []byte("a"), EndKey: Key([]byte("d")).Next()}
	sBtoD := Span{Key: []byte("b"), EndKey: []byte("d")}
	sBtoDNext := Span{Key: []byte("b"), EndKey: Key([]byte("d")).Next()}
	// Invalid spans.
	sCtoA := Span{Key: []byte("c"), EndKey: []byte("a")}
	sDtoB := Span{Key: []byte("d"), EndKey: []byte("b")}

	testData := []struct {
		s1, s2   Span
		combined Span
	}{
		{sA, sA, sA},
		{sA, sD, sAtoDNext},
		{sA, sBtoD, sAtoD},
		{sBtoD, sA, sAtoD},
		{sD, sBtoD, sBtoDNext},
		{sBtoD, sD, sBtoDNext},
		{sA, sAtoC, sAtoC},
		{sAtoC, sA, sAtoC},
		{sAtoC, sAtoC, sAtoC},
		{sAtoC, sBtoD, sAtoD},
		{sBtoD, sAtoC, sAtoD},
		// Invalid spans.
		{sAtoC, sDtoB, Span{}},
		{sDtoB, sAtoC, Span{}},
		{sBtoD, sCtoA, Span{}},
		{sCtoA, sBtoD, Span{}},
	}
	for i, test := range testData {
		if combined := test.s1.Combine(test.s2); !reflect.DeepEqual(combined, test.combined) {
			t.Errorf("%d: expected combined %s; got %s between %s vs. %s", i, test.combined, combined, test.s1, test.s2)
		}
	}
}

// TestSpanContains verifies methods to check whether a key
// or key range is contained within the span.
func TestSpanContains(t *testing.T) {
	s := Span{Key: []byte("a"), EndKey: []byte("b")}

	testData := []struct {
		start, end string
		contains   bool
	}{
		// Single keys.
		{"a", "", true},
		{"aa", "", true},
		{"`", "", false},
		{"b", "", false},
		{"c", "", false},
		// Key ranges.
		{"a", "b", true},
		{"a", "aa", true},
		{"aa", "b", true},
		{"0", "9", false},
		{"`", "a", false},
		{"b", "bb", false},
		{"0", "bb", false},
		{"aa", "bb", false},
		{"b", "a", false},
	}
	for i, test := range testData {
		if s.Contains(sp(test.start, test.end)) != test.contains {
			t.Errorf("%d: expected span %q-%q within range to be %v",
				i, test.start, test.end, test.contains)
		}
	}
}

func TestSpanSplitOnKey(t *testing.T) {
	s := Span{Key: []byte("b"), EndKey: []byte("c")}

	testData := []struct {
		split []byte
		left  Span
		right Span
	}{
		// Split on start/end key should fail.
		{
			[]byte("b"),
			s,
			Span{},
		},
		{
			[]byte("c"),
			s,
			Span{},
		},

		// Before start key.
		{
			[]byte("a"),
			s,
			Span{},
		},
		// After end key.
		{
			[]byte("d"),
			s,
			Span{},
		},

		// Simple split.
		{
			[]byte("bb"),
			sp("b", "bb"),
			sp("bb", "c"),
		},
	}
	for testIdx, test := range testData {
		t.Run(strconv.Itoa(testIdx), func(t *testing.T) {
			actualL, actualR := s.SplitOnKey(test.split)
			if !test.left.Equal(actualL) {
				t.Fatalf("expected left span after split to be %v, got %v", test.left, actualL)
			}

			if !test.right.Equal(actualR) {
				t.Fatalf("expected right span after split to be %v, got %v", test.right, actualL)
			}
		})
	}
}

func TestSpanValid(t *testing.T) {
	testData := []struct {
		start, end []byte
		valid      bool
	}{
		{[]byte("a"), nil, true},
		{[]byte("a"), []byte("b"), true},
		{[]byte(""), []byte(""), false},
		{[]byte(""), []byte("a"), true},
		{[]byte("a"), []byte("a"), false},
		{[]byte("b"), []byte("aa"), false},
	}
	for i, test := range testData {
		s := Span{Key: test.start, EndKey: test.end}
		if test.valid != s.Valid() {
			t.Errorf("%d: expected span %q-%q to return %t for Valid, instead got %t",
				i, test.start, test.end, test.valid, s.Valid())
		}
	}
}

// TestRSpanContains verifies methods to check whether a key
// or key range is contained within the span.
func TestRSpanContains(t *testing.T) {
	rs := RSpan{Key: []byte("a"), EndKey: []byte("b")}

	testData := []struct {
		start, end []byte
		contains   bool
	}{
		// Single keys.
		{[]byte("a"), []byte("a"), true},
		{[]byte("a"), nil, true},
		{[]byte("aa"), []byte("aa"), true},
		{[]byte("`"), []byte("`"), false},
		{[]byte("b"), []byte("b"), false},
		{[]byte("b"), nil, false},
		{[]byte("c"), []byte("c"), false},
		// Key ranges.
		{[]byte("a"), []byte("b"), true},
		{[]byte("a"), []byte("aa"), true},
		{[]byte("aa"), []byte("b"), true},
		{[]byte("0"), []byte("9"), false},
		{[]byte("`"), []byte("a"), false},
		{[]byte("b"), []byte("bb"), false},
		{[]byte("0"), []byte("bb"), false},
		{[]byte("aa"), []byte("bb"), false},
		{[]byte("b"), []byte("a"), false},
	}
	for i, test := range testData {
		if bytes.Equal(test.start, test.end) {
			if rs.ContainsKey(test.start) != test.contains {
				t.Errorf("%d: expected key %q within range", i, test.start)
			}
		}
		if rs.ContainsKeyRange(test.start, test.end) != test.contains {
			t.Errorf("%d: expected key %q within range", i, test.start)
		}
	}
}

// TestRSpanContainsKeyInverted verifies ContainsKeyInverted to check whether a key
// is contained within the span.
func TestRSpanContainsKeyInverted(t *testing.T) {
	rs := RSpan{Key: []byte("b"), EndKey: []byte("c")}

	testData := []struct {
		key      RKey
		contains bool
	}{
		// Single keys.
		{RKey("a"), false},
		{RKey("b"), false},
		{RKey("bb"), true},
		{RKey("c"), true},
		{RKey("c").Next(), false},
	}
	for i, test := range testData {
		if rs.ContainsKeyInverted(test.key) != test.contains {
			t.Errorf("%d: expected key %q within range", i, test.key)
		}
	}
}

// TestRSpanIntersect verifies rSpan.intersect.
func TestRSpanIntersect(t *testing.T) {
	rs := RSpan{Key: RKey("b"), EndKey: RKey("e")}

	testData := []struct {
		startKey, endKey RKey
		expected         RSpan
	}{
		// Partially overlapping.
		{RKey("a"), RKey("c"), RSpan{Key: RKey("b"), EndKey: RKey("c")}},
		{RKey("d"), RKey("f"), RSpan{Key: RKey("d"), EndKey: RKey("e")}},
		// Descriptor surrounds the span.
		{RKey("a"), RKey("f"), RSpan{Key: RKey("b"), EndKey: RKey("e")}},
		// Span surrounds the descriptor.
		{RKey("c"), RKey("d"), RSpan{Key: RKey("c"), EndKey: RKey("d")}},
		// Descriptor has the same range as the span.
		{RKey("b"), RKey("e"), RSpan{Key: RKey("b"), EndKey: RKey("e")}},
	}

	for i, test := range testData {
		desc := RangeDescriptor{}
		desc.StartKey = test.startKey
		desc.EndKey = test.endKey

		actual, err := rs.Intersect(&desc)
		if err != nil {
			t.Error(err)
			continue
		}
		if !actual.Equal(test.expected) {
			t.Errorf("%d: expected %+v but got %+v", i, test.expected, actual)
		}
	}

	// Error scenarios
	errorTestData := []struct {
		startKey, endKey RKey
	}{
		{RKey("a"), RKey("b")},
		{RKey("e"), RKey("f")},
		{RKey("f"), RKey("g")},
	}
	for i, test := range errorTestData {
		desc := RangeDescriptor{}
		desc.StartKey = test.startKey
		desc.EndKey = test.endKey
		if _, err := rs.Intersect(&desc); err == nil {
			t.Errorf("%d: unexpected success", i)
		}
	}
}

func BenchmarkValueSetBytes(b *testing.B) {
	v := Value{}
	bytes := make([]byte, 16)

	for i := 0; i < b.N; i++ {
		v.SetBytes(bytes)
	}
}

func BenchmarkValueSetFloat(b *testing.B) {
	v := Value{}
	f := 1.1

	for i := 0; i < b.N; i++ {
		v.SetFloat(f)
	}
}

func BenchmarkValueSetBool(b *testing.B) {
	v := Value{}
	bo := true

	for i := 0; i < b.N; i++ {
		v.SetBool(bo)
	}
}

func BenchmarkValueSetInt(b *testing.B) {
	v := Value{}
	in := int64(1)

	for i := 0; i < b.N; i++ {
		v.SetInt(in)
	}
}

func BenchmarkValueSetProto(b *testing.B) {
	v := Value{}
	p := &Value{}

	for i := 0; i < b.N; i++ {
		if err := v.SetProto(p); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkValueSetTime(b *testing.B) {
	v := Value{}
	ti := time.Time{}

	for i := 0; i < b.N; i++ {
		v.SetTime(ti)
	}
}

func BenchmarkValueSetDecimal(b *testing.B) {
	v := Value{}
	dec := apd.New(11, -1)

	for i := 0; i < b.N; i++ {
		if err := v.SetDecimal(dec); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkValueSetTuple(b *testing.B) {
	v := Value{}
	bytes := make([]byte, 16)

	for i := 0; i < b.N; i++ {
		v.SetTuple(bytes)
	}
}

func BenchmarkValueGetBytes(b *testing.B) {
	v := Value{}
	bytes := make([]byte, 16)
	v.SetBytes(bytes)

	for i := 0; i < b.N; i++ {
		if _, err := v.GetBytes(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkValueGetFloat(b *testing.B) {
	v := Value{}
	f := 1.1
	v.SetFloat(f)

	for i := 0; i < b.N; i++ {
		if _, err := v.GetFloat(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkValueGetBool(b *testing.B) {
	v := Value{}
	bo := true
	v.SetBool(bo)

	for i := 0; i < b.N; i++ {
		if _, err := v.GetBool(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkValueGetInt(b *testing.B) {
	v := Value{}
	in := int64(1)
	v.SetInt(in)

	for i := 0; i < b.N; i++ {
		if _, err := v.GetInt(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkValueGetProto(b *testing.B) {
	v := Value{}
	p, dst := &Value{}, &Value{}
	if err := v.SetProto(p); err != nil {
		b.Fatal(err)
	}

	for i := 0; i < b.N; i++ {
		if err := v.GetProto(dst); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkValueGetTime(b *testing.B) {
	v := Value{}
	ti := time.Time{}
	v.SetTime(ti)

	for i := 0; i < b.N; i++ {
		if _, err := v.GetTime(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkValueGetDecimal(b *testing.B) {
	v := Value{}
	dec := apd.New(11, -1)
	if err := v.SetDecimal(dec); err != nil {
		b.Fatal(err)
	}

	for i := 0; i < b.N; i++ {
		if _, err := v.GetDecimal(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkValueGetTuple(b *testing.B) {
	v := Value{}
	bytes := make([]byte, 16)
	v.SetTuple(bytes)

	for i := 0; i < b.N; i++ {
		if _, err := v.GetTuple(); err != nil {
			b.Fatal(err)
		}
	}
}

func TestValuePrettyPrint(t *testing.T) {
	var boolValue Value
	boolValue.SetBool(true)

	var intValue Value
	intValue.SetInt(7)

	var floatValue Value
	floatValue.SetFloat(6.28)

	var timeValue Value
	timeValue.SetTime(time.Date(2016, 6, 29, 16, 2, 50, 5, time.UTC))

	var decimalValue Value
	_ = decimalValue.SetDecimal(apd.New(628, -2))

	var durationValue Value
	_ = durationValue.SetDuration(duration.DecodeDuration(1, 2, 3))

	var tupleValue Value
	tupleBytes := encoding.EncodeBytesValue(encoding.EncodeIntValue(nil, 1, 8), 2, []byte("foo"))
	tupleValue.SetTuple(tupleBytes)

	var bytesValuePrintable, bytesValueNonPrintable Value
	bytesValuePrintable.SetBytes([]byte("abc"))
	bytesValueNonPrintable.SetBytes([]byte{0x89})

	var bitArrayValue Value
	ba := bitarray.MakeBitArrayFromInt64(8, 58, 7)
	bitArrayValue.SetBitArray(ba)

	var errValue Value
	errValue.SetInt(7)
	errValue.setTag(ValueType_FLOAT)

	var errTagValue Value
	errTagValue.SetInt(7)
	errTagValue.setTag(ValueType(99))

	tests := []struct {
		v        Value
		expected string
	}{
		{boolValue, "/INT/1"},
		{intValue, "/INT/7"},
		{floatValue, "/FLOAT/6.28"},
		{timeValue, "/TIME/2016-06-29T16:02:50.000000005Z"},
		{decimalValue, "/DECIMAL/6.28"},
		{durationValue, "/DURATION/1 mon 2 days 00:00:00+3ns"},
		{MakeValueFromBytes([]byte{0x1, 0x2, 0xF, 0xFF}), "/BYTES/0x01020fff"},
		{MakeValueFromString("foo"), "/BYTES/foo"},
		{tupleValue, "/TUPLE/1:1:Int/8/2:3:Bytes/foo"},
		{bytesValuePrintable, "/BYTES/abc"},
		{bytesValueNonPrintable, "/BYTES/0x89"},
		{bitArrayValue, "/BITARRAY/B00111010"},
		{errValue, "/<err: float64 value should be exactly 8 bytes: 1>"},
		{errTagValue, "/<err: unknown tag: 99>"},
	}

	for i, test := range tests {
		if str := test.v.PrettyPrint(); str != test.expected {
			t.Errorf("%d: got %q expected %q", i, str, test.expected)
		}
	}
}
func TestUpdateObservedTimestamps(t *testing.T) {
	f := func(nodeID NodeID, walltime int64) ObservedTimestamp {
		return ObservedTimestamp{
			NodeID: nodeID,
			Timestamp: hlc.ClockTimestamp{
				WallTime: walltime,
			},
		}
	}

	testCases := []struct {
		input    observedTimestampSlice
		expected observedTimestampSlice
	}{
		{nil, nil},
		{
			observedTimestampSlice{f(1, 1)},
			observedTimestampSlice{f(1, 1)},
		},
		{
			observedTimestampSlice{f(1, 1), f(1, 2)},
			observedTimestampSlice{f(1, 1)},
		},
		{
			observedTimestampSlice{f(1, 2), f(1, 1)},
			observedTimestampSlice{f(1, 1)},
		},
		{
			observedTimestampSlice{f(1, 1), f(2, 1)},
			observedTimestampSlice{f(1, 1), f(2, 1)},
		},
		{
			observedTimestampSlice{f(2, 1), f(1, 1)},
			observedTimestampSlice{f(1, 1), f(2, 1)},
		},
		{
			observedTimestampSlice{f(1, 1), f(2, 1), f(3, 1)},
			observedTimestampSlice{f(1, 1), f(2, 1), f(3, 1)},
		},
		{
			observedTimestampSlice{f(3, 1), f(2, 1), f(1, 1)},
			observedTimestampSlice{f(1, 1), f(2, 1), f(3, 1)},
		},
		{
			observedTimestampSlice{f(2, 1), f(3, 1), f(1, 1)},
			observedTimestampSlice{f(1, 1), f(2, 1), f(3, 1)},
		},
	}
	for _, c := range testCases {
		t.Run("", func(t *testing.T) {
			var s observedTimestampSlice
			for _, v := range c.input {
				s = s.update(v.NodeID, v.Timestamp)
			}
			if !reflect.DeepEqual(c.expected, s) {
				t.Fatalf("%s", pretty.Diff(c.expected, s))
			}
		})
	}
}

func TestChangeReplicasTrigger_String(t *testing.T) {
	defer leaktest.AfterTest(t)()

	vi := VOTER_INCOMING
	vo := VOTER_OUTGOING
	vd := VOTER_DEMOTING_LEARNER
	l := LEARNER
	repl1 := ReplicaDescriptor{NodeID: 1, StoreID: 2, ReplicaID: 3, Type: &vi}
	repl2 := ReplicaDescriptor{NodeID: 4, StoreID: 5, ReplicaID: 6, Type: &vo}
	learner := ReplicaDescriptor{NodeID: 7, StoreID: 8, ReplicaID: 9, Type: &l}
	repl3 := ReplicaDescriptor{NodeID: 10, StoreID: 11, ReplicaID: 12, Type: &vd}
	crt := ChangeReplicasTrigger{
		InternalAddedReplicas:   []ReplicaDescriptor{repl1},
		InternalRemovedReplicas: []ReplicaDescriptor{repl2, repl3},
		Desc: &RangeDescriptor{
			RangeID:  1,
			StartKey: RKey("a"),
			EndKey:   RKey("b"),
			InternalReplicas: []ReplicaDescriptor{
				repl1,
				repl2,
				learner,
				repl3,
			},
			NextReplicaID: 10,
			Generation:    5,
		},
	}
	act := crt.String()
	exp := "ENTER_JOINT(r6 r12 l12 v3) [(n1,s2):3VOTER_INCOMING], " +
		"[(n4,s5):6VOTER_OUTGOING (n10,s11):12VOTER_DEMOTING_LEARNER]: " +
		"after=[(n1,s2):3VOTER_INCOMING (n4,s5):6VOTER_OUTGOING (n7,s8):9LEARNER " +
		"(n10,s11):12VOTER_DEMOTING_LEARNER] next=10"
	require.Equal(t, exp, act)

	crt.InternalRemovedReplicas = nil
	crt.InternalAddedReplicas = nil
	repl1.Type = ReplicaTypeVoterFull()
	crt.Desc.SetReplicas(MakeReplicaSet([]ReplicaDescriptor{repl1, learner}))
	act = crt.String()
	require.Empty(t, crt.Added())
	require.Empty(t, crt.Removed())
	exp = "LEAVE_JOINT: after=[(n1,s2):3 (n7,s8):9LEARNER] next=10"
	require.Equal(t, exp, act)
}

type mockCRT struct {
	v2 bool
	ChangeReplicasTrigger
}

func (m mockCRT) alwaysV2() bool {
	return m.v2
}

func TestChangeReplicasTrigger_ConfChange(t *testing.T) {
	defer leaktest.AfterTest(t)()

	sl := func(alt ...interface{}) []ReplicaDescriptor {
		t.Helper()
		if len(alt)%2 != 0 {
			t.Fatal("need pairs")
		}
		var rDescs []ReplicaDescriptor
		for i := 0; i < len(alt); i += 2 {
			typ := alt[i].(ReplicaType)
			id := alt[i+1].(int)
			rDescs = append(rDescs, ReplicaDescriptor{
				Type:      &typ,
				NodeID:    NodeID(3 * id),
				StoreID:   StoreID(2 * id),
				ReplicaID: ReplicaID(id),
			})
		}
		return rDescs
	}

	type in struct {
		v2              bool
		add, del, repls []ReplicaDescriptor
	}

	mk := func(in in) mockCRT {
		m := mockCRT{v2: in.v2}
		m.ChangeReplicasTrigger.InternalAddedReplicas = in.add
		m.ChangeReplicasTrigger.InternalRemovedReplicas = in.del
		m.Desc = &RangeDescriptor{}
		m.Desc.SetReplicas(MakeReplicaSet(in.repls))
		return m
	}

	vf1 := sl(VOTER_FULL, 1)
	vo1 := sl(VOTER_OUTGOING, 1)
	vi1 := sl(VOTER_INCOMING, 1)
	vl1 := sl(LEARNER, 1)

	testCases := []struct {
		crt mockCRT
		exp raftpb.ConfChangeI
		err string
	}{
		// A replica of type VOTER_OUTGOING being added makes no sense.
		{crt: mk(in{add: vo1, repls: vo1}), err: "can't add replica in state VOTER_OUTGOING"},
		// But an incoming one can be added, and the result must be a joint change.
		{crt: mk(in{add: vi1, repls: vi1}), exp: raftpb.ConfChangeV2{
			Transition: raftpb.ConfChangeTransitionJointExplicit,
			Changes:    []raftpb.ConfChangeSingle{{Type: raftpb.ConfChangeAddNode, NodeID: 1}},
		}},
		// A replica of type VOTER_INCOMING being removed makes no sense.
		{crt: mk(in{del: vi1}), err: "can't remove replica in state VOTER_INCOMING"},
		// But during a joint removal we can see VOTER_OUTGOING.
		{crt: mk(in{del: vo1, repls: vo1}), exp: raftpb.ConfChangeV2{
			Transition: raftpb.ConfChangeTransitionJointExplicit,
			Changes:    []raftpb.ConfChangeSingle{{Type: raftpb.ConfChangeRemoveNode, NodeID: 1}},
		}},

		// Adding a voter via the V1 path.
		{crt: mk(in{add: vf1, repls: vf1}), exp: raftpb.ConfChange{
			Type:   raftpb.ConfChangeAddNode,
			NodeID: 1,
		}},
		// Adding a learner via the V1 path.
		{crt: mk(in{add: vl1, repls: vl1}), exp: raftpb.ConfChange{
			Type:   raftpb.ConfChangeAddLearnerNode,
			NodeID: 1,
		}},

		// Removing a voter or learner via the V1 path but falsely the replica is still in the descriptor.
		{crt: mk(in{del: vf1, repls: vf1}), err: "(n3,s2):1 must no longer be present in descriptor"},
		{crt: mk(in{del: vl1, repls: vl1}), err: "(n3,s2):1LEARNER must no longer be present in descriptor"},
		// Well-formed examples.
		{crt: mk(in{del: vf1}), exp: raftpb.ConfChange{
			Type:   raftpb.ConfChangeRemoveNode,
			NodeID: 1,
		}},
		{crt: mk(in{del: vl1}), exp: raftpb.ConfChange{
			Type:   raftpb.ConfChangeRemoveNode,
			NodeID: 1,
		}},
		// Adding a voter via the V2 path but without joint consensus.
		{crt: mk(in{v2: true, add: vf1, repls: vf1}), exp: raftpb.ConfChangeV2{
			Transition: raftpb.ConfChangeTransitionAuto,
			Changes: []raftpb.ConfChangeSingle{{
				Type:   raftpb.ConfChangeAddNode,
				NodeID: 1,
			}},
		}},
		// Ditto, but with joint consensus requested.
		{crt: mk(in{v2: true, add: vi1, repls: vi1}), exp: raftpb.ConfChangeV2{
			Transition: raftpb.ConfChangeTransitionJointExplicit,
			Changes: []raftpb.ConfChangeSingle{{
				Type:   raftpb.ConfChangeAddNode,
				NodeID: 1,
			}},
		}},

		// Adding a learner via the V2 path and without joint consensus. (There is currently
		// no way to request joint consensus when adding a single learner, but there is no
		// reason one would ever want that).
		{crt: mk(in{v2: true, add: vl1, repls: vl1}), exp: raftpb.ConfChangeV2{
			Transition: raftpb.ConfChangeTransitionAuto,
			Changes: []raftpb.ConfChangeSingle{{
				Type:   raftpb.ConfChangeAddLearnerNode,
				NodeID: 1,
			}},
		}},

		// Removing a voter or learner via the V2 path without joint consensus.
		// Note that this means that the replica is not in the desc any more.
		{crt: mk(in{v2: true, del: vf1}), exp: raftpb.ConfChangeV2{
			Transition: raftpb.ConfChangeTransitionAuto,
			Changes: []raftpb.ConfChangeSingle{{
				Type:   raftpb.ConfChangeRemoveNode,
				NodeID: 1,
			}},
		}},
		{crt: mk(in{v2: true, del: vl1}), exp: raftpb.ConfChangeV2{
			Transition: raftpb.ConfChangeTransitionAuto,
			Changes: []raftpb.ConfChangeSingle{{
				Type:   raftpb.ConfChangeRemoveNode,
				NodeID: 1,
			}},
		}},

		// Ditto but with joint consensus. (This can happen only with a voter;
		// learners disappear immediately).
		{crt: mk(in{v2: true, del: vo1, repls: vo1}), exp: raftpb.ConfChangeV2{
			Transition: raftpb.ConfChangeTransitionJointExplicit,
			Changes: []raftpb.ConfChangeSingle{{
				Type:   raftpb.ConfChangeRemoveNode,
				NodeID: 1,
			}},
		}},

		// Run a more complex change (necessarily) via the V2 path.
		{crt: mk(in{
			add: sl( // Voter additions.
				VOTER_INCOMING, 6, LEARNER, 4, VOTER_INCOMING, 3,
			),
			del: sl(
				// Voter removals.
				LEARNER, 2, VOTER_OUTGOING, 8, VOTER_DEMOTING_LEARNER, 9,
			),
			repls: sl(
				// Replicas.
				VOTER_FULL, 1,
				VOTER_INCOMING, 6, // added
				VOTER_INCOMING, 3, // added
				VOTER_DEMOTING_LEARNER, 9, // removing
				LEARNER, 4, // added
				VOTER_OUTGOING, 8, // removing
				VOTER_FULL, 10,
			)}),
			exp: raftpb.ConfChangeV2{
				Transition: raftpb.ConfChangeTransitionJointExplicit,
				Changes: []raftpb.ConfChangeSingle{
					{NodeID: 2, Type: raftpb.ConfChangeRemoveNode},
					{NodeID: 8, Type: raftpb.ConfChangeRemoveNode},
					{NodeID: 9, Type: raftpb.ConfChangeRemoveNode},
					{NodeID: 9, Type: raftpb.ConfChangeAddLearnerNode},
					{NodeID: 6, Type: raftpb.ConfChangeAddNode},
					{NodeID: 4, Type: raftpb.ConfChangeAddLearnerNode},
					{NodeID: 3, Type: raftpb.ConfChangeAddNode},
				}},
		},

		// Leave a joint config.
		{
			crt: mk(in{repls: sl(VOTER_FULL, 1)}),
			exp: raftpb.ConfChangeV2{},
		},
		// If we're asked to leave a joint state but the descriptor is still joint,
		// that's a problem.
		{
			crt: mk(in{v2: true, repls: sl(VOTER_INCOMING, 1)}),
			err: "descriptor enters joint state, but trigger is requesting to leave one",
		},
		{
			crt: mk(in{v2: true, repls: sl(VOTER_OUTGOING, 1)}),
			err: "descriptor enters joint state, but trigger is requesting to leave one",
		},
	}

	for _, test := range testCases {
		t.Run("", func(t *testing.T) {
			cc, err := confChangeImpl(test.crt, nil /* payload */)
			if test.err == "" {
				require.NoError(t, err)
				require.Equal(t, test.exp, cc)
			} else {
				require.EqualError(t, err, test.err)
			}
		})
	}
}

// TestAsLockUpdates verifies that txn.LocksAsLockUpdates propagates all the
// important fields from the txn to each intent.
func TestTxnLocksAsLockUpdates(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ts := hlc.Timestamp{WallTime: 1}
	txn := MakeTransaction("hello", Key("k"), 0, ts, 0)

	txn.Status = COMMITTED
	txn.IgnoredSeqNums = []enginepb.IgnoredSeqNumRange{{Start: 0, End: 0}}
	txn.LockSpans = []Span{{Key: Key("a"), EndKey: Key("b")}}
	for _, intent := range txn.LocksAsLockUpdates() {
		require.Equal(t, txn.Status, intent.Status)
		require.Equal(t, txn.IgnoredSeqNums, intent.IgnoredSeqNums)
		require.Equal(t, txn.TxnMeta, intent.Txn)
	}
}

func TestAddIgnoredSeqNumRange(t *testing.T) {
	type r = enginepb.IgnoredSeqNumRange

	mr := func(a, b enginepb.TxnSeq) r {
		return r{Start: a, End: b}
	}

	testData := []struct {
		list     []r
		newRange r
		exp      []r
	}{
		{
			[]r{},
			mr(1, 2),
			[]r{mr(1, 2)},
		},
		{
			[]r{mr(1, 2)},
			mr(1, 4),
			[]r{mr(1, 4)},
		},
		{
			[]r{mr(1, 2), mr(3, 6)},
			mr(8, 10),
			[]r{mr(1, 2), mr(3, 6), mr(8, 10)},
		},
		{
			[]r{mr(1, 2), mr(5, 6)},
			mr(3, 8),
			[]r{mr(1, 2), mr(3, 8)},
		},
		{
			[]r{mr(1, 2), mr(5, 6)},
			mr(1, 8),
			[]r{mr(1, 8)},
		},
	}

	for _, tc := range testData {
		txn := Transaction{
			IgnoredSeqNums: tc.list,
		}
		txn.AddIgnoredSeqNumRange(tc.newRange)
		require.Equal(t, tc.exp, txn.IgnoredSeqNums)
	}
}
