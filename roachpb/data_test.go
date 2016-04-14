// Copyright 2014 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package roachpb

import (
	"bytes"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"gopkg.in/inf.v0"

	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/randutil"
	"github.com/cockroachdb/cockroach/util/uuid"
)

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

	noExtraCap := make([]byte, 2, 2)
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
		{Key{0x00, 0xff}, Key{0x01, 0x00}},
		{Key{0x00, 0xff, 0xff}, Key{0x01, 0x00, 0x00}},
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

func TestKeyString(t *testing.T) {
	if Key("hello").String() != `"hello"` {
		t.Errorf("expected key to display pretty version: %s", Key("hello"))
	}
	if RKeyMax.String() != `"\xff\xff"` {
		t.Errorf("expected key max to display pretty version: %s", RKeyMax)
	}
}

func makeTS(walltime int64, logical int32) Timestamp {
	return Timestamp{
		WallTime: walltime,
		Logical:  logical,
	}
}

func TestLess(t *testing.T) {
	a := Timestamp{}
	b := Timestamp{}
	if a.Less(b) || b.Less(a) {
		t.Errorf("expected %+v == %+v", a, b)
	}
	b = makeTS(1, 0)
	if !a.Less(b) {
		t.Errorf("expected %+v < %+v", a, b)
	}
	a = makeTS(1, 1)
	if !b.Less(a) {
		t.Errorf("expected %+v < %+v", b, a)
	}
}

func TestEqual(t *testing.T) {
	a := Timestamp{}
	b := Timestamp{}
	if !a.Equal(b) {
		t.Errorf("expected %+v == %+v", a, b)
	}
	b = makeTS(1, 0)
	if a.Equal(b) {
		t.Errorf("expected %+v < %+v", a, b)
	}
	a = makeTS(1, 1)
	if b.Equal(a) {
		t.Errorf("expected %+v < %+v", b, a)
	}
}

func TestTimestampNext(t *testing.T) {
	testCases := []struct {
		ts, expNext Timestamp
	}{
		{makeTS(1, 2), makeTS(1, 3)},
		{makeTS(1, math.MaxInt32-1), makeTS(1, math.MaxInt32)},
		{makeTS(1, math.MaxInt32), makeTS(2, 0)},
		{makeTS(math.MaxInt32, math.MaxInt32), makeTS(math.MaxInt32+1, 0)},
	}
	for i, c := range testCases {
		if next := c.ts.Next(); !next.Equal(c.expNext) {
			t.Errorf("%d: expected %s; got %s", i, c.expNext, next)
		}
	}
}

func TestTimestampPrev(t *testing.T) {
	testCases := []struct {
		ts, expPrev Timestamp
	}{
		{makeTS(1, 2), makeTS(1, 1)},
		{makeTS(1, 1), makeTS(1, 0)},
		{makeTS(1, 0), makeTS(0, math.MaxInt32)},
	}
	for i, c := range testCases {
		if prev := c.ts.Prev(); !prev.Equal(c.expPrev) {
			t.Errorf("%d: expected %s; got %s", i, c.expPrev, prev)
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

	i := int64(1)
	v.SetInt(i)
	if r, err := v.GetInt(); err != nil {
		t.Fatal(err)
	} else if i != r {
		t.Errorf("set %d on a value and extracted it, expected %d back, but got %d", i, i, r)
	}

	dec := inf.NewDec(11, 1)
	if err := v.SetDecimal(dec); err != nil {
		t.Fatal(err)
	}
	if r, err := v.GetDecimal(); err != nil {
		t.Fatal(err)
	} else if dec.Cmp(r) != 0 {
		t.Errorf("set %s on a value and extracted it, expected %s back, but got %s", dec, dec, r)
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

func TestTxnEqual(t *testing.T) {
	tc := []struct {
		txn1, txn2 *Transaction
		eq         bool
	}{
		{nil, nil, true},
		{&Transaction{}, nil, false},
		{&Transaction{TxnMeta: TxnMeta{ID: uuid.NewV4()}}, &Transaction{TxnMeta: TxnMeta{ID: uuid.NewV4()}}, false},
	}
	for i, c := range tc {
		if c.txn1.Equal(c.txn2) != c.txn2.Equal(c.txn1) || c.txn1.Equal(c.txn2) != c.eq {
			t.Errorf("%d: wanted %t", i, c.eq)
		}
	}
}

func TestTxnIDEqual(t *testing.T) {
	txn1, txn2 := uuid.NewV4(), uuid.NewV4()
	txn1Copy := *txn1

	testCases := []struct {
		a, b     *uuid.UUID
		expEqual bool
	}{
		{txn1, txn1, true},
		{txn1, txn2, false},
		{txn1, &txn1Copy, true},
	}
	for i, test := range testCases {
		if eq := TxnIDEqual(test.a, test.b); eq != test.expEqual {
			t.Errorf("%d: expected %q == %q: %t; got %t", i, test.a, test.b, test.expEqual, eq)
		}
	}
}

func TestTransactionString(t *testing.T) {
	txnID, err := uuid.FromBytes([]byte("ת\x0f^\xe4-Fؽ\xf7\x16\xe4\xf9\xbe^\xbe"))
	if err != nil {
		t.Fatal(err)
	}
	ts1 := makeTS(10, 11)
	txn := Transaction{
		TxnMeta: TxnMeta{
			Isolation: SERIALIZABLE,
			Key:       Key("foo"),
			ID:        txnID,
			Epoch:     2,
			Timestamp: makeTS(20, 21),
			Priority:  957356782,
		},
		Name:          "name",
		Status:        COMMITTED,
		LastHeartbeat: &ts1,
		OrigTimestamp: makeTS(30, 31),
		MaxTimestamp:  makeTS(40, 41),
	}
	expStr := `"name" id=d7aa0f5e key="foo" rw=false pri=44.58039917 iso=SERIALIZABLE stat=COMMITTED ` +
		`epo=2 ts=0.000000020,21 orig=0.000000030,31 max=0.000000040,41 wto=false`

	if str := txn.String(); str != expStr {
		t.Errorf("expected txn %s; got %s", expStr, str)
	}

	var txnEmpty Transaction
	_ = txnEmpty.String() // prevent regression of NPE

	var cmd RaftCommand
	cmd.Cmd.Txn = &txn
	if actStr, idStr := fmt.Sprintf("%s", &cmd), txn.ID.String(); !strings.Contains(actStr, idStr) {
		t.Fatalf("expected to find '%s' in '%s'", idStr, actStr)
	}
}

// TestTransactionObservedTimestamp verifies that txn.{Get,Update}ObservedTimestamp work as
// advertised.
func TestTransactionObservedTimestamp(t *testing.T) {
	var txn Transaction
	rng, seed := randutil.NewPseudoRand()
	t.Logf("running with seed %d", seed)
	ids := append([]int{109, 104, 102, 108, 1000}, rand.Perm(100)...)
	timestamps := make(map[NodeID]Timestamp, len(ids))
	for i := 0; i < len(ids); i++ {
		timestamps[NodeID(i)] = ZeroTimestamp.Add(rng.Int63(), 0)
	}
	for i, n := range ids {
		nodeID := NodeID(n)
		if ts, ok := txn.GetObservedTimestamp(nodeID); ok {
			t.Fatalf("%d: false positive hit %s in %v", nodeID, ts, ids[:i+1])
		}
		txn.UpdateObservedTimestamp(nodeID, timestamps[nodeID])
		txn.UpdateObservedTimestamp(nodeID, MaxTimestamp) // should be noop
		if exp, act := i+1, len(txn.ObservedTimestamps); act != exp {
			t.Fatalf("%d: expected %d entries, got %d: %v", nodeID, exp, act, txn.ObservedTimestamps)
		}
	}
	for _, m := range ids {
		checkID := NodeID(m)
		exp := timestamps[checkID]
		if act, _ := txn.GetObservedTimestamp(checkID); !act.Equal(exp) {
			t.Fatalf("%d: expected %s, got %s", checkID, exp, act)
		}
	}

	var emptyTxn Transaction
	ts := ZeroTimestamp.Add(1, 2)
	emptyTxn.UpdateObservedTimestamp(NodeID(1), ts)
	if actTS, _ := emptyTxn.GetObservedTimestamp(NodeID(1)); !actTS.Equal(ts) {
		t.Fatalf("unexpected: %s (wanted %s)", actTS, ts)
	}
}

var nonZeroTxn = Transaction{
	TxnMeta: TxnMeta{
		Isolation:  SNAPSHOT,
		Key:        Key("foo"),
		ID:         uuid.NewV4(),
		Epoch:      2,
		Timestamp:  makeTS(20, 21),
		Priority:   957356782,
		Sequence:   123,
		BatchIndex: 1,
	},
	Name:               "name",
	Status:             COMMITTED,
	LastHeartbeat:      &Timestamp{1, 2},
	OrigTimestamp:      makeTS(30, 31),
	MaxTimestamp:       makeTS(40, 41),
	ObservedTimestamps: map[NodeID]Timestamp{1: makeTS(1, 2)},
	Writing:            true,
	WriteTooOld:        true,
	Intents:            []Span{{Key: []byte("a"), EndKey: []byte("b")}},
}

func TestTransactionUpdate(t *testing.T) {
	txn := nonZeroTxn
	if err := util.NoZeroField(txn); err != nil {
		t.Fatal(err)
	}

	var txn2 Transaction
	txn2.Update(&txn)

	if err := util.NoZeroField(txn2); err != nil {
		t.Fatal(err)
	}

	var txn3 Transaction
	txn3.ID = uuid.NewV4()
	txn3.Name = "carl"
	txn3.Isolation = SNAPSHOT
	txn3.Update(&txn)

	if err := util.NoZeroField(txn3); err != nil {
		t.Fatal(err)
	}
}

func TestTransactionClone(t *testing.T) {
	txn := nonZeroTxn.Clone()

	fields := util.EqualPtrFields(reflect.ValueOf(nonZeroTxn), reflect.ValueOf(txn), "")
	sort.Strings(fields)

	// Verify that the only equal pointer fields after cloning are the ones
	// listed below. If this test fails, please update the list below and/or
	// Transaction.Clone().
	expFields := []string{
		"Intents.EndKey",
		"Intents.Key",
		"TxnMeta.ID",
		"TxnMeta.Key",
	}
	if !reflect.DeepEqual(expFields, fields) {
		t.Fatalf("%s != %s", expFields, fields)
	}
	if !reflect.DeepEqual(nonZeroTxn, txn) {
		t.Fatalf("e = %v, v = %v", nonZeroTxn, txn)
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
	userPs := []UserPriority{
		0.001,
		0.01,
		0.1,
		0.5,
		0, // Same as 1.0 (special cased below)
		1.0,
		2.0,
		10.0,
		100.0,
		1000.0,
	}

	// Generate values for all priorities
	const trials = 100000
	values := make([][trials]int32, len(userPs))
	for i, userPri := range userPs {
		for t := 0; t < trials; t++ {
			values[i][t] = MakePriority(userPri)
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
		expPri  int32
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
// enforced and still yield randomized values.
func TestMakePriorityLimits(t *testing.T) {
	userPs := []UserPriority{
		0.000000001,
		0.00001,
		0.00009,
		10001,
		100000,
		math.MaxFloat64,
	}
	const trials = 100
	for _, userPri := range userPs {
		seen := map[int32]struct{}{} // set of priorities
		for j := 0; j < trials; j++ {
			seen[MakePriority(userPri)] = struct{}{}
		}
		if len(seen) < 85 {
			t.Errorf("%f: expected randomized values, got %d: %v", userPri, len(seen), seen)
		}
	}
}

func TestSpanOverlaps(t *testing.T) {
	sA := Span{Key: []byte("a")}
	sD := Span{Key: []byte("d")}
	sAtoC := Span{Key: []byte("a"), EndKey: []byte("c")}
	sBtoD := Span{Key: []byte("b"), EndKey: []byte("d")}

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
	}
	for i, test := range testData {
		if o := test.s1.Overlaps(test.s2); o != test.overlaps {
			t.Errorf("%d: expected overlap %t; got %t between %s vs. %s", i, test.overlaps, o, test.s1, test.s2)
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
		if bytes.Compare(test.start, test.end) == 0 {
			if rs.ContainsKey(test.start) != test.contains {
				t.Errorf("%d: expected key %q within range", i, test.start)
			}
		}
		if rs.ContainsKeyRange(test.start, test.end) != test.contains {
			t.Errorf("%d: expected key %q within range", i, test.start)
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
		if bytes.Compare(actual.Key, test.expected.Key) != 0 ||
			bytes.Compare(actual.EndKey, test.expected.EndKey) != 0 {
			t.Errorf("%d: expected RSpan [%q,%q) but got [%q,%q)",
				i, test.expected.Key, test.expected.EndKey,
				actual.Key, actual.EndKey)
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

func TestLeaseCovers(t *testing.T) {
	mk := func(ds ...int64) (sl []Timestamp) {
		for _, d := range ds {
			sl = append(sl, ZeroTimestamp.Add(d, 0))
		}
		return sl
	}

	ts10 := mk(10)[0]
	ts1K := mk(1000)[0]

	for i, test := range []struct {
		lease   Lease
		in, out []Timestamp
	}{
		{
			lease: Lease{
				StartStasis: mk(1)[0],
				Expiration:  ts1K,
			},
			in:  mk(0),
			out: mk(1, 100, 500, 999, 1000),
		},
		{
			lease: Lease{
				Start:       ts10,
				StartStasis: mk(500)[0],
				Expiration:  ts1K,
			},
			out: mk(500, 999, 1000, 1001, 2000),
			// Note that the lease covers timestamps before its start timestamp.
			in: mk(0, 9, 10, 300, 499),
		},
	} {
		for _, ts := range test.in {
			if !test.lease.Covers(ts) {
				t.Errorf("%d: should contain %s", i, ts)
			}
		}
		for _, ts := range test.out {
			if test.lease.Covers(ts) {
				t.Errorf("%d: must not contain %s", i, ts)
			}
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
	dec := inf.NewDec(11, 1)

	for i := 0; i < b.N; i++ {
		if err := v.SetDecimal(dec); err != nil {
			b.Fatal(err)
		}
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
	dec := inf.NewDec(11, 1)
	if err := v.SetDecimal(dec); err != nil {
		b.Fatal(err)
	}

	for i := 0; i < b.N; i++ {
		if _, err := v.GetDecimal(); err != nil {
			b.Fatal(err)
		}
	}
}
