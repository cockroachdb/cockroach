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

	"github.com/kr/pretty"

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils/zerofields"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

func makeTS(walltime int64, logical int32) hlc.Timestamp {
	return hlc.Timestamp{
		WallTime: walltime,
		Logical:  logical,
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
		if tc.eq != tc.v1.EqualData(*tc.v2) {
			t.Errorf("%d: wanted eq=%t", i, tc.eq)
		}
		// Test symmetry.
		if tc.eq != tc.v2.EqualData(*tc.v1) {
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
	txn := MakeTransaction("test", Key("a"), 1, enginepb.SERIALIZABLE, origNow, 0)
	// Advance the txn timestamp.
	txn.Timestamp.Add(10, 2)
	txn.BumpEpoch()
	if a, e := txn.Epoch, uint32(1); a != e {
		t.Errorf("expected epoch %d; got %d", e, a)
	}
	if txn.EpochZeroTimestamp == (hlc.Timestamp{}) {
		t.Errorf("expected non-nil epoch zero timestamp")
	} else if txn.EpochZeroTimestamp != origNow {
		t.Errorf("expected zero timestamp == origNow; %s != %s", txn.EpochZeroTimestamp, origNow)
	}
}

func TestTransactionInclusiveTimeBounds(t *testing.T) {
	verify := func(txn Transaction, expMin, expMax hlc.Timestamp) {
		if min, max := txn.InclusiveTimeBounds(); min != expMin || max != expMax {
			t.Errorf("expected (%s-%s); got (%s-%s)", expMin, expMax, min, max)
		}
	}
	origNow := makeTS(1, 1)
	txn := MakeTransaction("test", Key("a"), 1, enginepb.SERIALIZABLE, origNow, 0)
	verify(txn, origNow, origNow)
	txn.Timestamp.Forward(makeTS(1, 2))
	verify(txn, origNow, makeTS(1, 2))
	txn.Restart(1, 1, makeTS(2, 1))
	verify(txn, origNow, makeTS(2, 1))
	txn.Timestamp.Forward(makeTS(3, 1))
	verify(txn, origNow, makeTS(3, 1))
}

// TestTransactionObservedTimestamp verifies that txn.{Get,Update}ObservedTimestamp work as
// advertised.
func TestTransactionObservedTimestamp(t *testing.T) {
	var txn Transaction
	rng, seed := randutil.NewPseudoRand()
	t.Logf("running with seed %d", seed)
	ids := append([]int{109, 104, 102, 108, 1000}, rand.Perm(100)...)
	timestamps := make(map[NodeID]hlc.Timestamp, len(ids))
	for i := 0; i < len(ids); i++ {
		timestamps[NodeID(i)] = hlc.Timestamp{WallTime: rng.Int63()}
	}
	for i, n := range ids {
		nodeID := NodeID(n)
		if ts, ok := txn.GetObservedTimestamp(nodeID); ok {
			t.Fatalf("%d: false positive hit %s in %v", nodeID, ts, ids[:i+1])
		}
		txn.UpdateObservedTimestamp(nodeID, timestamps[nodeID])
		txn.UpdateObservedTimestamp(nodeID, hlc.MaxTimestamp) // should be noop
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
	ts := hlc.Timestamp{WallTime: 1, Logical: 2}
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
	expTS := hlc.Timestamp{WallTime: 10}
	txn.UpdateObservedTimestamp(nodeID, expTS)
	if ts, ok := txn.GetObservedTimestamp(nodeID); !ok || !ts.Equal(expTS) {
		t.Errorf("expected %s; got %s", expTS, ts)
	}
	expTS = hlc.Timestamp{WallTime: 9}
	txn.UpdateObservedTimestamp(nodeID, expTS)
	if ts, ok := txn.GetObservedTimestamp(nodeID); !ok || !ts.Equal(expTS) {
		t.Errorf("expected %s; got %s", expTS, ts)
	}
}

var nonZeroTxn = Transaction{
	TxnMeta: enginepb.TxnMeta{
		Isolation:  enginepb.SNAPSHOT,
		Key:        Key("foo"),
		ID:         uuid.MakeV4(),
		Epoch:      2,
		Timestamp:  makeTS(20, 21),
		Priority:   957356782,
		Sequence:   123,
		BatchIndex: 1,
	},
	Name:                     "name",
	Status:                   COMMITTED,
	LastHeartbeat:            makeTS(1, 2),
	OrigTimestamp:            makeTS(30, 31),
	RefreshedTimestamp:       makeTS(20, 22),
	MaxTimestamp:             makeTS(40, 41),
	ObservedTimestamps:       []ObservedTimestamp{{NodeID: 1, Timestamp: makeTS(1, 2)}},
	Writing:                  true,
	WriteTooOld:              true,
	RetryOnPush:              true,
	Intents:                  []Span{{Key: []byte("a"), EndKey: []byte("b")}},
	EpochZeroTimestamp:       makeTS(1, 1),
	OrigTimestampWasObserved: true,
}

func TestTransactionUpdate(t *testing.T) {
	txn := nonZeroTxn
	if err := zerofields.NoZeroField(txn); err != nil {
		t.Fatal(err)
	}

	var txn2 Transaction
	txn2.Update(&txn)

	if err := zerofields.NoZeroField(txn2); err != nil {
		t.Fatal(err)
	}

	var txn3 Transaction
	txn3.ID = uuid.MakeV4()
	txn3.Name = "carl"
	txn3.Isolation = enginepb.SNAPSHOT
	txn3.Update(&txn)

	if err := zerofields.NoZeroField(txn3); err != nil {
		t.Fatal(err)
	}
}

func TestTransactionUpdateEpochZero(t *testing.T) {
	txn := nonZeroTxn
	var txn2 Transaction
	txn2.Update(&txn)

	if a, e := txn2.EpochZeroTimestamp, txn.EpochZeroTimestamp; a != e {
		t.Errorf("expected epoch zero %s; got %s", e, a)
	}

	txn3 := nonZeroTxn
	txn3.EpochZeroTimestamp = nonZeroTxn.EpochZeroTimestamp.Prev()
	txn.Update(&txn3)

	if a, e := txn.EpochZeroTimestamp, txn3.EpochZeroTimestamp; a != e {
		t.Errorf("expected epoch zero %s; got %s", e, a)
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
	// Verify min & max.
	if a, e := MakePriority(MinUserPriority), int32(MinTxnPriority); a != e {
		t.Errorf("expected min txn priority %d; got %d", e, a)
	}
	if a, e := MakePriority(MaxUserPriority), int32(MaxTxnPriority); a != e {
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
		expected := int32(MinTxnPriority)
		if userPri > 1 {
			expected = int32(MaxTxnPriority)
		}
		if actual := MakePriority(userPri); actual != expected {
			t.Errorf("%f: expected txn priority %d; got %d", userPri, expected, actual)
		}
	}
}

func TestLeaseEquivalence(t *testing.T) {
	r1 := ReplicaDescriptor{NodeID: 1, StoreID: 1, ReplicaID: 1}
	r2 := ReplicaDescriptor{NodeID: 2, StoreID: 2, ReplicaID: 2}
	ts1 := makeTS(1, 1)
	ts2 := makeTS(2, 1)
	ts3 := makeTS(3, 1)

	epoch1 := Lease{Replica: r1, Start: ts1, Epoch: 1}
	epoch2 := Lease{Replica: r1, Start: ts1, Epoch: 2}
	expire1 := Lease{Replica: r1, Start: ts1, Expiration: ts2.Clone()}
	expire2 := Lease{Replica: r1, Start: ts1, Expiration: ts3.Clone()}
	epoch2TS2 := Lease{Replica: r2, Start: ts2, Epoch: 2}
	expire2TS2 := Lease{Replica: r2, Start: ts2, Expiration: ts3.Clone()}

	proposed1 := Lease{Replica: r1, Start: ts1, Epoch: 1, ProposedTS: ts1.Clone()}
	proposed2 := Lease{Replica: r1, Start: ts1, Epoch: 2, ProposedTS: ts1.Clone()}
	proposed3 := Lease{Replica: r1, Start: ts1, Epoch: 1, ProposedTS: ts2.Clone()}

	stasis1 := Lease{Replica: r1, Start: ts1, Epoch: 1, DeprecatedStartStasis: ts1.Clone()}
	stasis2 := Lease{Replica: r1, Start: ts1, Epoch: 1, DeprecatedStartStasis: ts2.Clone()}

	testCases := []struct {
		l, ol      Lease
		expSuccess bool
	}{
		{epoch1, epoch1, true},        // same epoch lease
		{expire1, expire1, true},      // same expiration lease
		{epoch1, epoch2, false},       // different epoch leases
		{epoch1, epoch2TS2, false},    // different epoch leases
		{expire1, expire2TS2, false},  // different expiration leases
		{expire1, expire2, true},      // same expiration lease, extended
		{expire2, expire1, false},     // same expiration lease, extended but backwards
		{epoch1, expire1, false},      // epoch and expiration leases
		{expire1, epoch1, false},      // expiration and epoch leases
		{proposed1, proposed1, true},  // exact leases with identical timestamps
		{proposed1, proposed2, false}, // same proposed timestamps, but diff epochs
		{proposed1, proposed3, true},  // different proposed timestamps, same lease
		{stasis1, stasis2, true},      // same lease, different stasis timestamps
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
		Start: hlc.Timestamp{WallTime: 10},
		Epoch: 123,

		// The bug-trigger.
		Expiration: new(hlc.Timestamp),

		// Similar potential bug triggers, but these were actually handled correctly.
		DeprecatedStartStasis: new(hlc.Timestamp),
		ProposedTS:            &hlc.Timestamp{WallTime: 10},
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
		Start                 hlc.Timestamp
		Expiration            *hlc.Timestamp
		Replica               ReplicaDescriptor
		DeprecatedStartStasis *hlc.Timestamp
		ProposedTS            *hlc.Timestamp
		Epoch                 int64
		Sequence              LeaseSequence
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

	ts := hlc.Timestamp{Logical: 1}
	testCases := []Lease{
		{Start: ts},
		{Expiration: &ts},
		{Replica: ReplicaDescriptor{NodeID: 1}},
		{DeprecatedStartStasis: &ts},
		{ProposedTS: &ts},
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

func TestLeaseFuzzNullability(t *testing.T) {
	var l Lease
	protoutil.Walk(&l, protoutil.ZeroInsertingVisitor)
	if l.Expiration == nil {
		t.Fatal("unexpectedly nil expiration")
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
		if combined := test.s1.Combine(test.s2); !combined.Equal(test.combined) {
			t.Errorf("%d: expected combined %s; got %s between %s vs. %s", i, test.combined, combined, test.s1, test.s2)
		}
	}
}

// TestSpanContains verifies methods to check whether a key
// or key range is contained within the span.
func TestSpanContains(t *testing.T) {
	s := Span{Key: []byte("a"), EndKey: []byte("b")}

	testData := []struct {
		start, end []byte
		contains   bool
	}{
		// Single keys.
		{[]byte("a"), nil, true},
		{[]byte("aa"), nil, true},
		{[]byte("`"), nil, false},
		{[]byte("b"), nil, false},
		{[]byte("c"), nil, false},
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
		if s.Contains(Span{test.start, test.end}) != test.contains {
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
			Span{[]byte("b"), []byte("bb")},
			Span{[]byte("bb"), []byte("c")},
		},
	}
	for testIdx, test := range testData {
		t.Run(strconv.Itoa(testIdx), func(t *testing.T) {
			actualL, actualR := s.SplitOnKey(test.split)
			if !test.left.EqualValue(actualL) {
				t.Fatalf("expected left span after split to be %v, got %v", test.left, actualL)
			}

			if !test.right.EqualValue(actualR) {
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
		s := Span{test.start, test.end}
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
	_ = durationValue.SetDuration(duration.Duration{Months: 1, Days: 2, Nanos: 3})

	var tupleValue Value
	tupleBytes := encoding.EncodeBytesValue(encoding.EncodeIntValue(nil, 1, 8), 2, []byte("foo"))
	tupleValue.SetTuple(tupleBytes)

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
		{durationValue, "/DURATION/1mon2d3ns"},
		{MakeValueFromBytes([]byte{0x1, 0x2, 0xF, 0xFF}), "/BYTES/01020fff"},
		{MakeValueFromString("foo"), "/BYTES/foo"},
		{tupleValue, "/TUPLE/1:1:Int/8/2:3:Bytes/foo"},
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
			Timestamp: hlc.Timestamp{
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
