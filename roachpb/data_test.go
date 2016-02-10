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
	"math"
	"math/rand"
	"reflect"
	"sort"
	"testing"
	"time"

	"gopkg.in/inf.v0"

	"github.com/cockroachdb/cockroach/util"
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

	testCases := []struct {
		key  Key
		next Key
	}{
		{nil, Key("\x00")},
		{Key(""), Key("\x00")},
		{Key("test key"), Key("test key\x00")},
		{Key("\xff"), Key("\xff\x00")},
		{Key("xoxo\x00"), Key("xoxo\x00\x00")},
	}
	for i, c := range testCases {
		if !bytes.Equal(c.key.Next(), c.next) {
			t.Errorf("%d: unexpected next bytes for %q: %q", i, c.key, c.key.Next())
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
		{&Transaction{TxnMeta: TxnMeta{ID: []byte("A")}}, &Transaction{TxnMeta: TxnMeta{ID: []byte("B")}}, false},
	}
	for i, c := range tc {
		if c.txn1.Equal(c.txn2) != c.txn2.Equal(c.txn1) || c.txn1.Equal(c.txn2) != c.eq {
			t.Errorf("%d: wanted %t", i, c.eq)
		}
	}
}

func TestTxnIDEqual(t *testing.T) {
	txn1, txn2 := uuid.NewUUID4(), uuid.NewUUID4()
	txn1Copy := append([]byte(nil), txn1...)

	testCases := []struct {
		a, b     []byte
		expEqual bool
	}{
		{txn1, txn1, true},
		{txn1, txn2, false},
		{txn1, txn1Copy, true},
	}
	for i, test := range testCases {
		if eq := TxnIDEqual(test.a, test.b); eq != test.expEqual {
			t.Errorf("%d: expected %q == %q: %t; got %t", i, test.a, test.b, test.expEqual, eq)
		}
	}
}

func TestTransactionString(t *testing.T) {
	id := []byte("ת\x0f^\xe4-Fؽ\xf7\x16\xe4\xf9\xbe^\xbe")
	ts1 := makeTS(10, 11)
	txn := Transaction{
		TxnMeta: TxnMeta{
			Key:       Key("foo"),
			ID:        id,
			Epoch:     2,
			Timestamp: makeTS(20, 21),
		},
		Name:          "name",
		Priority:      957356782,
		Isolation:     SERIALIZABLE,
		Status:        COMMITTED,
		LastHeartbeat: &ts1,
		OrigTimestamp: makeTS(30, 31),
		MaxTimestamp:  makeTS(40, 41),
	}
	expStr := `"name" id=d7aa0f5e key="foo" rw=false pri=44.58039917 iso=SERIALIZABLE stat=COMMITTED ` +
		`epo=2 ts=0.000000020,21 orig=0.000000030,31 max=0.000000040,41`

	if str := txn.String(); str != expStr {
		t.Errorf("expected txn %s; got %s", expStr, str)
	}
}

// TestNodeList verifies that its exported methods Add() and Contain()
// operate as expected.
func TestNodeList(t *testing.T) {
	sn := NodeList{}
	items := append([]int{109, 104, 102, 108, 1000}, rand.Perm(100)...)
	for i := range items {
		n := NodeID(items[i])
		if sn.Contains(n) {
			t.Fatalf("%d: false positive hit for %d on slice %v",
				i, n, sn.Nodes)
		}
		// Add this item and, for good measure, all the previous ones.
		for j := i; j >= 0; j-- {
			sn.Add(NodeID(items[j]))
		}
		if nodes := sn.Nodes; len(nodes) != i+1 {
			t.Fatalf("%d: missing values or duplicates: %v",
				i, nodes)
		}
		if !sn.Contains(n) {
			t.Fatalf("%d: false negative hit for %d on slice %v",
				i, n, sn.Nodes)
		}
	}
}

var ts = makeTS(10, 11)
var nonZeroTxn = Transaction{
	TxnMeta: TxnMeta{
		Key:       Key("foo"),
		ID:        uuid.NewUUID4(),
		Epoch:     2,
		Timestamp: makeTS(20, 21),
	},
	Name:          "name",
	Priority:      957356782,
	Isolation:     SNAPSHOT,
	Status:        COMMITTED,
	LastHeartbeat: &Timestamp{1, 2},
	OrigTimestamp: makeTS(30, 31),
	MaxTimestamp:  makeTS(40, 41),
	CertainNodes: NodeList{
		Nodes: []NodeID{101, 103, 105},
	},
	Writing:  true,
	Sequence: 123,
	Intents:  []Span{{Key: []byte("a"), EndKey: []byte("b")}},
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
	txn3.ID = uuid.NewUUID4()
	txn3.Name = "carl"
	txn3.Isolation = SNAPSHOT
	txn3.Update(&txn)

	if err := util.NoZeroField(txn3); err != nil {
		t.Fatal(err)
	}
}

func equalPtrFields(src, dst reflect.Value, prefix string) []string {
	t := dst.Type()
	if t.Kind() != reflect.Struct {
		return nil
	}
	if srcType := src.Type(); srcType != t {
		return nil
	}
	var res []string
	for i := 0; i < t.NumField(); i++ {
		srcF, dstF := src.Field(i), dst.Field(i)
		switch f := t.Field(i); f.Type.Kind() {
		case reflect.Ptr:
			if srcF.Interface() == dstF.Interface() {
				res = append(res, prefix+f.Name)
			}
		case reflect.Slice:
			if srcF.Pointer() == dstF.Pointer() {
				res = append(res, prefix+f.Name)
			}
			l := dstF.Len()
			if srcLen := srcF.Len(); srcLen < l {
				l = srcLen
			}
			for i := 0; i < l; i++ {
				res = append(res, equalPtrFields(srcF.Index(i), dstF.Index(i), f.Name+".")...)
			}
		case reflect.Struct:
			res = append(res, equalPtrFields(srcF, dstF, f.Name+".")...)
		}
	}
	return res
}

func TestTransactionClone(t *testing.T) {
	txn := nonZeroTxn.Clone()

	fields := equalPtrFields(reflect.ValueOf(nonZeroTxn), reflect.ValueOf(txn), "")
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

func TestTransactionIDLen(t *testing.T) {
	if l := len(nonZeroTxn.ID); l != TransactionIDLen {
		t.Fatalf("expected %d, got %d", TransactionIDLen, l)
	}
}
