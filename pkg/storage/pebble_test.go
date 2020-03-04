// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble"
)

func TestPebbleTimeBoundPropCollector(t *testing.T) {
	defer leaktest.AfterTest(t)()

	datadriven.RunTest(t, "testdata/time_bound_props", func(t *testing.T, d *datadriven.TestData) string {
		c := &pebbleTimeBoundPropCollector{}
		switch d.Cmd {
		case "build":
			for _, line := range strings.Split(d.Input, "\n") {
				parts := strings.Fields(line)
				if len(parts) != 2 {
					return fmt.Sprintf("malformed line: %s, expected: <key>/<timestamp> <value>", line)
				}
				keyParts := strings.Split(parts[0], "/")
				if len(keyParts) != 2 {
					return fmt.Sprintf("malformed key: %s, expected: <key>/<timestamp>", parts[0])
				}

				key := []byte(keyParts[0])
				timestamp, err := strconv.Atoi(keyParts[1])
				if err != nil {
					return err.Error()
				}
				ikey := pebble.InternalKey{
					UserKey: EncodeKey(MVCCKey{
						Key:       key,
						Timestamp: hlc.Timestamp{WallTime: int64(timestamp)},
					}),
				}

				value := []byte(parts[1])
				if timestamp == 0 {
					if n, err := fmt.Sscanf(string(value), "timestamp=%d", &timestamp); err != nil {
						return err.Error()
					} else if n != 1 {
						return fmt.Sprintf("malformed txn timestamp: %s, expected timestamp=<value>", value)
					}
					meta := &enginepb.MVCCMetadata{}
					meta.Timestamp.WallTime = int64(timestamp)
					meta.Txn = &enginepb.TxnMeta{}
					var err error
					value, err = protoutil.Marshal(meta)
					if err != nil {
						return err.Error()
					}
				}

				if err := c.Add(ikey, value); err != nil {
					return err.Error()
				}
			}

			// Retrieve the properties and sort them for test determinism.
			m := make(map[string]string)
			if err := c.Finish(m); err != nil {
				return err.Error()
			}
			var keys []string
			for k := range m {
				keys = append(keys, k)
			}
			sort.Strings(keys)

			var buf bytes.Buffer
			for _, k := range keys {
				fmt.Fprintf(&buf, "%s: %x\n", k, m[k])
			}
			return buf.String()

		default:
			return fmt.Sprintf("unknown command: %s", d.Cmd)
		}
	})
}

func TestPebbleIterReuse(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// Regression test for https://github.com/cockroachdb/cockroach/issues/42354
	// and similar issues arising from improper re-initialization of cached
	// iterators.

	eng := createTestPebbleEngine()
	defer eng.Close()

	batch := eng.NewBatch()
	for i := 0; i < 100; i++ {
		key := MVCCKey{[]byte{byte(i)}, hlc.Timestamp{WallTime: 100}}
		if err := batch.Put(key, []byte("foo")); err != nil {
			t.Fatal(err)
		}
	}

	iter1 := batch.NewIterator(IterOptions{LowerBound: []byte{40}, UpperBound: []byte{50}})
	valuesCount := 0
	// Seek to a value before the lower bound. Identical to seeking to the lower bound.
	iter1.SeekGE(MVCCKey{Key: []byte{30}})
	for ; ; iter1.Next() {
		ok, err := iter1.Valid()
		if err != nil {
			t.Fatal(err)
		} else if !ok {
			break
		}
		i := iter1.UnsafeKey().Key[0]
		if i < 40 || i >= 50 {
			t.Fatalf("iterator returned key out of bounds: %d", i)
		}

		valuesCount++
	}

	if valuesCount != 10 {
		t.Fatalf("expected 10 values, got %d", valuesCount)
	}
	iter1.Close()

	// Create another iterator, with no lower bound but an upper bound that
	// is lower than the previous iterator's lower bound. This should still result
	// in the right amount of keys being returned; the lower bound from the
	// previous iterator should get zeroed.
	iter2 := batch.NewIterator(IterOptions{UpperBound: []byte{10}})
	valuesCount = 0
	iter1.SeekGE(MVCCKey{Key: []byte{0}})
	for ; ; iter2.Next() {
		ok, err := iter1.Valid()
		if err != nil {
			t.Fatal(err)
		} else if !ok {
			break
		}

		i := iter2.UnsafeKey().Key[0]
		if i >= 10 {
			t.Fatalf("iterator returned key out of bounds: %d", i)
		}
		valuesCount++
	}

	if valuesCount != 10 {
		t.Fatalf("expected 10 values, got %d", valuesCount)
	}
	iter2.Close()
}

func makeMVCCKey(a string) MVCCKey {
	return MVCCKey{Key: []byte(a)}
}

func TestPebbleSeparatorSuccessor(t *testing.T) {
	defer leaktest.AfterTest(t)()

	sepCases := []struct {
		a, b, want MVCCKey
	}{
		// Many cases here are adapted from a Pebble unit test.

		// Non-empty b values.
		{makeMVCCKey("black"), makeMVCCKey("blue"), makeMVCCKey("blb")},
		{makeMVCCKey(""), makeMVCCKey("2"), makeMVCCKey("")},
		{makeMVCCKey("1"), makeMVCCKey("2"), makeMVCCKey("1")},
		{makeMVCCKey("1"), makeMVCCKey("29"), makeMVCCKey("2")},
		{makeMVCCKey("13"), makeMVCCKey("19"), makeMVCCKey("14")},
		{makeMVCCKey("13"), makeMVCCKey("99"), makeMVCCKey("2")},
		{makeMVCCKey("135"), makeMVCCKey("19"), makeMVCCKey("14")},
		{makeMVCCKey("1357"), makeMVCCKey("19"), makeMVCCKey("14")},
		{makeMVCCKey("1357"), makeMVCCKey("2"), makeMVCCKey("14")},
		{makeMVCCKey("13\xff"), makeMVCCKey("14"), makeMVCCKey("13\xff")},
		{makeMVCCKey("13\xff"), makeMVCCKey("19"), makeMVCCKey("14")},
		{makeMVCCKey("1\xff\xff"), makeMVCCKey("19"), makeMVCCKey("1\xff\xff")},
		{makeMVCCKey("1\xff\xff"), makeMVCCKey("2"), makeMVCCKey("1\xff\xff")},
		{makeMVCCKey("1\xff\xff"), makeMVCCKey("9"), makeMVCCKey("2")},
		{makeMVCCKey("1\xfd\xff"), makeMVCCKey("1\xff"), makeMVCCKey("1\xfe")},
		{MVCCKey{
			Key:       []byte("1\xff\xff"),
			Timestamp: hlc.Timestamp{WallTime: 20, Logical: 3},
		}, makeMVCCKey("9"), makeMVCCKey("2")},
		{MVCCKey{
			Key:       []byte("1\xff\xff"),
			Timestamp: hlc.Timestamp{WallTime: 20, Logical: 3},
		}, makeMVCCKey("19"), MVCCKey{
			Key:       []byte("1\xff\xff"),
			Timestamp: hlc.Timestamp{WallTime: 20, Logical: 3},
		},
		},
		// Empty b values.
		{makeMVCCKey(""), makeMVCCKey(""), makeMVCCKey("")},
		{makeMVCCKey("green"), makeMVCCKey(""), makeMVCCKey("green")},
		{makeMVCCKey("1"), makeMVCCKey(""), makeMVCCKey("1")},
		{makeMVCCKey("11\xff"), makeMVCCKey(""), makeMVCCKey("11\xff")},
		{makeMVCCKey("1\xff"), makeMVCCKey(""), makeMVCCKey("1\xff")},
		{makeMVCCKey("1\xff\xff"), makeMVCCKey(""), makeMVCCKey("1\xff\xff")},
		{makeMVCCKey("\xff"), makeMVCCKey(""), makeMVCCKey("\xff")},
		{makeMVCCKey("\xff\xff"), makeMVCCKey(""), makeMVCCKey("\xff\xff")},
	}
	for _, tc := range sepCases {
		t.Run("", func(t *testing.T) {
			got := string(MVCCComparer.Separator(nil, EncodeKey(tc.a), EncodeKey(tc.b)))
			if got != string(EncodeKey(tc.want)) {
				t.Errorf("a, b = %q, %q: got %q, want %q", tc.a, tc.b, got, tc.want)
			}
		})
	}

	succCases := []struct {
		a, want MVCCKey
	}{
		// Many cases adapted from Pebble test.
		{makeMVCCKey("black"), makeMVCCKey("c")},
		{makeMVCCKey("green"), makeMVCCKey("h")},
		{makeMVCCKey(""), makeMVCCKey("")},
		{makeMVCCKey("13"), makeMVCCKey("2")},
		{makeMVCCKey("135"), makeMVCCKey("2")},
		{makeMVCCKey("13\xff"), makeMVCCKey("2")},
		{MVCCKey{
			Key:       []byte("1\xff\xff"),
			Timestamp: hlc.Timestamp{WallTime: 20, Logical: 3},
		}, makeMVCCKey("2")},
		{makeMVCCKey("\xff"), makeMVCCKey("\xff")},
		{makeMVCCKey("\xff\xff"), makeMVCCKey("\xff\xff")},
		{makeMVCCKey("\xff\xff\xff"), makeMVCCKey("\xff\xff\xff")},
		{makeMVCCKey("\xfe\xff\xff"), makeMVCCKey("\xff")},
		{MVCCKey{
			Key:       []byte("\xff\xff"),
			Timestamp: hlc.Timestamp{WallTime: 20, Logical: 3},
		}, MVCCKey{
			Key:       []byte("\xff\xff"),
			Timestamp: hlc.Timestamp{WallTime: 20, Logical: 3},
		}},
	}
	for _, tc := range succCases {
		t.Run("", func(t *testing.T) {
			got := string(MVCCComparer.Successor(nil, EncodeKey(tc.a)))
			if got != string(EncodeKey(tc.want)) {
				t.Errorf("a = %q: got %q, want %q", tc.a, got, tc.want)
			}
		})
	}

}

func BenchmarkMVCCKeyCompare(b *testing.B) {
	rng := rand.New(rand.NewSource(timeutil.Now().Unix()))
	keys := make([][]byte, 1000)
	for i := range keys {
		k := MVCCKey{
			Key: randutil.RandBytes(rng, 8),
			Timestamp: hlc.Timestamp{
				WallTime: int64(rng.Intn(5)),
			},
		}
		keys[i] = EncodeKey(k)
	}

	b.ResetTimer()
	var c int
	for i, j := 0, 0; i < b.N; i, j = i+1, j+3 {
		c = MVCCKeyCompare(keys[i%len(keys)], keys[j%len(keys)])
	}
	if testing.Verbose() {
		fmt.Fprint(ioutil.Discard, c)
	}
}
