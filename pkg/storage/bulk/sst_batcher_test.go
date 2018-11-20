// Copyright 2018 The Cockroach Authors.
//
/// Licensed under the Apache License, Version 2.0 (the "License");
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

package bulk

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestAddBatched(t *testing.T) {
	defer leaktest.AfterTest(t)()
	t.Run("batch=default", func(t *testing.T) {
		runTestImport(t, 32<<20)
	})
	t.Run("batch=1", func(t *testing.T) {
		runTestImport(t, 1)
	})
}

func runTestImport(t *testing.T, batchSize int64) {

	ctx := context.Background()
	s, _, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	const split1, split2 = 3, 5
	// Each test case consists of some number of batches of keys, represented as
	// ints [0, 8). Splits are at 3 and 5.
	for i, testCase := range [][][]int{
		// Simple cases, no spanning splits, try first, last, middle, etc in each.
		// r1
		{{0}},
		{{1}},
		{{2}},
		{{0, 1, 2}},
		{{0}, {1}, {2}},

		// r2
		{{3}},
		{{4}},
		{{3, 4}},
		{{3}, {4}},

		// r3
		{{5}},
		{{5, 6, 7}},
		{{6}},

		// batches exactly matching spans.
		{{0, 1, 2}, {3, 4}, {5, 6, 7}},

		// every key, in its own batch.
		{{0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}},

		// every key in one big batch.
		{{0, 1, 2, 3, 4, 5, 6, 7}},

		// Look for off-by-ones on and around the splits.
		{{2, 3}},
		{{1, 3}},
		{{2, 4}},
		{{1, 4}},
		{{1, 5}},
		{{2, 5}},

		// Mixture of split-aligned and non-aligned batches.
		{{1}, {5}, {6}},
		{{1, 2, 3}, {4, 5}, {6, 7}},
		{{0}, {2, 3, 5}, {7}},
		{{0, 4}, {5, 7}},
		{{0, 3}, {4}},
	} {
		t.Run(fmt.Sprintf("%d-%v", i, testCase), func(t *testing.T) {
			prefix := encoding.EncodeUvarintAscending(keys.MakeTablePrefix(uint32(100+i)), uint64(1))
			key := func(i int) roachpb.Key {
				return encoding.EncodeStringAscending(append([]byte{}, prefix...), fmt.Sprintf("k%d", i))
			}

			if err := kvDB.AdminSplit(ctx, key(split1), key(split1)); err != nil {
				t.Fatal(err)
			}
			if err := kvDB.AdminSplit(ctx, key(split2), key(split2)); err != nil {
				t.Fatal(err)
			}

			ts := hlc.Timestamp{WallTime: 100}
			b, err := MakeFixedTimestampSSTBatcher(ctx, kvDB, batchSize, ts)
			if err != nil {
				t.Fatal(err)
			}

			defer b.Close()

			var expected []client.KeyValue

			for _, batch := range testCase {
				for _, x := range batch {
					k := key(x)
					v := roachpb.MakeValueFromString(fmt.Sprintf("value-%d", x))
					v.Timestamp = ts
					v.InitChecksum(k)
					t.Logf("adding: %v", k)
					if err := b.Add(ctx, k, v.RawBytes); err != nil {
						t.Fatal(err)
					}
					t.Logf("batch: %d", b.sstWriter.DataSize)
					expected = append(expected, client.KeyValue{Key: k, Value: &v})
				}
			}
			if err := b.Flush(ctx); err != nil {
				t.Fatal(err)
			}
			t.Logf("flushed batch: %d", b.sstWriter.DataSize)

			added := b.GetSummary()
			t.Logf("Wrote %d total", added.DataSize)

			got, err := kvDB.Scan(ctx, key(0), key(8), 0)
			if err != nil {
				t.Fatalf("%+v", err)
			}

			if !reflect.DeepEqual(got, expected) {
				for i := 0; i < len(got) || i < len(expected); i++ {
					if i < len(expected) {
						t.Logf("expected %d\t%v\t%v", i, expected[i].Key, expected[i].Value)
					}
					if i < len(got) {
						t.Logf("got      %d\t%v\t%v", i, got[i].Key, got[i].Value)
					}
				}
				t.Fatalf("got %+v expected %+v", got, expected)
			}
		})
	}
}
