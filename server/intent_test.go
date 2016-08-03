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
// Author: Tobias Schottdorf

package server

import (
	"fmt"
	"reflect"
	"sort"
	"testing"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/internal/client"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/storage/storagebase"
	"github.com/cockroachdb/cockroach/testutils/serverutils"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/randutil"
	"github.com/cockroachdb/cockroach/util/syncutil"
)

func TestIntentResolution(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		keys   []string
		ranges [][2]string
		exp    []string
	}{
		// Note that the first key (or, range, if no keys present) determines
		// the base key of the Txn. In these examples, it's always the first
		// range, so "a"-"s" is local. Any examples added must stick to that
		// convention and write the first key into "a"-"s".

		{
			keys:   []string{"a", "x", "b", "c", "s"},
			ranges: [][2]string{{"d", "e"}},
			exp:    []string{"s", "x"},
		},
		{
			keys:   []string{"h", "y", "z"},
			ranges: [][2]string{{"g", "z"}},
			exp:    []string{`"s"-"z\x00"`},
		},
		{
			keys:   []string{"q", "s"},
			ranges: [][2]string{{"a", "w"}, {"b", "x"}, {"t", "u"}},
			exp:    []string{`"s"-"x"`},
		},
		{
			keys:   []string{"q", "s", "y", "v"},
			ranges: [][2]string{{"a", "s"}, {"r", "t"}, {"u", "w"}},
			exp:    []string{`"s"-"t"`, `"u"-"w"`, "y"},
		},
	}

	splitKey := []byte("s")
	for i, tc := range testCases {
		// Use deterministic randomness to randomly put the writes in separate
		// batches or commit them with EndTransaction.
		rnd, seed := randutil.NewPseudoRand()
		log.Infof(context.Background(), "%d: using intent test seed %d", i, seed)

		results := map[string]struct{}{}
		func() {
			var storeKnobs storage.StoreTestingKnobs
			var mu syncutil.Mutex
			closer := make(chan struct{}, 2)
			var done bool
			storeKnobs.TestingCommandFilter =
				func(filterArgs storagebase.FilterArgs) *roachpb.Error {
					mu.Lock()
					defer mu.Unlock()
					header := filterArgs.Req.Header()
					// Ignore anything outside of the intent key range of "a" - "z"
					if header.Key.Compare(roachpb.Key("a")) < 0 || header.Key.Compare(roachpb.Key("z")) > 0 {
						return nil
					}
					var entry string
					switch arg := filterArgs.Req.(type) {
					case *roachpb.ResolveIntentRequest:
						if arg.Status == roachpb.COMMITTED {
							entry = string(header.Key)
						}
					case *roachpb.ResolveIntentRangeRequest:
						if arg.Status == roachpb.COMMITTED {
							entry = fmt.Sprintf("%s-%s", header.Key, header.EndKey)
						}
					}
					if entry != "" {
						log.Infof(context.Background(), "got %s", entry)
						results[entry] = struct{}{}
					}
					if len(results) >= len(tc.exp) && !done {
						done = true
						close(closer)
					}
					return nil
				}

			s, _, kvDB := serverutils.StartServer(t, base.TestServerArgs{
				Knobs: base.TestingKnobs{Store: &storeKnobs}})
			defer s.Stopper().Stop()
			// Split the Range. This should not have any asynchronous intents.
			if err := kvDB.AdminSplit(splitKey); err != nil {
				t.Fatal(err)
			}

			if err := kvDB.Txn(func(txn *client.Txn) error {
				b := txn.NewBatch()
				if tc.keys[0] >= string(splitKey) {
					t.Fatalf("first key %s must be < split key %s", tc.keys[0], splitKey)
				}
				for i, key := range tc.keys {
					// The first write must not go to batch, it anchors the
					// transaction to the correct range.
					local := i != 0 && rnd.Intn(2) == 0
					log.Infof(context.Background(), "%d: %s: local: %t", i, key, local)
					if local {
						b.Put(key, "test")
					} else if err := txn.Put(key, "test"); err != nil {
						return err
					}
				}

				for _, kr := range tc.ranges {
					local := rnd.Intn(2) == 0
					log.Infof(context.Background(), "%d: [%s,%s): local: %t", i, kr[0], kr[1], local)
					if local {
						b.DelRange(kr[0], kr[1], false)
					} else if err := txn.DelRange(kr[0], kr[1]); err != nil {
						return err
					}
				}

				return txn.CommitInBatch(b)
			}); err != nil {
				t.Fatalf("%d: %s", i, err)
			}
			<-closer // wait for async intents
			// Use Raft to make it likely that any straddling intent
			// resolutions have come in. Don't touch existing data; that could
			// generate unexpected intent resolutions.
			if _, err := kvDB.Scan("z\x00", "z\x00\x00", 0); err != nil {
				t.Fatal(err)
			}
		}()
		// Verification. Note that this runs after the system has stopped, so that
		// everything asynchronous has already happened.
		expResult := tc.exp
		sort.Strings(expResult)
		var actResult []string
		for k := range results {
			actResult = append(actResult, k)
		}
		sort.Strings(actResult)
		if !reflect.DeepEqual(actResult, expResult) {
			t.Fatalf("%d: unexpected non-local intents, expected %s: %s", i, expResult, actResult)
		}
	}
}
