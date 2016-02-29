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
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/util/leaktest"
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
		// range, so "a"-"s" is local.
		{
			// All local points, except for "s" and "x"
			keys:   []string{"a", "x", "b", "c", "s"},
			ranges: [][2]string{{"d", "e"}},
			exp:    []string{"s", "x"},
		},
		{
			// h is local, y is covered by the Range below but still gets an
			// explicit request since we do it all in the same batch (didn't
			// seem worth optimizing), and z is an explicit end point.
			keys:   []string{"h", "y", "z"},
			ranges: [][2]string{{"g", "z"}},
			exp:    []string{`"s"-"z"`, "z" /* optimizable: */, "y"},
		},
		{
			// This test demonstrates another redundancy. Two overlapping key
			// ranges aren't reduced to a wider range intent for the non-local
			// part, though the contained ranges "a"-"u" and "t"-"u" are.
			// Might make sense to optimize this away at some point, along with
			// "s" which is also already covered by the range intents.
			keys:   []string{"q", "s"},
			ranges: [][2]string{{"a", "w"}, {"b", "x"}, {"t", "u"}},
			exp:    []string{`"s"-"w"`, `"s"-"x"` /* optimizable: */, `"t"-"u"`, "s"},
		},
	}

	splitKey := []byte("s")
	results := map[string]struct{}{}
	defer func() { storage.TestingCommandFilter = nil }()
	for i, tc := range testCases {
		var mu sync.Mutex
		closer := make(chan struct{}, 2)
		storage.TestingCommandFilter = func(_ roachpb.StoreID, args roachpb.Request, _ roachpb.Header) error {
			mu.Lock()
			defer mu.Unlock()
			header := args.Header()
			// Ignore anything outside of the intent key range of "a" - "z"
			if header.Key.Compare(roachpb.Key("a")) < 0 || header.Key.Compare(roachpb.Key("z")) > 0 {
				return nil
			}
			switch args.(type) {
			case *roachpb.ResolveIntentRequest:
				results[string(header.Key)] = struct{}{}
			case *roachpb.ResolveIntentRangeRequest:
				results[fmt.Sprintf("%s-%s", header.Key, header.EndKey)] = struct{}{}
			}
			if len(results) == len(tc.exp) {
				closer <- struct{}{}
			}
			return nil
		}
		func() {
			s := StartTestServer(t)
			defer s.Stop()

			go func() {
				// Sets a timeout, cut short by the stopper having drained.
				select {
				case <-time.After(time.Second):
				case <-s.Server.stopper.ShouldStop():
					return
				}
				closer <- struct{}{}
			}()

			// Split the Range. This should not have any asynchronous intents.
			if err := s.db.AdminSplit(splitKey); err != nil {
				t.Fatal(err)
			}

			if pErr := s.db.Txn(func(txn *client.Txn) *roachpb.Error {
				b := txn.NewBatch()
				for _, key := range tc.keys {
					b.Put(key, "test")
				}
				for _, kr := range tc.ranges {
					b.DelRange(kr[0], kr[1], false)
				}
				return txn.CommitInBatch(b)
			}); pErr != nil {
				t.Fatalf("%d: %s", i, pErr)
			}
			<-closer // wait for async intents
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
		if !reflect.DeepEqual(expResult, expResult) {
			t.Fatalf("%d: unexpected non-local intents, expected %s: %s", i, expResult, expResult)
		}
	}
}
