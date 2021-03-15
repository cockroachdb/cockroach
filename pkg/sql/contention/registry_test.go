// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package contention_test

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/contention"
	"github.com/cockroachdb/cockroach/pkg/sql/contentionpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

// TestRegistry runs the datadriven test found in testdata/contention_registry.
// The format of these tests is:
// # Use and create if nonexistent a registry named "a".
// use registry=a
// ----
//
// # Add a contention event to the registry but don't verify state.
// # tableid is the table ID.
// # indexid is the index ID.
// # key is a string key that a contention event was generated for.
// # txnid is an id that represents the contending transaction.
// # duration is the duration of ns the contention event lasted for.
// ev tableid=1 indexid=1 key=key txnid=a duration=1
// ----
//
// use registry=b
// ----
//
// # Add a contention event to the registry and verify state.
// evcheck tableid=1 indexid=1 key=key txnid=b duration=2
// ----
// < Registry b as string >
//
// # Merge two registries into one and verify state.
// merge first=a second=b
// ----
// < Merged registries a and b as string >
func TestRegistry(t *testing.T) {
	uuidMap := make(map[string]uuid.UUID)
	testFriendlyRegistryString := func(stringRepresentation string) string {
		// Swap out all UUIDs for corresponding test-friendly IDs.
		for friendlyID, txnID := range uuidMap {
			stringRepresentation = strings.Replace(stringRepresentation, txnID.String(), friendlyID, -1)
		}
		return stringRepresentation
	}
	registryMap := make(map[string]*contention.Registry)
	// registry is the current registry.
	var registry *contention.Registry
	datadriven.RunTest(t, "testdata/contention_registry", func(t *testing.T, d *datadriven.TestData) string {
		switch d.Cmd {
		case "use":
			var registryKey string
			d.ScanArgs(t, "registry", &registryKey)
			var ok bool
			registry, ok = registryMap[registryKey]
			if !ok {
				registry = contention.NewRegistry()
				registryMap[registryKey] = registry
			}
			return d.Expected
		case "merge":
			var firstRegistryKey, secondRegistryKey string
			d.ScanArgs(t, "first", &firstRegistryKey)
			first, ok := registryMap[firstRegistryKey]
			if !ok {
				return fmt.Sprintf("registry %q not found", first)
			}
			d.ScanArgs(t, "second", &secondRegistryKey)
			second, ok := registryMap[secondRegistryKey]
			if !ok {
				return fmt.Sprintf("registry %q not found", second)
			}
			merged := contention.MergeSerializedRegistries(first.Serialize(), second.Serialize())
			var b strings.Builder
			for i := range merged.IndexContentionEvents {
				b.WriteString(merged.IndexContentionEvents[i].String())
			}
			for i := range merged.NonSQLKeysContention {
				b.WriteString(merged.NonSQLKeysContention[i].String())
			}
			return testFriendlyRegistryString(b.String())
		case "ev", "evnonsql", "evcheck":
			var (
				key      string
				id       string
				duration string
			)
			var keyBytes []byte
			if d.Cmd != "evnonsql" {
				var tableIDStr string
				var indexIDStr string
				d.ScanArgs(t, "tableid", &tableIDStr)
				tableID, err := strconv.Atoi(tableIDStr)
				if err != nil {
					return fmt.Sprintf("could not parse table ID %s as int: %v", tableIDStr, err)
				}
				d.ScanArgs(t, "indexid", &indexIDStr)
				indexID, err := strconv.Atoi(indexIDStr)
				if err != nil {
					return fmt.Sprintf("could not parse index ID %s as int: %v", indexIDStr, err)
				}
				keyBytes = keys.MakeTableIDIndexID(nil, uint32(tableID), uint32(indexID))
			} else {
				// Choose such a byte sequence that keys.DecodeTableIDIndexID
				// would fail on this prefix.
				keyBytes = []byte{255}
			}
			d.ScanArgs(t, "key", &key)
			d.ScanArgs(t, "id", &id)
			contendingTxnID, ok := uuidMap[id]
			if !ok {
				// If a UUID wasn't created, create it and keep it for future reference.
				contendingTxnID = uuid.MakeV4()
				uuidMap[id] = contendingTxnID
			}
			d.ScanArgs(t, "duration", &duration)
			contentionDuration, err := strconv.Atoi(duration)
			if err != nil {
				return fmt.Sprintf("could not parse duration %s as int: %v", duration, err)
			}
			keyBytes = encoding.EncodeStringAscending(keyBytes, key)
			registry.AddContentionEvent(roachpb.ContentionEvent{
				Key: keyBytes,
				TxnMeta: enginepb.TxnMeta{
					ID: contendingTxnID,
				},
				Duration: time.Duration(contentionDuration),
			})
			if d.Cmd != "evcheck" {
				return d.Expected
			}
			fallthrough
		case "check":
			return testFriendlyRegistryString(registry.String())
		default:
			return fmt.Sprintf("unknown command: %s", d.Cmd)
		}
	})
}

func TestRegistryConcurrentAdds(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numGoroutines = 10
	var wg sync.WaitGroup
	wg.Add(numGoroutines)
	registry := contention.NewRegistry()
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			registry.AddContentionEvent(roachpb.ContentionEvent{
				Key: keys.MakeTableIDIndexID(nil /* key */, 1 /* tableID */, 1 /* indexID */),
			})
		}()
	}

	wg.Wait()

	require.Equal(t, uint64(numGoroutines), contention.CalculateTotalNumContentionEvents(registry))
}

// TestSerializedRegistryInvariants verifies that the serialized registries
// maintain all invariants, namely that
// - all three levels of objects are subject to the respective maximum size
// - all three levels of objects satisfy the respective ordering
//   requirements.
func TestSerializedRegistryInvariants(t *testing.T) {
	rng, _ := randutil.NewPseudoRand()
	const nonSQLKeyProbability = 0.1
	const sizeLimit = 5
	const keySpaceSize = sizeLimit * sizeLimit
	// Use large limit on the number of contention events so that the likelihood
	// of "collisions" is pretty high.
	const maxNumContentionEvents = keySpaceSize * keySpaceSize * keySpaceSize
	testIndexMapMaxSize := 1 + rng.Intn(sizeLimit)
	testOrderedKeyMapMaxSize := 1 + rng.Intn(sizeLimit)
	testMaxNumTxns := 1 + rng.Intn(sizeLimit)

	cleanup := contention.SetSizeConstants(testIndexMapMaxSize, testOrderedKeyMapMaxSize, testMaxNumTxns)
	defer cleanup()

	// keySpace defines a continuous byte slice that we will be sub-slicing in
	// order to get the keys.
	keySpace := make([]byte, keySpaceSize)
	_, err := rng.Read(keySpace)
	require.NoError(t, err)
	getKey := func() []byte {
		keyStart := rng.Intn(len(keySpace))
		keyEnd := keyStart + 1 + rng.Intn(len(keySpace)-keyStart)
		return keySpace[keyStart:keyEnd]
	}

	// populateRegistry add a random number of contention events (limited by
	// maxNumContentionEvents) to r.
	populateRegistry := func(r *contention.Registry) {
		numContentionEvents := rng.Intn(maxNumContentionEvents)
		for i := 0; i < numContentionEvents; i++ {
			var key []byte
			if rng.Float64() > nonSQLKeyProbability {
				// Create a key with a valid SQL prefix.
				tableID := uint32(1 + rng.Intn(testIndexMapMaxSize+1))
				indexID := uint32(1 + rng.Intn(testIndexMapMaxSize+1))
				key = keys.MakeTableIDIndexID(nil /* key */, tableID, indexID)
			}
			key = append(key, getKey()...)
			r.AddContentionEvent(roachpb.ContentionEvent{
				Key: key,
				TxnMeta: enginepb.TxnMeta{
					ID:  uuid.MakeV4(),
					Key: getKey(),
				},
				Duration: time.Duration(int64(rng.Uint64())),
			})
		}
	}

	// checkSerializedRegistryInvariants verifies that all invariants about the
	// sizes of registries and the ordering of objects at all levels are
	// maintained.
	checkSerializedRegistryInvariants := func(r contentionpb.SerializedRegistry) {
		// Check the total size of the index contention events in the serialized
		// registry.
		ice := r.IndexContentionEvents
		require.GreaterOrEqual(t, testIndexMapMaxSize, len(ice))
		for i := range ice {
			if i > 0 {
				// Check the ordering of IndexContentionEvents.
				require.LessOrEqual(t, ice[i].NumContentionEvents, ice[i-1].NumContentionEvents)
				if ice[i].NumContentionEvents == ice[i-1].NumContentionEvents {
					require.LessOrEqual(t, int64(ice[i].CumulativeContentionTime), int64(ice[i-1].CumulativeContentionTime))
				}
			}
			// Check the number of contended keys.
			keys := ice[i].Events
			require.GreaterOrEqual(t, testOrderedKeyMapMaxSize, len(keys))
			for j := range keys {
				if j > 0 {
					// Check the ordering of the keys.
					require.True(t, keys[j].Key.Compare(keys[j-1].Key) >= 0)
				}
				// Check the number of contended transactions on this key.
				txns := keys[j].Txns
				require.GreaterOrEqual(t, testMaxNumTxns, len(txns))
				for k := range txns {
					if k > 0 {
						// Check the ordering of the transactions.
						require.LessOrEqual(t, txns[k].Count, txns[k-1].Count)
					}
				}
			}
		}

		// Check the total size of the contention events on non-SQL keys in the
		// serialized registry.
		nkc := r.NonSQLKeysContention
		require.GreaterOrEqual(t, testOrderedKeyMapMaxSize, len(nkc))
		for i := range nkc {
			if i > 0 {
				// Check the ordering of the keys.
				require.True(t, nkc[i].Key.Compare(nkc[i-1].Key) >= 0)
			}
			// Check the number of contended transactions on this key.
			txns := nkc[i].Txns
			require.GreaterOrEqual(t, testMaxNumTxns, len(txns))
			for k := range txns {
				if k > 0 {
					// Check the ordering of the transactions.
					require.LessOrEqual(t, txns[k].Count, txns[k-1].Count)
				}
			}
		}
	}

	createNewSerializedRegistry := func() contentionpb.SerializedRegistry {
		r := contention.NewRegistry()
		populateRegistry(r)
		s := r.Serialize()
		checkSerializedRegistryInvariants(s)
		return s
	}

	m := createNewSerializedRegistry()

	const numMerges = 5
	for i := 0; i < numMerges; i++ {
		s := createNewSerializedRegistry()
		m = contention.MergeSerializedRegistries(m, s)
		checkSerializedRegistryInvariants(m)
	}
}
