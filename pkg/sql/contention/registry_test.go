// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package contention

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/cache"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
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
func TestRegistry(t *testing.T) {
	uuidMap := make(map[string]uuid.UUID)
	testFriendlyRegistryString := func(r *Registry) string {
		stringRepresentation := r.String()
		// Swap out all UUIDs for corresponding test-friendly IDs.
		for friendlyID, txnID := range uuidMap {
			stringRepresentation = strings.Replace(stringRepresentation, txnID.String(), friendlyID, -1)
		}
		return stringRepresentation
	}
	registryMap := make(map[string]*Registry)
	// registry is the current registry.
	var registry *Registry
	datadriven.RunTest(t, "testdata/contention_registry", func(t *testing.T, d *datadriven.TestData) string {
		switch d.Cmd {
		case "use":
			var registryKey string
			d.ScanArgs(t, "registry", &registryKey)
			var ok bool
			registry, ok = registryMap[registryKey]
			if !ok {
				registry = NewRegistry()
				registryMap[registryKey] = registry
			}
			return d.Expected
		case "ev", "evcheck":
			var (
				tableIDStr string
				indexIDStr string
				key        string
				id         string
				duration   string
			)
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
			keyBytes := keys.MakeTableIDIndexID(nil, uint32(tableID), uint32(indexID))
			keyBytes = encoding.EncodeStringAscending(keyBytes, key)
			if err := registry.AddContentionEvent(roachpb.ContentionEvent{
				Key: keyBytes,
				TxnMeta: enginepb.TxnMeta{
					ID: contendingTxnID,
				},
				Duration: time.Duration(contentionDuration),
			}); err != nil {
				return err.Error()
			}
			if d.Cmd == "evcheck" {
				return testFriendlyRegistryString(registry)
			}
			return d.Expected
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
	registry := NewRegistry()
	errCh := make(chan error, numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			errCh <- registry.AddContentionEvent(roachpb.ContentionEvent{
				Key: keys.MakeTableIDIndexID(nil /* key */, 1 /* tableID */, 1 /* indexID */),
			})
		}()
	}

	wg.Wait()

	for drained := false; drained; {
		select {
		case err := <-errCh:
			require.NoError(t, err, "unexpected error from goroutine adding contention event")
		default:
			// Nothing else to read.
			drained = true
		}
	}

	numContentionEvents := uint64(0)
	registry.indexMap.internalCache.Do(func(e *cache.Entry) {
		v := e.Value.(*indexMapValue)
		numContentionEvents += v.numContentionEvents
	})
	require.Equal(t, uint64(numGoroutines), numContentionEvents)
}
