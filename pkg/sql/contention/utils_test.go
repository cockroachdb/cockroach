// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package contention

import "github.com/cockroachdb/cockroach/pkg/util/cache"

// SetSizeConstants updates the constants for the sizes of caches of the
// registries for tests. If any of the passed-in arguments is not positive, it
// is ignored. A cleanup function is returned to restore the original values.
func SetSizeConstants(indexMap, orderedKeyMap, numTxns int) func() {
	oldIndexMapMaxSize := indexMapMaxSize
	oldOrderedKeyMapMaxSize := orderedKeyMapMaxSize
	oldMaxNumTxns := maxNumTxns
	if indexMap > 0 {
		indexMapMaxSize = indexMap
	}
	if orderedKeyMap > 0 {
		orderedKeyMapMaxSize = orderedKeyMap
	}
	if numTxns > 0 {
		maxNumTxns = numTxns
	}
	return func() {
		indexMapMaxSize = oldIndexMapMaxSize
		orderedKeyMapMaxSize = oldOrderedKeyMapMaxSize
		maxNumTxns = oldMaxNumTxns
	}
}

// CalculateTotalNumContentionEvents returns the total number of contention
// events that r knows about.
func CalculateTotalNumContentionEvents(r *Registry) uint64 {
	numContentionEvents := uint64(0)
	r.indexMap.internalCache.Do(func(e *cache.Entry) {
		v := e.Value.(*indexMapValue)
		numContentionEvents += v.numContentionEvents
	})
	return numContentionEvents
}
