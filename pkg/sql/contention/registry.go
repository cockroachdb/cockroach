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
	"strings"
	"time"
	"unsafe"

	"github.com/biogo/store/llrb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/util/cache"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// Registry is an object that keeps track of aggregated contention information.
// It can be thought of as three maps:
// 1) The top-level map is a mapping from a tableID/indexID which uniquely
// describes an index to that index's contention information.
// 2) The contention information is itself a map from keys in that index
// (maintained in sorted order for better observability) to a list of
// transactions that caused contention events on the keys.
// 3) These transactions map the transaction ID to the number of times that
// transaction was observed (i.e. how many contention events that txn
// generated).
// The datadriven test contains string representations of this struct which make
// it easier to visualize.
type Registry struct {
	// globalLock is a coarse-grained lock over the registry which allows for
	// concurrent calls to AddContentionEvent. Note that this is not optimal since
	// all calls to AddContentionEvent will need to acquire this global lock.
	// This can be but is not optimized since this mutex only has the potential to
	// be a bottleneck if there is a lot of contention. In this case, there are
	// probably bigger problems to worry about.
	globalLock syncutil.Mutex
	// indexMap is an LRU cache that keeps track of up to indexMapMaxSize
	// contended indexes.
	indexMap *indexMap
}

const (
	// indexMapMaxSize specifies the maximum number of indexes a Registry should
	// keep track of contention events for.
	indexMapMaxSize = 50
	// orderedKeyMapMaxSize specifies the maximum number of keys in a given table
	// to keep track of contention events for.
	orderedKeyMapMaxSize = 50
	// maxNumTxns specifies the maximum number of txns that caused contention
	// events to keep track of.
	maxNumTxns = 10
)

// TODO(asubiotto): Remove once used.
var _ = GetRegistryEstimatedMaxMemoryFootprintInBytes

// GetRegistryEstimatedMaxMemoryFootprintInBytes returns the estimated memory
// footprint of a contention.Registry given an average key size of
// estimatedAverageKeySize bytes.
// Serves to reserve a reasonable amount of memory for this object.
// Around ~4.5MB at the time of writing.
func GetRegistryEstimatedMaxMemoryFootprintInBytes() uintptr {
	const estimatedAverageKeySize = 64
	txnsMapSize := maxNumTxns * (unsafe.Sizeof(uuid.UUID{}) + unsafe.Sizeof(int(0)))
	orderedKeyMapSize := orderedKeyMapMaxSize * ((unsafe.Sizeof(comparableKey{}) * estimatedAverageKeySize) + txnsMapSize)
	return indexMapMaxSize * (unsafe.Sizeof(indexMapKey{}) + unsafe.Sizeof(indexMapValue{}) + orderedKeyMapSize)
}

var orderedKeyMapCfg = cache.Config{
	Policy: cache.CacheLRU,
	ShouldEvict: func(size int, _, _ interface{}) bool {
		return size > orderedKeyMapMaxSize
	},
}

var txnCacheCfg = cache.Config{
	Policy: cache.CacheLRU,
	ShouldEvict: func(size int, _, _ interface{}) bool {
		return size > maxNumTxns
	},
}

type comparableKey roachpb.Key

// Compare implements llrb.Comparable.
func (c comparableKey) Compare(other llrb.Comparable) int {
	return roachpb.Key(c).Compare(roachpb.Key(other.(comparableKey)))
}

type indexMapKey struct {
	tableID descpb.ID
	indexID descpb.IndexID
}

// indexMapValue is the value associated with an indexMapKey. It contains
// contention information about the tableID/indexID pair.
type indexMapValue struct {
	// numContentionEvents is the number of contention events that have happened.
	numContentionEvents uint64
	// cumulativeContentionTime is the total duration that transactions touching
	// this index have spent contended.
	cumulativeContentionTime time.Duration
	// orderedKeyMap is an LRU cache that keeps track of up to
	// orderedKeyMapMaxSize keys that were the subject of contention in the given
	// index. The keys are roachpb.Keys and the values are *cache.UnorderedCaches,
	// which in turn is a cache of txn IDs of txns that caused contention events
	// and the number of times that each txn ID was observed.
	orderedKeyMap *cache.OrderedCache
}

// newIndexMapValue creates a new indexMapValue for a contention event
// initialized with that event's data.
func newIndexMapValue(c roachpb.ContentionEvent) *indexMapValue {
	txnCache := cache.NewUnorderedCache(txnCacheCfg)
	txnCache.Add(c.TxnMeta.ID, 1)
	keyMap := cache.NewOrderedCache(orderedKeyMapCfg)
	keyMap.Add(comparableKey(c.Key), txnCache)
	return &indexMapValue{
		numContentionEvents:      1,
		cumulativeContentionTime: c.Duration,
		orderedKeyMap:            keyMap,
	}
}

// addContentionEvent adds the given contention event to previously aggregated
// contention data.
func (v *indexMapValue) addContentionEvent(c roachpb.ContentionEvent) {
	v.numContentionEvents++
	v.cumulativeContentionTime += c.Duration
	var numTimesThisTxnWasEncountered int
	txnCache, ok := v.orderedKeyMap.Get(comparableKey(c.Key))
	if ok {
		if txnVal, ok := txnCache.(*cache.UnorderedCache).Get(c.TxnMeta.ID); ok {
			numTimesThisTxnWasEncountered = txnVal.(int)
		}
	} else {
		// This key was not found in the map. Create a new txn cache for this key.
		txnCache = cache.NewUnorderedCache(txnCacheCfg)
		v.orderedKeyMap.Add(comparableKey(c.Key), txnCache)
	}
	txnCache.(*cache.UnorderedCache).Add(c.TxnMeta.ID, numTimesThisTxnWasEncountered+1)
}

// indexMap is a helper struct that wraps an LRU cache sized up to
// indexMapMaxSize and maps tableID/indexID pairs to indexMapValues.
type indexMap struct {
	internalCache *cache.UnorderedCache
	scratchKey    indexMapKey
}

func newIndexMap() *indexMap {
	return &indexMap{
		internalCache: cache.NewUnorderedCache(cache.Config{
			Policy: cache.CacheLRU,
			ShouldEvict: func(size int, _, _ interface{}) bool {
				return size > indexMapMaxSize
			},
		}),
	}
}

// get gets the indexMapValue keyed by the tableID/indexID pair and returns it
// and true if it exists, nil and false if not. This counts as a cache access.
func (m *indexMap) get(tableID descpb.ID, indexID descpb.IndexID) (*indexMapValue, bool) {
	m.scratchKey.tableID = tableID
	m.scratchKey.indexID = indexID
	v, ok := m.internalCache.Get(m.scratchKey)
	if !ok {
		return nil, false
	}
	return v.(*indexMapValue), true
}

// add adds the indexMapValue keyed by the tableID/indexID pair. This counts as
// a cache access.
func (m *indexMap) add(tableID descpb.ID, indexID descpb.IndexID, v *indexMapValue) {
	m.scratchKey.tableID = tableID
	m.scratchKey.indexID = indexID
	m.internalCache.Add(m.scratchKey, v)
}

// NewRegistry creates a new Registry.
func NewRegistry() *Registry {
	r := &Registry{
		indexMap: newIndexMap(),
	}
	return r
}

// AddContentionEvent adds a new ContentionEvent to the Registry.
func (r *Registry) AddContentionEvent(c roachpb.ContentionEvent) error {
	_, rawTableID, rawIndexID, err := keys.DecodeTableIDIndexID(c.Key)
	if err != nil {
		return err
	}
	tableID := descpb.ID(rawTableID)
	indexID := descpb.IndexID(rawIndexID)
	r.globalLock.Lock()
	defer r.globalLock.Unlock()
	if v, ok := r.indexMap.get(tableID, indexID); !ok {
		// This is the first contention event seen for the given tableID/indexID
		// pair.
		r.indexMap.add(tableID, indexID, newIndexMapValue(c))
	} else {
		v.addContentionEvent(c)
	}
	return nil
}

// String returns a string representation of the Registry.
func (r *Registry) String() string {
	r.globalLock.Lock()
	defer r.globalLock.Unlock()
	var b strings.Builder
	r.indexMap.internalCache.Do(func(e *cache.Entry) {
		key := e.Key.(indexMapKey)
		b.WriteString(fmt.Sprintf("tableID=%d indexID=%d\n", key.tableID, key.indexID))
		writeChild := func(prefix, s string) {
			b.WriteString(prefix + s)
		}
		const prefixString = "  "
		prefix := prefixString
		v := e.Value.(*indexMapValue)
		writeChild(prefix, fmt.Sprintf("num contention events: %d\n", v.numContentionEvents))
		writeChild(prefix, fmt.Sprintf("cumulative contention time: %s\n", v.cumulativeContentionTime))
		writeChild(prefix, "keys:\n")
		keyPrefix := prefix + prefixString
		v.orderedKeyMap.Do(func(k, txnCache interface{}) bool {
			writeChild(keyPrefix, fmt.Sprintf("%s contending txns:\n", roachpb.Key(k.(comparableKey))))
			txnPrefix := keyPrefix + prefixString
			txnCache.(*cache.UnorderedCache).Do(func(e *cache.Entry) {
				writeChild(txnPrefix, fmt.Sprintf("id=%s count=%d\n", e.Key.(uuid.UUID), e.Value.(int)))
			})
			return false
		})
	})
	return b.String()
}
