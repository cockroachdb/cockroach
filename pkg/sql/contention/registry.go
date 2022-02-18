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
	"bytes"
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/biogo/store/llrb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/contentionpb"
	"github.com/cockroachdb/cockroach/pkg/util/cache"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
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
// It also tracks the information about contention on non-SQL keys separately.
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
	// nonSQLKeysMap is an LRU cache that keeps track of up to
	// orderedKeyMapMaxSize non-SQL contended keys.
	nonSQLKeysMap *nonSQLKeysMap

	// eventStore stores the historical contention events and maps the
	// transactions in the contention events to their corresponding transaction
	// fingerprint ID.
	eventStore *eventStore
}

var (
	// indexMapMaxSize specifies the maximum number of indexes a Registry should
	// keep track of contention events for.
	indexMapMaxSize = 50
	// orderedKeyMapMaxSize specifies the maximum number of keys in a given table
	// to keep track of contention events for.
	orderedKeyMapMaxSize = 50
	// maxNumTxns specifies the maximum number of txns that caused contention
	// events to keep track of.
	maxNumTxns = 10

	_ eventReader = &Registry{}
)

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
	txnCache.Add(c.TxnMeta.ID, uint64(1))
	keyMap := cache.NewOrderedCache(orderedKeyMapCfg)
	keyMap.Add(comparableKey(c.Key), txnCache)
	return &indexMapValue{
		numContentionEvents:      1,
		cumulativeContentionTime: c.Duration,
		orderedKeyMap:            keyMap,
	}
}

// addContentionEvent adds the given contention event to previously aggregated
// contention data. It assumes that c.Key is a SQL key.
func (v *indexMapValue) addContentionEvent(c roachpb.ContentionEvent) {
	v.numContentionEvents++
	v.cumulativeContentionTime += c.Duration
	var numTimesThisTxnWasEncountered uint64
	txnCache, ok := v.orderedKeyMap.Get(comparableKey(c.Key))
	if ok {
		if txnVal, ok := txnCache.(*cache.UnorderedCache).Get(c.TxnMeta.ID); ok {
			numTimesThisTxnWasEncountered = txnVal.(uint64)
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

// nonSQLKeyMapValue is the value associated with a non-SQL key. It contains
// contention information about that key.
type nonSQLKeyMapValue struct {
	// numContentionEvents is the number of contention events that have
	// happened on the key.
	numContentionEvents uint64
	// cumulativeContentionTime is the total duration that transactions touching
	// the key have spent contended.
	cumulativeContentionTime time.Duration
	// txnCache is an LRU cache of txn IDs of txns that caused contention events
	// and the number of times that each txn ID was observed.
	txnCache *cache.UnorderedCache
}

// newNonSQLKeyMapValue creates a new nonSQLKeyMapValue for a contention event
// initialized with that event's data.
func newNonSQLKeyMapValue(c roachpb.ContentionEvent) *nonSQLKeyMapValue {
	txnCache := cache.NewUnorderedCache(txnCacheCfg)
	txnCache.Add(c.TxnMeta.ID, uint64(1))
	return &nonSQLKeyMapValue{
		numContentionEvents:      1,
		cumulativeContentionTime: c.Duration,
		txnCache:                 txnCache,
	}
}

// addContentionEvent adds the given contention event to previously aggregated
// contention data. It assumes that c.Key is a non-SQL key.
func (v *nonSQLKeyMapValue) addContentionEvent(c roachpb.ContentionEvent) {
	v.numContentionEvents++
	v.cumulativeContentionTime += c.Duration
	var numTimesThisTxnWasEncountered uint64
	if numTimes, ok := v.txnCache.Get(c.TxnMeta.ID); ok {
		numTimesThisTxnWasEncountered = numTimes.(uint64)
	}
	v.txnCache.Add(c.TxnMeta.ID, numTimesThisTxnWasEncountered+1)
}

// nonSQLKeysMap is a helper struct that wraps an LRU cache sized up to
// orderedKeyMapMaxSize mapping non-SQL keys to nonSQLKeyMapValues.
type nonSQLKeysMap struct {
	*cache.OrderedCache
}

func newNonSQLKeysMap() *nonSQLKeysMap {
	return &nonSQLKeysMap{
		OrderedCache: cache.NewOrderedCache(orderedKeyMapCfg),
	}
}

// NewRegistry creates a new Registry.
func NewRegistry(st *cluster.Settings, endpoint ResolverEndpoint) *Registry {
	return &Registry{
		indexMap:      newIndexMap(),
		nonSQLKeysMap: newNonSQLKeysMap(),
		eventStore:    newEventStore(st, endpoint, timeutil.Now),
	}
}

// Start starts the background goroutines for the Registry.
func (r *Registry) Start(ctx context.Context, stopper *stop.Stopper) {
	r.eventStore.start(ctx, stopper)
}

// AddContentionEvent adds a new ContentionEvent to the Registry.
func (r *Registry) AddContentionEvent(event contentionpb.ExtendedContentionEvent) {
	c := event.BlockingEvent
	r.globalLock.Lock()
	defer r.globalLock.Unlock()
	// Remove the tenant ID prefix if there is any.
	c.Key, _, _ = keys.DecodeTenantPrefix(c.Key)
	_, rawTableID, rawIndexID, err := keys.DecodeTableIDIndexID(c.Key)
	if err != nil {
		// The key is not a valid SQL key, so we store it in a separate cache.
		if v, ok := r.nonSQLKeysMap.Get(comparableKey(c.Key)); !ok {
			// This is the first contention event seen for this key.
			r.nonSQLKeysMap.Add(comparableKey(c.Key), newNonSQLKeyMapValue(c))
		} else {
			v.(*nonSQLKeyMapValue).addContentionEvent(c)
		}
		return
	}
	tableID := descpb.ID(rawTableID)
	indexID := descpb.IndexID(rawIndexID)
	if v, ok := r.indexMap.get(tableID, indexID); !ok {
		// This is the first contention event seen for the given tableID/indexID
		// pair.
		r.indexMap.add(tableID, indexID, newIndexMapValue(c))
	} else {
		v.addContentionEvent(c)
	}

	r.eventStore.addEvent(event)
}

// ForEachEvent implements the eventReader interface.
func (r *Registry) ForEachEvent(op func(event *contentionpb.ExtendedContentionEvent) error) error {
	return r.eventStore.ForEachEvent(op)
}

// FlushEventsForTest flushes contention events in the write-buffer into the in-memory
// store.
func (r *Registry) FlushEventsForTest(ctx context.Context) error {
	return r.eventStore.flushAndResolve(ctx)
}

func serializeTxnCache(txnCache *cache.UnorderedCache) []contentionpb.SingleTxnContention {
	txns := make([]contentionpb.SingleTxnContention, txnCache.Len())
	var txnCount int
	txnCache.Do(func(e *cache.Entry) {
		txns[txnCount].TxnID = e.Key.(uuid.UUID)
		txns[txnCount].Count = e.Value.(uint64)
		txnCount++
	})
	sortSingleTxnContention(txns)
	return txns
}

// Serialize returns the serialized representation of the registry. Refer to
// comments on contentionpb.SerializedRegistry for the orderings that are
// maintained at different levels of objects.
func (r *Registry) Serialize() contentionpb.SerializedRegistry {
	r.globalLock.Lock()
	defer r.globalLock.Unlock()
	var resp contentionpb.SerializedRegistry

	// Process all SQL keys information.
	resp.IndexContentionEvents = make([]contentionpb.IndexContentionEvents, r.indexMap.internalCache.Len())
	var iceCount int
	r.indexMap.internalCache.Do(func(e *cache.Entry) {
		ice := &resp.IndexContentionEvents[iceCount]
		key := e.Key.(indexMapKey)
		ice.TableID = key.tableID
		ice.IndexID = key.indexID
		v := e.Value.(*indexMapValue)
		ice.NumContentionEvents = v.numContentionEvents
		ice.CumulativeContentionTime = v.cumulativeContentionTime
		ice.Events = make([]contentionpb.SingleKeyContention, v.orderedKeyMap.Len())
		var skcCount int
		v.orderedKeyMap.Do(func(k, txnCacheInterface interface{}) bool {
			txnCache := txnCacheInterface.(*cache.UnorderedCache)
			skc := &ice.Events[skcCount]
			skc.Key = roachpb.Key(k.(comparableKey))
			skc.Txns = serializeTxnCache(txnCache)
			skcCount++
			return false
		})
		iceCount++
	})
	sortIndexContentionEvents(resp.IndexContentionEvents)

	// Process all non-SQL keys information.
	resp.NonSQLKeysContention = make([]contentionpb.SingleNonSQLKeyContention, r.nonSQLKeysMap.Len())
	var snkcCount int
	r.nonSQLKeysMap.Do(func(k, nonSQLKeyMapVal interface{}) bool {
		snkc := &resp.NonSQLKeysContention[snkcCount]
		snkc.Key = roachpb.Key(k.(comparableKey))
		v := nonSQLKeyMapVal.(*nonSQLKeyMapValue)
		snkc.NumContentionEvents = v.numContentionEvents
		snkc.CumulativeContentionTime = v.cumulativeContentionTime
		snkc.Txns = serializeTxnCache(v.txnCache)
		snkcCount++
		return false
	})

	return resp
}

// sortIndexContentionEvents sorts all of the index contention events in-place
// according to their importance (as defined by the total number of contention
// events).
func sortIndexContentionEvents(ice []contentionpb.IndexContentionEvents) {
	sort.Slice(ice, func(i, j int) bool {
		if ice[i].NumContentionEvents != ice[j].NumContentionEvents {
			return ice[i].NumContentionEvents > ice[j].NumContentionEvents
		}
		return ice[i].CumulativeContentionTime > ice[j].CumulativeContentionTime
	})
}

// sortSingleTxnContention sorts the transactions in-place according to the
// frequency of their occurrence in DESC order.
func sortSingleTxnContention(txns []contentionpb.SingleTxnContention) {
	sort.Slice(txns, func(i, j int) bool {
		return txns[i].Count > txns[j].Count
	})
}

// String returns a string representation of the Registry.
func (r *Registry) String() string {
	var b strings.Builder
	serialized := r.Serialize()
	for i := range serialized.IndexContentionEvents {
		b.WriteString(serialized.IndexContentionEvents[i].String())
	}
	for i := range serialized.NonSQLKeysContention {
		b.WriteString(serialized.NonSQLKeysContention[i].String())
	}
	return b.String()
}

// MergeSerializedRegistries merges the serialized representations of two
// Registries into one. first is modified in-place.
//
// The result will contain at most indexMapMaxSize number of
// IndexContentionEvents objects (with the most important objects - as defined
// by the total number of contention events - kept from both arguments) and at
// most orderedKeyMapMaxSize number of SingleNonSQLKeyContention objects (those
// with smaller lexicographically keys are kept from both arguments). Other
// constants (orderedKeyMapMaxSize and maxNumTxns) are also respected by the
// internal slices.
//
// The returned representation has the same ordering guarantees as described for
// contentionpb.SerializedRegistry.
func MergeSerializedRegistries(
	first, second contentionpb.SerializedRegistry,
) contentionpb.SerializedRegistry {
	// Merge IndexContentionEvents.
	firstICE, secondICE := first.IndexContentionEvents, second.IndexContentionEvents
	for s := range secondICE {
		found := false
		for f := range firstICE {
			if firstICE[f].TableID == secondICE[s].TableID && firstICE[f].IndexID == secondICE[s].IndexID {
				firstICE[f] = mergeIndexContentionEvents(firstICE[f], secondICE[s])
				found = true
				break
			}
		}
		if !found {
			firstICE = append(firstICE, secondICE[s])
		}
	}
	// Sort all of the index contention events so that more frequent ones are at
	// the front and then truncate if needed.
	sortIndexContentionEvents(firstICE)
	if len(firstICE) > indexMapMaxSize {
		firstICE = firstICE[:indexMapMaxSize]
	}
	first.IndexContentionEvents = firstICE

	// Merge SingleNonSQLKeyContention.
	//
	// Go over the non-SQL keys from both inputs and merge them so that we stay
	// under the limit. We take advantage of the fact that non-SQL keys for both
	// inputs are already ordered.
	//
	// Note that this code is basically a copy of the corresponding code in
	// mergeIndexContentionEvents, but it is very hard to abstract it away and
	// reuse.
	firstNKC, secondNKC := first.NonSQLKeysContention, second.NonSQLKeysContention
	maxNumNonSQLKeys := len(firstNKC) + len(secondNKC)
	if maxNumNonSQLKeys > orderedKeyMapMaxSize {
		maxNumNonSQLKeys = orderedKeyMapMaxSize
	}
	result := make([]contentionpb.SingleNonSQLKeyContention, maxNumNonSQLKeys)
	resultIdx, firstIdx, secondIdx := 0, 0, 0
	// Iterate while we haven't taken enough non-SQL keys and at least one input
	// is not exhausted.
	for resultIdx < maxNumNonSQLKeys && (firstIdx < len(firstNKC) || secondIdx < len(secondNKC)) {
		var cmp int
		if firstIdx == len(firstNKC) {
			// We've exhausted the list of non-SQL keys from the first input, so
			// we will need to take from the second input.
			cmp = 1
		} else if secondIdx == len(secondNKC) {
			// We've exhausted the list of non-SQL keys from the second input,
			// so we will need to take from the first input.
			cmp = -1
		} else {
			cmp = firstNKC[firstIdx].Key.Compare(secondNKC[secondIdx].Key)
		}
		if cmp == 0 {
			// The keys are the same, so we're merging the contention
			// information from both inputs.
			f := firstNKC[firstIdx]
			s := secondNKC[secondIdx]
			result[resultIdx].Key = f.Key
			result[resultIdx].NumContentionEvents = f.NumContentionEvents + s.NumContentionEvents
			result[resultIdx].CumulativeContentionTime = f.CumulativeContentionTime + s.CumulativeContentionTime
			result[resultIdx].Txns = mergeSingleKeyContention(f.Txns, s.Txns)
			firstIdx++
			secondIdx++
		} else if cmp < 0 {
			// The first non-SQL key is smaller, so we will take it as is.
			result[resultIdx] = firstNKC[firstIdx]
			firstIdx++
		} else {
			// The second non-SQL key is smaller, so we will take it as is.
			result[resultIdx] = secondNKC[secondIdx]
			secondIdx++
		}
		resultIdx++
	}
	// We might have merged the contention information for some non-SQL so that
	// we allocated more than necessary, so we need to truncate if that's the
	// case.
	result = result[:resultIdx]
	first.NonSQLKeysContention = result

	return first
}

// mergeIndexContentionEvents merges two lists of contention events that
// occurred on the same index. It will panic if the indexes are different.
//
// The result will contain at most orderedKeyMapMaxSize number of single key
// contention events (ordered by the keys).
func mergeIndexContentionEvents(
	first contentionpb.IndexContentionEvents, second contentionpb.IndexContentionEvents,
) contentionpb.IndexContentionEvents {
	if first.TableID != second.TableID || first.IndexID != second.IndexID {
		panic(fmt.Sprintf("attempting to merge contention events from different indexes\n%s%s", first.String(), second.String()))
	}
	var result contentionpb.IndexContentionEvents
	result.TableID = first.TableID
	result.IndexID = first.IndexID
	result.NumContentionEvents = first.NumContentionEvents + second.NumContentionEvents
	result.CumulativeContentionTime = first.CumulativeContentionTime + second.CumulativeContentionTime
	// Go over the events from both inputs and merge them so that we stay under
	// the limit. We take advantage of the fact that events for both inputs are
	// already ordered by their keys.
	//
	// Note that this code is basically a copy of the corresponding code in
	// MergeSerializedRegistries for non-SQL keys, but it is very hard to
	// abstract it away and reuse.
	maxNumEvents := len(first.Events) + len(second.Events)
	if maxNumEvents > orderedKeyMapMaxSize {
		maxNumEvents = orderedKeyMapMaxSize
	}
	result.Events = make([]contentionpb.SingleKeyContention, maxNumEvents)
	resultIdx, firstIdx, secondIdx := 0, 0, 0
	// Iterate while we haven't taken enough events and at least one input is
	// not exhausted.
	for resultIdx < maxNumEvents && (firstIdx < len(first.Events) || secondIdx < len(second.Events)) {
		var cmp int
		if firstIdx == len(first.Events) {
			// We've exhausted the list of events from the first input, so we
			// will need to take from the second input.
			cmp = 1
		} else if secondIdx == len(second.Events) {
			// We've exhausted the list of events from the second input, so we
			// will need to take from the first input.
			cmp = -1
		} else {
			cmp = first.Events[firstIdx].Key.Compare(second.Events[secondIdx].Key)
		}
		if cmp == 0 {
			// The keys are the same, so we're merging the events from both
			// inputs.
			f := first.Events[firstIdx]
			s := second.Events[secondIdx]
			result.Events[resultIdx].Key = f.Key
			result.Events[resultIdx].Txns = mergeSingleKeyContention(f.Txns, s.Txns)
			firstIdx++
			secondIdx++
		} else if cmp < 0 {
			// The first event is smaller, so we will take it as is.
			result.Events[resultIdx] = first.Events[firstIdx]
			firstIdx++
		} else {
			// The second event is smaller, so we will take it as is.
			result.Events[resultIdx] = second.Events[secondIdx]
			secondIdx++
		}
		resultIdx++
	}
	// We might have merged some events so that we allocated more than
	// necessary, so we need to truncate if that's the case.
	result.Events = result.Events[:resultIdx]
	return result
}

// mergeSingleKeyContention merges two lists of contention events that occurred
// on the same key updating first in-place. Objects are ordered by the count
// (after merge when applicable) in DESC order.
//
// The result will contain at most maxNumTxns number of transactions.
func mergeSingleKeyContention(
	first, second []contentionpb.SingleTxnContention,
) []contentionpb.SingleTxnContention {
	for s := range second {
		found := false
		for f := range first {
			if bytes.Equal(first[f].TxnID.GetBytes(), second[s].TxnID.GetBytes()) {
				first[f].Count += second[s].Count
				found = true
				break
			}
		}
		if !found {
			first = append(first, second[s])
		}
	}
	// Sort all of the transactions so that more frequent ones are at the front
	// and then truncate if needed.
	sortSingleTxnContention(first)
	if len(first) > maxNumTxns {
		first = first[:maxNumTxns]
	}
	return first
}
