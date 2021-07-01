// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sessiondata

// SequenceCache stores sequence values that have already been created in KV
// and are available to be given out as sequence numbers. Values for sequences
// are keyed by the descpb.ID of each sequence. These IDs are represented as
// uint32 to prevent an import cycle with the descpb package. The cache should
// only be accessed using the provided API.
//
// The cache ensures that values are invalidated when new descriptor versions are seen. Note that
// new descriptor versions may not monotonically increase. For example, the sequence schema
// may be altered in a txn, so the cache sees a new version V and invalidates/repopulates itself. Then,
// the txn may get rolled back, so the cache will see version V-1 and invalidate/repopulate itself again.
type SequenceCache map[uint32]*sequenceCacheEntry

type sequenceCacheEntry struct {
	// cachedVersion stores the descpb.DescriptorVersion that cached values are associated with.
	// The version is checked to determine if cache needs to be invalidated. The version is stored as
	// a uint32 to prevent an import cycle with the descpb package.
	cachedVersion uint32
	// currentValue stores the present value of the sequence to be given out.
	currentValue int64
	// increment stores the amount to increment the currentVal by each time the
	// currentVal is used. This value corresponds to descpb.TableDescriptor_SequenceOpts.Increment.
	increment int64
	// numValues represents the number of values to cache. The cache is considered
	// to be empty when numValues is 0.
	numValues int64
}

// NextValue fetches the next value in the sequence cache. If the values in the cache have all been
// given out or if the descriptor version has changed, then fetchNextValues() is used to repopulate the cache.
func (sc SequenceCache) NextValue(
	seqID uint32, clientVersion uint32, fetchNextValues func() (int64, int64, int64, error),
) (int64, error) {
	// Create entry for this sequence ID if there are no existing entries.
	if _, found := sc[seqID]; !found {
		sc[seqID] = &sequenceCacheEntry{}
	}
	cacheEntry := sc[seqID]

	if cacheEntry.numValues > 0 && cacheEntry.cachedVersion == clientVersion {
		cacheEntry.currentValue += cacheEntry.increment
		cacheEntry.numValues--
		return cacheEntry.currentValue - cacheEntry.increment, nil
	}

	currentValue, increment, numValues, err := fetchNextValues()
	if err != nil {
		return 0, err
	}

	// One value must be returned, and the rest of the values are stored.
	val := currentValue
	cacheEntry.currentValue = currentValue + increment
	cacheEntry.increment = increment
	cacheEntry.numValues = numValues - 1
	cacheEntry.cachedVersion = clientVersion
	return val, nil
}
