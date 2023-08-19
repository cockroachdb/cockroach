package rangecache

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/errors"
)

const (
	// RangeLookupPrefetchCount is the maximum number of range descriptors to prefetch
	// during range lookups.
	RangeLookupPrefetchCount = 8
)

// FirstRangeProvider is capable of providing DistSender with the descriptor of
// the first range in the cluster and notifying the DistSender when the first
// range in the cluster has changed.
type FirstRangeProvider interface {
	// GetFirstRangeDescriptor returns the RangeDescriptor for the first range
	// in the cluster.
	GetFirstRangeDescriptor() (*roachpb.RangeDescriptor, error)

	// OnFirstRangeChanged calls the provided callback when the RangeDescriptor
	// for the first range has changed.
	OnFirstRangeChanged(func(*roachpb.RangeDescriptor))
}

// RangeLookup is a type which can query range descriptors from an
// underlying datastore. This interface is used by RangeCache to
// initially retrieve information which will be cached.
type RangeLookup interface {
	// RangeLookup takes a key to look up descriptors for. Two slices of range
	// descriptors are returned. The first of these slices holds descriptors
	// whose [startKey,endKey) spans contain the given key (possibly from
	// intents), and the second holds prefetched adjacent descriptors.
	//
	// Note that the acceptable consistency values are the constants defined
	// in this package: ReadFromFollower and ReadFromLeaseholder. The
	// RangeLookupConsistency type is aliased to kvpb.ReadConsistencyType
	// in order to permit implementations of this interface to import this
	// package.
	RangeLookup(
		ctx context.Context,
		key roachpb.RKey,
		consistency RangeLookupConsistency,
		useReverseScan bool,
	) ([]roachpb.RangeDescriptor, []roachpb.RangeDescriptor, error)
}

type RangeLookupKV struct {
	// firstRangeProvider provides the range descriptor for range one.
	// This is not required if a RangeLookup is supplied.
	firstRangeProvider FirstRangeProvider
	sender             kv.Sender
}

// RangeLookup implements the RangeLookup interface.
//
// It uses LookupRange to perform a lookup scan for the provided key, using
// DistSender itself as the client.Sender. This means that the scan will recurse
// into DistSender, which will in turn use the RangeDescriptorCache again to
// lookup the RangeDescriptor necessary to perform the scan.
//
// The client has some control over the consistency of the lookup. The
// acceptable values for the consistency argument are INCONSISTENT
// or READ_UNCOMMITTED. We use INCONSISTENT for an optimistic lookup
// pass. If we don't find a new enough descriptor, we do a leaseholder
// read at READ_UNCOMMITTED in order to read intents as well as committed
// values. The reason for this is that it's not clear whether the intent
// or the previous value points to the correct location of the Range. It gets
// even more complicated when there are split-related intents or a txn record
// co-located with a replica involved in the split. Since we cannot know the
// correct answer, we look up both the pre- and post- transaction values.
//
// Note that consistency levels CONSISTENT or INCONSISTENT will result in an
// assertion failed error. See the commentary on kv.RangeLookup for more
// details.
func (ds *RangeLookupKV) RangeLookup(
	ctx context.Context, key roachpb.RKey, rc RangeLookupConsistency, useReverseScan bool,
) ([]roachpb.RangeDescriptor, []roachpb.RangeDescriptor, error) {

	// In this case, the requested key is stored in the cluster's first
	// range. Return the first range, which is always gossiped and not
	// queried from the datastore.
	if keys.RangeMetaKey(key).Equal(roachpb.RKeyMin) {
		desc, err := ds.firstRangeProvider.GetFirstRangeDescriptor()
		if err != nil {
			return nil, nil, err
		}
		return []roachpb.RangeDescriptor{*desc}, nil, nil
	}

	switch rc {
	case kvpb.INCONSISTENT, kvpb.READ_UNCOMMITTED:
	default:
		return nil, nil, errors.AssertionFailedf("invalid consistency level %v", rc)
	}

	// By using DistSender as the sender, we guarantee that even if the desired
	// RangeDescriptor is not on the first range we send the lookup too, we'll
	// still find it when we scan to the next range. This addresses the issue
	// described in #18032 and #16266, allowing us to support meta2 splits.
	return kv.RangeLookup(ctx, ds.sender, key.AsRawKey(), rc, RangeLookupPrefetchCount, useReverseScan)
}
