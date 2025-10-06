// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package engineccl

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/rditer"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/objstorage/remote"
	"github.com/cockroachdb/pebble/rangekey"
)

func configureForSharedStorage(opts *pebble.Options, remoteStorage remote.Storage) error {
	opts.Experimental.RemoteStorage = remote.MakeSimpleFactory(map[remote.Locator]remote.Storage{
		"": remoteStorage,
	})
	opts.Experimental.CreateOnShared = remote.CreateOnSharedLower
	opts.Experimental.CreateOnSharedLocator = ""
	return nil
}

// iterateReplicaKeySpansShared iterates over the range's user key span,
// skipping any keys present in shared files. It calls the appropriate visitor
// function for the type of key visited, namely, point keys, range deletes and
// range keys. Shared files that are skipped during this iteration are also
// surfaced through a dedicated visitor. Note that this method only iterates
// over a range's user key span; IterateReplicaKeySpans must be called to
// iterate over the other key spans.
//
// Must use a reader with consistent iterators.
func iterateReplicaKeySpansShared(
	ctx context.Context,
	desc *roachpb.RangeDescriptor,
	st *cluster.Settings,
	_ uuid.UUID,
	reader storage.Reader,
	visitPoint func(key *pebble.InternalKey, val pebble.LazyValue, info pebble.IteratorLevel) error,
	visitRangeDel func(start, end []byte, seqNum pebble.SeqNum) error,
	visitRangeKey func(start, end []byte, keys []rangekey.Key) error,
	visitSharedFile func(sst *pebble.SharedSSTMeta) error,
	visitExternalFile func(sst *pebble.ExternalFile) error,
) error {
	if !reader.ConsistentIterators() {
		panic("reader must provide consistent iterators")
	}
	if err := utilccl.CheckEnterpriseEnabled(st, "disaggregated shared storage"); err != nil {
		// NB: ScanInternal returns ErrInvalidSkipSharedIteration if we can't do
		// a skip-shared iteration. Return the same error here so the caller can
		// fall back to regular, non-shared snapshots.
		return pebble.ErrInvalidSkipSharedIteration
	}
	spans := rditer.Select(desc.RangeID, rditer.SelectOpts{
		ReplicatedSpansFilter: rditer.ReplicatedSpansUserOnly,
		ReplicatedBySpan:      desc.RSpan(),
	})
	span := spans[0]
	return reader.ScanInternal(ctx, span.Key, span.EndKey, visitPoint, visitRangeDel, visitRangeKey, visitSharedFile, visitExternalFile)
}

func init() {
	storage.ConfigureForSharedStorage = configureForSharedStorage
	rditer.IterateReplicaKeySpansShared = iterateReplicaKeySpansShared
}
