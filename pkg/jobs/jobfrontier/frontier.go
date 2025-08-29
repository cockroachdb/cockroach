// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jobfrontier

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/span"
	"github.com/cockroachdb/errors"
)

// ShardFunc is a function that determines which shard a span belongs to.
// It returns the shard key suffix to append to the base info key.
// If sharding is not needed, this can be nil.
type ShardFunc func(span roachpb.Span) string

// Unsharded is a nil ShardFunc that signals to not shard at all.
var Unsharded ShardFunc = nil

const frontierPrefix = "frontier/"
const shardSep = "_"

// Get loads a complete frontier from persistent storage and returns it.
//
// The returned frontier will contain all spans and their timestamps that were
// previously stored via Store(). The spans are derived from the persisted data.
func Get(
	ctx context.Context,
	txn isql.Txn,
	jobID jobspb.JobID,
	name string,
) (span.Frontier, bool, error) {
	infoStorage := jobs.InfoStorageForJob(txn, jobID)

	// Read all persisted entries, both as entries and as plain spans; we need the
	// latter form to construct the frontier and the former to advance it.
	// TODO(dt): we could avoid duplicate allocation here if we added an API to
	// construct frontier directly from entries.
	var entries []frontierEntry
	var spans []roachpb.Span

	keyPrefix := frontierPrefix + name + shardSep

	var found bool
	if err := infoStorage.Iterate(ctx, keyPrefix, func(_ string, value []byte) error {
		found = true
		var r jobspb.ResolvedSpans
		if err := protoutil.Unmarshal(value, &r); err != nil {
			return err
		}
		for _, sp := range r.ResolvedSpans {
			entries = append(entries, frontierEntry{Span: sp.Span, Timestamp: sp.Timestamp})
			spans = append(spans, sp.Span)
		}
		return nil
	}); err != nil || !found {
		return nil, false, err
	}

	// Construct frontier to track the set of spans found and advance it to their
	// persisted timestamps. This implies we perist zero-timestamp spans to keep
	// the set of tracked spans even if they do not have progress.
	frontier, err := span.MakeFrontier(spans...)
	if err != nil {
		return nil, false, err
	}
	for _, entry := range entries {
		if _, err := frontier.Forward(entry.Span, entry.Timestamp); err != nil {
			return nil, false, err
		}
	}

	return frontier, true, nil
}

// Store persists a frontier's current state to storage.
//
// All span entries in the frontier and their current timestamps will be
// persisted. Any previously stored frontier data under the same key(s) will
// be replaced.
//
// The shardFunc, if non-nil, will be used to bucket spans into 'shards' that
// are stored in separate info keys under the passed name prefix. If nil, all
// spans are stored in one key, using the shard name "full".
//
// InfoStorage keys are prefixed with "frontier/", and all shards, including the
// single, implicit "full" shard for unsharded, are under the passed name, so a
// full key would be "frontier/{name}_full".
func Store(
	ctx context.Context,
	txn isql.Txn,
	jobID jobspb.JobID,
	name string,
	shardFunc ShardFunc,
	frontier span.Frontier,
) error {
	infoStorage := jobs.InfoStorageForJob(txn, jobID)

	// Wipe any existing frontier shards, since we cannot rely on the shard func
	// to return the same set of shards to guarantee a full overwrite. Slightly
	// annoying that each shard's info store write will also issue a delete during
	// its write that is duplicative since we already deleted everything here, but
	// we don't really have a choice. We could specialize non-sharded/fixed-shard
	// frontiers (non-sharded is just fixed=1), where the write call would handle
	// deleting any prior entry, but doesn't seem worth it: you need a promise it
	// does not become sharded later, so would probably want to be a separate API.
	if err := deleteEntries(ctx, infoStorage, name); err != nil {
		return err
	}

	// Collect all frontier entries
	var all []jobspb.ResolvedSpan
	all = make([]jobspb.ResolvedSpan, 0, frontier.Len())
	for spanEntry, timestamp := range frontier.Entries() {
		all = append(all, jobspb.ResolvedSpan{
			Span:      spanEntry,
			Timestamp: timestamp,
		})
	}

	// If sharding is not enabled, store all entries under a "full" shard.
	if shardFunc == nil {
		return storeEntries(ctx, infoStorage, name, "full", all)
	}

	// Group entries by shard.
	shardGroups := make(map[string][]jobspb.ResolvedSpan)
	for _, sp := range all {
		shardKey := shardFunc(sp.Span)
		shardGroups[shardKey] = append(shardGroups[shardKey], sp)
	}
	for shardKey, entries := range shardGroups {
		if err := storeEntries(ctx, infoStorage, name, shardKey, entries); err != nil {
			return err
		}
	}
	return nil
}

func storeEntries(
	ctx context.Context, infoStorage jobs.InfoStorage, name, shard string, entries []jobspb.ResolvedSpan,
) error {
	data, err := protoutil.Marshal(&jobspb.ResolvedSpans{ResolvedSpans: entries})
	if err != nil {
		return errors.Wrap(err, "failed to serialize frontier entries")
	}
	key := fmt.Sprintf("%s%s%s%s", frontierPrefix, name, shardSep, shard)
	return infoStorage.Write(ctx, key, data)
}

// Delete removes a persisted frontier by the given name for the given job.
func Delete(
	ctx context.Context,
	txn isql.Txn,
	jobID jobspb.JobID,
	name string,
) error {
	infoStorage := jobs.InfoStorageForJob(txn, jobID)
	return deleteEntries(ctx, infoStorage, name)
}

func deleteEntries(ctx context.Context, infoStorage jobs.InfoStorage, name string) error {
	startKey := frontierPrefix + name + shardSep
	endKey := frontierPrefix + name + string(rune(shardSep[0])+1)
	return infoStorage.DeleteRange(ctx, startKey, endKey, 0 /* no limit */)
}

// frontierEntry represents a single persisted frontier entry.
// This is used internally for serialization but may be useful for testing.
type frontierEntry struct {
	Span      roachpb.Span
	Timestamp hlc.Timestamp
}
