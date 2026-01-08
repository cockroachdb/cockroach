// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backfill

import (
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// SSTManifestBuffer accumulates SST metadata emitted during an index backfill
// map stage. Callers may append new manifests and snapshot the buffer when
// persisting checkpoints so they avoid copying slices manually throughout the
// ingestion pipeline.
type SSTManifestBuffer struct {
	syncutil.Mutex
	manifests []jobspb.IndexBackfillSSTManifest
	dirty     bool
}

// NewSSTManifestBuffer constructs a buffer initialized with the provided
// manifests.
func NewSSTManifestBuffer(initial []jobspb.IndexBackfillSSTManifest) *SSTManifestBuffer {
	buf := &SSTManifestBuffer{}
	buf.manifests = append(buf.manifests, initial...)
	if len(initial) > 0 {
		buf.dirty = true
	}
	return buf
}

func (b *SSTManifestBuffer) snapshotLocked() []jobspb.IndexBackfillSSTManifest {
	return append([]jobspb.IndexBackfillSSTManifest(nil), b.manifests...)
}

// Snapshot returns a copy of the buffered manifests.
func (b *SSTManifestBuffer) Snapshot() []jobspb.IndexBackfillSSTManifest {
	b.Lock()
	defer b.Unlock()
	return b.snapshotLocked()
}

// Append adds the provided manifests to the buffer and returns the updated
// snapshot.
func (b *SSTManifestBuffer) Append(
	newManifests []jobspb.IndexBackfillSSTManifest,
) []jobspb.IndexBackfillSSTManifest {
	if len(newManifests) == 0 {
		return b.Snapshot()
	}
	b.Lock()
	defer b.Unlock()
	b.manifests = append(b.manifests, newManifests...)
	b.dirty = true
	return b.snapshotLocked()
}

// Dirty reports whether the buffer has accumulated manifests that have not been
// snapshotted since the last call to SnapshotAndMarkClean.
func (b *SSTManifestBuffer) Dirty() bool {
	if b == nil {
		return false
	}
	b.Lock()
	defer b.Unlock()
	return b.dirty
}

// SnapshotAndMarkClean returns a copy of the buffered manifests and clears the
// dirty flag. Callers should invoke this once they have persisted the snapshot.
func (b *SSTManifestBuffer) SnapshotAndMarkClean() []jobspb.IndexBackfillSSTManifest {
	if b == nil {
		return nil
	}
	b.Lock()
	defer b.Unlock()
	b.dirty = false
	return b.snapshotLocked()
}

// StripTenantPrefixFromSSTManifests normalizes SST manifest metadata by
// removing tenant prefixes before persisting it in job state. This matches the
// CompletedSpans handling and keeps job progress tenant-agnostic.
func StripTenantPrefixFromSSTManifests(
	codec keys.SQLCodec, manifests []jobspb.IndexBackfillSSTManifest,
) ([]jobspb.IndexBackfillSSTManifest, error) {
	ret := make([]jobspb.IndexBackfillSSTManifest, len(manifests))
	for i, out := range manifests {
		ret[i].URI = out.URI
		ret[i].FileSize = out.FileSize
		if out.WriteTimestamp != nil {
			ts := *out.WriteTimestamp
			ret[i].WriteTimestamp = &ts
		}
		if out.Span != nil {
			stripped, err := stripTenantPrefixFromSpans(codec, *out.Span)
			if err != nil {
				return nil, err
			}
			ret[i].Span = &stripped
		}
		if len(out.RowSample) > 0 {
			key, err := codec.StripTenantPrefix(out.RowSample)
			if err != nil {
				return nil, err
			}
			ret[i].RowSample = append(roachpb.Key(nil), key...)
		}
	}
	return ret, nil
}

// AddTenantPrefixToSSTManifests rehydrates manifests with the executing
// processor's tenant prefix so they can be used against the local keyspace.
func AddTenantPrefixToSSTManifests(
	codec keys.SQLCodec, manifests []jobspb.IndexBackfillSSTManifest,
) []jobspb.IndexBackfillSSTManifest {
	ret := make([]jobspb.IndexBackfillSSTManifest, len(manifests))
	for i, out := range manifests {
		ret[i].URI = out.URI
		ret[i].FileSize = out.FileSize
		if out.WriteTimestamp != nil {
			ts := *out.WriteTimestamp
			ret[i].WriteTimestamp = &ts
		}
		if out.Span != nil {
			sp := addTenantPrefixToSpan(codec, *out.Span)
			ret[i].Span = &sp
		}
		if len(out.RowSample) > 0 {
			prefixed := make(roachpb.Key, 0, len(codec.TenantPrefix())+len(out.RowSample))
			prefixed = append(prefixed, codec.TenantPrefix()...)
			prefixed = append(prefixed, out.RowSample...)
			ret[i].RowSample = prefixed
		}
	}
	return ret
}

func stripTenantPrefixFromSpans(codec keys.SQLCodec, span roachpb.Span) (roachpb.Span, error) {
	var err error
	var out roachpb.Span
	if out.Key, err = codec.StripTenantPrefix(span.Key); err != nil {
		return roachpb.Span{}, err
	}
	if out.EndKey, err = codec.StripTenantPrefix(span.EndKey); err != nil {
		return roachpb.Span{}, err
	}
	return out, nil
}

func addTenantPrefixToSpan(codec keys.SQLCodec, span roachpb.Span) roachpb.Span {
	prefix := codec.TenantPrefix()
	prefix = prefix[:len(prefix):len(prefix)]
	out := roachpb.Span{
		Key:    append(prefix, span.Key...),
		EndKey: append(prefix, span.EndKey...),
	}
	return out
}
