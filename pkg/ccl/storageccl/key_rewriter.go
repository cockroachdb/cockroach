// Copyright 2016 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/LICENSE

package storageccl

import (
	"bytes"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/pkg/errors"
)

// PrefixRewrite holds information for a single byte replacement of a prefix.
type PrefixRewrite struct {
	OldPrefix []byte
	NewPrefix []byte
}

// PrefixRewriter is a matcher for an ordered list of pairs of byte prefix
// rewrite rules. For dependency reasons, the implementation of the matching
// is here, but the interesting constructor is in sqlccl.
type PrefixRewriter []PrefixRewrite

// RewriteKey modifies key using the first matching rule and returns
// it. If no rules matched, returns false and the original input key.
func (p PrefixRewriter) RewriteKey(key []byte) ([]byte, bool) {
	for _, rewrite := range p {
		if bytes.HasPrefix(key, rewrite.OldPrefix) {
			if len(rewrite.OldPrefix) == len(rewrite.NewPrefix) {
				copy(key[:len(rewrite.OldPrefix)], rewrite.NewPrefix)
				return key, true
			}
			// TODO(dan): Special case when key's cap() is enough.
			newKey := make([]byte, 0, len(rewrite.NewPrefix)+len(key)-len(rewrite.OldPrefix))
			newKey = append(newKey, rewrite.NewPrefix...)
			newKey = append(newKey, key[len(rewrite.OldPrefix):]...)
			return newKey, true
		}
	}
	return key, false
}

// KeyRewriter rewrites old table IDs to new table IDs. It is able to descend
// into interleaved keys, and is able to function on partial keys for spans
// and splits.
type KeyRewriter struct {
	prefixes PrefixRewriter
	descs    map[sqlbase.ID]*sqlbase.TableDescriptor
}

// MakeKeyRewriter creates a KeyRewriter from the Rekeys field of an
// ImportRequest.
func MakeKeyRewriter(rekeys []roachpb.ImportRequest_TableRekey) (*KeyRewriter, error) {
	var prefixes PrefixRewriter
	descs := make(map[sqlbase.ID]*sqlbase.TableDescriptor)
	for _, rekey := range rekeys {
		var desc sqlbase.Descriptor
		if err := desc.Unmarshal(rekey.NewDesc); err != nil {
			return nil, errors.Wrapf(err, "unmarshalling rekey descriptor for old table id %d", rekey.OldID)
		}
		table := desc.GetTable()
		if table == nil {
			return nil, errors.New("expected a table descriptor")
		}
		descs[sqlbase.ID(rekey.OldID)] = table
	}
	seenPrefixes := make(map[string]bool)
	for oldID, desc := range descs {
		// The PrefixEnd() of index 1 is the same as the prefix of index 2, so use a
		// map to avoid duplicating entries.

		for _, index := range desc.AllNonDropIndexes() {
			oldPrefix := roachpb.Key(makeKeyRewriterPrefixIgnoringInterleaved(oldID, index.ID))
			newPrefix := roachpb.Key(makeKeyRewriterPrefixIgnoringInterleaved(desc.ID, index.ID))
			if !seenPrefixes[string(oldPrefix)] {
				seenPrefixes[string(oldPrefix)] = true
				prefixes = append(prefixes, PrefixRewrite{
					OldPrefix: oldPrefix,
					NewPrefix: newPrefix,
				})
			}
			// All the encoded data for a index will have the prefix just added, but
			// if you need to translate a half-open range describing that prefix
			// (and we do), the prefix end needs to be in the map too.
			oldPrefix = oldPrefix.PrefixEnd()
			newPrefix = newPrefix.PrefixEnd()
			if !seenPrefixes[string(oldPrefix)] {
				seenPrefixes[string(oldPrefix)] = true
				prefixes = append(prefixes, PrefixRewrite{
					OldPrefix: oldPrefix,
					NewPrefix: newPrefix,
				})
			}
		}
	}
	return &KeyRewriter{
		prefixes: prefixes,
		descs:    descs,
	}, nil
}

// makeKeyRewriterPrefixIgnoringInterleaved creates a table/index prefix for
// the given table and index IDs. sqlbase.MakeIndexKeyPrefix is a similar
// function, but it takes into account interleaved ancestors, which we don't
// want here.
func makeKeyRewriterPrefixIgnoringInterleaved(tableID sqlbase.ID, indexID sqlbase.IndexID) []byte {
	var key []byte
	key = encoding.EncodeUvarintAscending(key, uint64(tableID))
	key = encoding.EncodeUvarintAscending(key, uint64(indexID))
	return key
}

// RewriteKey modifies key similar to RewriteKey, but is also
// able to account for interleaved tables. This function works by inspecting
// the key for table and index IDs, then uses the corresponding table and
// index descriptors to determine if interleaved data is present and if it
// is, to find the next prefix of an interleaved child, then calls itself
// recursively until all interleaved children have been rekeyed.
func (kr *KeyRewriter) RewriteKey(key []byte) ([]byte, bool, error) {
	return kr.rewriteKey(0, key)
}

func (kr *KeyRewriter) rewriteKey(skipKeys int, key []byte) ([]byte, bool, error) {
	// Fetch the original table ID for descriptor lookup. Ignore errors because
	// they will be caught later on if tableID isn't in descs or kr doesn't
	// perform a rewrite.
	_, tableID, _ := encoding.DecodeUvarintAscending(key)
	// Rewrite the first table ID.
	key, ok := kr.prefixes.RewriteKey(key)
	if !ok {
		return key, false, nil
	}
	desc := kr.descs[sqlbase.ID(tableID)]
	if desc == nil {
		return nil, false, errors.Errorf("missing descriptor for table %d", tableID)
	}
	// Check if this key may have interleaved children.
	k, _, indexID, err := sqlbase.DecodeTableIDIndexID(key)
	if err != nil {
		return nil, false, err
	}
	if len(k) == 0 {
		// If there isn't any more data, we are at some split boundary.
		return key, true, nil
	}
	idx, err := desc.FindIndexByID(indexID)
	if err != nil {
		return nil, false, err
	}
	if len(idx.InterleavedBy) == 0 {
		// Not interleaved.
		return key, true, nil
	}
	// We do not support interleaved secondary indexes.
	if idx.ID != desc.PrimaryIndex.ID {
		return nil, false, errors.New("restoring interleaved secondary indexes not supported")
	}
	colIDs, _ := idx.FullColumnIDs()
	for i := 0; i < len(colIDs)-skipKeys; i++ {
		n, err := encoding.PeekLength(k)
		if err != nil {
			return nil, false, err
		}
		k = k[n:]
	}
	// If we get a NotNULL, it's interleaved.
	k, ok = encoding.DecodeIfNotNull(k)
	if !ok {
		return key, true, nil
	}
	prefix := key[:len(key)-len(k)]
	k, ok, err = kr.rewriteKey(len(colIDs), k)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		// The interleaved child was not rewritten, skip this row.
		return key, false, nil
	}
	key = append(prefix, k...)
	return key, true, nil
}
