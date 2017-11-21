// Copyright 2016 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package storageccl

import (
	"bytes"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/pkg/errors"
)

// prefixRewrite holds information for a single []byte replacement of a prefix.
type prefixRewrite struct {
	OldPrefix []byte
	NewPrefix []byte
}

// prefixRewriter is a matcher for an ordered list of pairs of byte prefix
// rewrite rules.
type prefixRewriter []prefixRewrite

// RewriteKey modifies key using the first matching rule and returns
// it. If no rules matched, returns false and the original input key.
func (p prefixRewriter) rewriteKey(key []byte) ([]byte, bool) {
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
	prefixes prefixRewriter
	descs    map[sqlbase.ID]*sqlbase.TableDescriptor
}

// MakeKeyRewriter creates a KeyRewriter. This includes a simple []byte
// prefix rewriter to rewrite table IDs including prefix ends, and table
// descriptor data to traverse interleaved keys to child tables.
func MakeKeyRewriter(rekeys []roachpb.ImportRequest_TableRekey) (*KeyRewriter, error) {
	var prefixes prefixRewriter
	descs := make(map[sqlbase.ID]*sqlbase.TableDescriptor)
	for _, rekey := range rekeys {
		var desc sqlbase.Descriptor
		if err := protoutil.Unmarshal(rekey.NewDesc, &desc); err != nil {
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
				prefixes = append(prefixes, prefixRewrite{
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
				prefixes = append(prefixes, prefixRewrite{
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

// RewriteKey modifies key (possibly in place), changing all table IDs to
// their new value, including any interleaved table children and prefix
// ends. This function works by inspecting the key for table and index IDs,
// then uses the corresponding table and index descriptors to determine if
// interleaved data is present and if it is, to find the next prefix of an
// interleaved child, then calls itself recursively until all interleaved
// children have been rekeyed.
func (kr *KeyRewriter) RewriteKey(key []byte) ([]byte, bool, error) {
	// Fetch the original table ID for descriptor lookup. Ignore errors because
	// they will be caught later on if tableID isn't in descs or kr doesn't
	// perform a rewrite.
	_, tableID, _ := encoding.DecodeUvarintAscending(key)
	// Rewrite the first table ID.
	key, ok := kr.prefixes.rewriteKey(key)
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
	var skipCols int
	for _, ancestor := range idx.Interleave.Ancestors {
		skipCols += int(ancestor.SharedPrefixLen)
	}
	for i := 0; i < len(colIDs)-skipCols; i++ {
		n, err := encoding.PeekLength(k)
		if err != nil {
			return nil, false, err
		}
		k = k[n:]
	}
	// We might have an interleaved key.
	k, ok = encoding.DecodeIfInterleavedSentinel(k)
	if !ok {
		return key, true, nil
	}
	prefix := key[:len(key)-len(k)]
	k, ok, err = kr.RewriteKey(k)
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

// RewriteSpan returns a new span with both Key and EndKey rewritten using
// RewriteKey. Span start keys for the primary index will be rewritten to
// contain just the table ID. That is, /Table/51/1 -> /Table/51. An error
// is returned if either was not matched for rewrite.
func (kr *KeyRewriter) RewriteSpan(span roachpb.Span) (roachpb.Span, error) {
	newKey, ok, err := kr.RewriteKey(append([]byte(nil), span.Key...))
	if err != nil {
		return roachpb.Span{}, errors.Wrapf(err, "could not rewrite key: %s", span.Key)
	}
	if !ok {
		return roachpb.Span{}, errors.Errorf("could not rewrite key: %s", span.Key)
	}
	// Modify all spans that begin at the primary index to instead begin at the
	// start of the table. That is, change a span start key from /Table/51/1 to
	// /Table/51. Otherwise a permanently empty span at /Table/51-/Table/51/1
	// will be created.
	if b, id, idx, err := sqlbase.DecodeTableIDIndexID(newKey); err != nil {
		return roachpb.Span{}, errors.Wrapf(err, "could not rewrite key: %s", span.Key)
	} else if idx == 1 && len(b) == 0 {
		newKey = keys.MakeTablePrefix(uint32(id))
	}
	newEndKey, ok, err := kr.RewriteKey(append([]byte(nil), span.EndKey...))
	if err != nil {
		return roachpb.Span{}, errors.Wrapf(err, "could not rewrite key: %s", span.EndKey)
	}
	if !ok {
		return roachpb.Span{}, errors.Errorf("could not rewrite key: %s", span.EndKey)
	}
	return roachpb.Span{Key: newKey, EndKey: newEndKey}, nil
}
