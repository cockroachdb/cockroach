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
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
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

// MakeKeyRewriterFromRekeys makes a KeyRewriter from Rekey protos.
func MakeKeyRewriterFromRekeys(rekeys []roachpb.ImportRequest_TableRekey) (*KeyRewriter, error) {
	descs := make(map[sqlbase.ID]*sqlbase.TableDescriptor)
	for _, rekey := range rekeys {
		var desc sqlbase.Descriptor
		if err := protoutil.Unmarshal(rekey.NewDesc, &desc); err != nil {
			return nil, errors.Wrapf(err, "unmarshalling rekey descriptor for old table id %d", rekey.OldID)
		}
		table := desc.Table(hlc.Timestamp{})
		if table == nil {
			return nil, errors.New("expected a table descriptor")
		}
		descs[sqlbase.ID(rekey.OldID)] = table
	}
	return MakeKeyRewriter(descs)
}

// MakeKeyRewriter makes a KeyRewriter from a map of descs keyed by original ID.
func MakeKeyRewriter(descs map[sqlbase.ID]*sqlbase.TableDescriptor) (*KeyRewriter, error) {
	var prefixes prefixRewriter
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
	return keys.TODOSQLCodec.IndexPrefix(uint32(tableID), uint32(indexID))
}

// RewriteKey modifies key (possibly in place), changing all table IDs to their
// new value, including any interleaved table children and prefix ends. This
// function works by inspecting the key for table and index IDs, then uses the
// corresponding table and index descriptors to determine if interleaved data is
// present and if it is, to find the next prefix of an interleaved child, then
// calls itself recursively until all interleaved children have been rekeyed. If
// it encounters a table ID for which it does not have a configured rewrite, it
// returns the prefix of the key that was rewritten key. The returned boolean
// is true if and only if all of the table IDs found in the key were rewritten.
// If isFromSpan is true, failures in value decoding are assumed to be due to
// valid span manipulations, like PrefixEnd or Next having altered the trailing
// byte(s) to corrupt the value encoding -- in such a case we will not be able
// to decode the value (to determine how much further to scan for table IDs) but
// we can assume that since these manipulations are only done to the trailing
// byte that we're likely at the end anyway and do not need to search for any
// further table IDs to replace.
func (kr *KeyRewriter) RewriteKey(key []byte, isFromSpan bool) ([]byte, bool, error) {
	// Fetch the original table ID for descriptor lookup. Ignore errors because
	// they will be caught later on if tableID isn't in descs or kr doesn't
	// perform a rewrite.
	_, tableID, _ := keys.TODOSQLCodec.DecodeTablePrefix(key)
	// Rewrite the first table ID.
	key, ok := kr.prefixes.rewriteKey(key)
	if !ok {
		return nil, false, nil
	}
	desc := kr.descs[sqlbase.ID(tableID)]
	if desc == nil {
		return nil, false, errors.Errorf("missing descriptor for table %d", tableID)
	}
	// Check if this key may have interleaved children.
	k, _, indexID, err := keys.TODOSQLCodec.DecodeIndexPrefix(key)
	if err != nil {
		return nil, false, err
	}
	if len(k) == 0 {
		// If there isn't any more data, we are at some split boundary.
		return key, true, nil
	}
	idx, err := desc.FindIndexByID(sqlbase.IndexID(indexID))
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
			// PeekLength, and key decoding in general, can fail when reading the last
			// value from a key that is coming from a span. Keys in spans are often
			// altered e.g. by calling Next() or PrefixEnd() to ensure a given span is
			// inclusive or for other reasons, but the manipulations sometimes change
			// the encoded bytes, meaning they can no longer successfully decode as
			// back to the original values. This is OK when span boundaries mostly are
			// only required to be even divisions of keyspace, but when we try to go
			// back to interpreting them as keys, it can fall apart. Partitioning a
			// table (and applying zone configs) eagerly creates splits at the defined
			// partition boundaries, using PrefixEnd for their ends, resulting in such
			// spans.
			//
			// Fortunately, the only common span manipulations are to the trailing
			// byte of a key (e.g. incrementing or appending a null) so for our needs
			// here, if we fail to decode because of one of those manipulations, we
			// can assume that we are at the end of the key as far as fields where a
			// table ID which needs to be replaced can appear and consider the rewrite
			// of this key as being compelted successfully.
			//
			// Finally unlike key rewrites of actual row-data, span rewrites do not
			// need to be perfect: spans are only rewritten for use in pre-splitting
			// and work distribution, so even if it turned out that this assumption
			// was incorrect, it could cause a performance degradation but does not
			// pose a correctness risk.
			if isFromSpan {
				return key, true, nil
			}
			return nil, false, err
		}
		k = k[n:]
		// Check if we ran out of key before getting to an interleave child?
		if len(k) == 0 {
			return key, true, nil
		}
	}
	// We might have an interleaved key.
	k, ok = encoding.DecodeIfInterleavedSentinel(k)
	if !ok {
		return key, true, nil
	}
	prefix := key[:len(key)-len(k)]
	k, ok, err = kr.RewriteKey(k, isFromSpan)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		// The interleaved child was not rewritten, skip this row.
		return prefix, false, nil
	}
	key = append(prefix, k...)
	return key, true, nil
}
