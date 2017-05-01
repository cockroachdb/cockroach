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
	"errors"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
)

// KeyRewriter is an matcher for an ordered list of pairs of byte prefix rewrite
// rules. For dependency reasons, the implementation of the matching is here,
// but the interesting constructor is in sqlccl.
type KeyRewriter []roachpb.KeyRewrite

// RewriteKey modifies key using the first matching rule and returns it. If no
// rules matched, returns false and the original input key.
func (kr KeyRewriter) RewriteKey(key []byte) ([]byte, bool) {
	for _, rewrite := range kr {
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

// RewriteInterleavedKey modifies key similar to RewriteKey, but is also
// able to account for interleaved tables.
func RewriteInterleavedKey(kr KeyRewriter, descs map[sqlbase.ID]*sqlbase.TableDescriptor, skipKeys int, key []byte) ([]byte, bool, error) {
	// Fetch the original table ID for descriptor lookup. Ignore errors because
	// they will be caught later on if tableID isn't in descs or kr doesn't
	// perform a rewrite.
	_, tableID, _ := encoding.DecodeUvarintAscending(key)
	// Rewrite the first table ID.
	key, ok := kr.RewriteKey(key)
	if !ok {
		return key, false, nil
	}
	desc := descs[sqlbase.ID(tableID)]
	if desc == nil {
		return nil, false, errors.New("missing table descriptor")
	}
	// Check if this key may have interleaved children.
	k, _, indexID, err := sqlbase.DecodeTableIDIndexID(key)
	if err != nil {
		return nil, false, err
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
		return nil, false, errors.New("interleaved secondary indexes not supported")
	}
	colIDs, dirs := idx.FullColumnIDs()
	vals, err := sqlbase.MakeEncodedKeyVals(desc, colIDs)
	if err != nil {
		return nil, false, err
	}
	k, err = sqlbase.DecodeKeyVals(nil, vals[skipKeys:], dirs[skipKeys:], k)
	if err != nil {
		return nil, false, err
	}
	// If we get a NotNULL, it's interleaved.
	k, ok = encoding.DecodeIfNotNull(k)
	if ok {
		prefix := key[:len(key)-len(k)]
		k, ok, err = RewriteInterleavedKey(kr, descs, len(vals), k)
		if err != nil {
			return nil, false, err
		}
		if !ok {
			// The interleaved child was not rewritten, skip this row.
			return key, false, nil
		}
		key = append(prefix, k...)
	}
	return key, true, nil
}
