// Copyright 2016 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/LICENSE

package sqlccl

import (
	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
)

// MakeKeyRewriterForNewTableID creates a KeyRewriter that rewrites all keys
// from a table to have a new tableID. For dependency reasons, the
// implementation of the matching is in storageccl, but the interesting
// constructor is here.
func MakeKeyRewriterForNewTableID(
	desc *sqlbase.TableDescriptor, newTableID sqlbase.ID,
) storageccl.PrefixRewriter {
	newDesc := *desc
	newDesc.ID = newTableID

	// The PrefixEnd() of index 1 is the same as the prefix of index 2, so use a
	// map to avoid duplicating entries.
	prefixes := make(map[string]struct{})

	var kr storageccl.PrefixRewriter
	for _, index := range desc.AllNonDropIndexes() {
		oldPrefix := roachpb.Key(makeKeyRewriterPrefixIgnoringInterleaved(desc.ID, index.ID))
		newPrefix := roachpb.Key(makeKeyRewriterPrefixIgnoringInterleaved(newDesc.ID, index.ID))
		if _, ok := prefixes[string(oldPrefix)]; !ok {
			prefixes[string(oldPrefix)] = struct{}{}
			kr = append(kr, storageccl.PrefixRewrite{
				OldPrefix: oldPrefix,
				NewPrefix: newPrefix,
			})
		}
		// All the encoded data for a index will have the prefix just added, but
		// if you need to translate a half-open range describing that prefix
		// (and we do), the prefix end needs to be in the map too.
		oldPrefix = oldPrefix.PrefixEnd()
		newPrefix = newPrefix.PrefixEnd()
		if _, ok := prefixes[string(oldPrefix)]; !ok {
			prefixes[string(oldPrefix)] = struct{}{}
			kr = append(kr, storageccl.PrefixRewrite{
				OldPrefix: oldPrefix,
				NewPrefix: newPrefix,
			})
		}
	}
	return kr
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
