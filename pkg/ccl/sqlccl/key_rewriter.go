// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Cockroach Community Licence (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/pkg/ccl/LICENSE

package sqlccl

import (
	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/pkg/errors"
)

// MakeKeyRewriterForNewTableIDs creates a KeyRewriter that rewrites all keys
// from a set of tables to have a new tableID. For dependency reasons, the
// implementation of the matching is in storageccl, but the interesting
// constructor is here.
func MakeKeyRewriterForNewTableIDs(
	tables []*sqlbase.TableDescriptor, newTableIDs map[sqlbase.ID]sqlbase.ID,
) (storageccl.KeyRewriter, error) {
	var kr storageccl.KeyRewriter
	for _, table := range tables {
		newTableID, ok := newTableIDs[table.ID]
		if !ok {
			return nil, errors.Errorf("missing new table ID for [%d] %q", table.ID, table.Name)
		}
		kr = append(kr, MakeKeyRewriterForNewTableID(table, newTableID)...)
	}
	return kr, nil
}

// MakeKeyRewriterForNewTableID creates a KeyRewriter that rewrites all keys
// from a table to have a new tableID. For dependency reasons, the
// implementation of the matching is in storageccl, but the interesting
// constructor is here.
func MakeKeyRewriterForNewTableID(
	desc *sqlbase.TableDescriptor, newTableID sqlbase.ID,
) storageccl.KeyRewriter {
	newDesc := *desc
	newDesc.ID = newTableID

	// The PrefixEnd() of index 1 is the same as the prefix of index 2, so use a
	// map to avoid duplicating entries.
	prefixes := make(map[string]struct{})

	var kr storageccl.KeyRewriter
	for _, index := range desc.AllNonDropIndexes() {
		oldPrefix := roachpb.Key(sqlbase.MakeIndexKeyPrefix(desc, index.ID))
		newPrefix := roachpb.Key(sqlbase.MakeIndexKeyPrefix(&newDesc, index.ID))
		if _, ok := prefixes[string(oldPrefix)]; !ok {
			prefixes[string(oldPrefix)] = struct{}{}
			kr = append(kr, roachpb.KeyRewrite{
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
			kr = append(kr, roachpb.KeyRewrite{
				OldPrefix: oldPrefix,
				NewPrefix: newPrefix,
			})
		}
	}
	return kr
}
