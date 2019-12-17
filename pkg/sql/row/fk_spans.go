// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package row

import (
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// spanForValues produce access spans for a single FK constraint and a
// tuple of columns.
func (f fkExistenceCheckBaseHelper) spanForValues(values tree.Datums) (roachpb.Span, error) {
	if values == nil {
		key := roachpb.Key(f.searchPrefix)
		return roachpb.Span{Key: key, EndKey: key.PrefixEnd()}, nil
	}
	return FKCheckSpan(
		f.searchTable.TableDesc(), f.searchIdx, f.prefixLen, f.ids, values, f.searchPrefix,
	)
}

// FKCheckSpan returns a span that can be scanned to ascertain existence of a
// specific row in a given index.
func FKCheckSpan(
	tableDesc *sqlbase.TableDescriptor,
	index *sqlbase.IndexDescriptor,
	numCols int,
	colMap map[sqlbase.ColumnID]int,
	values []tree.Datum,
	keyPrefix []byte,
) (roachpb.Span, error) {
	// If we are scanning the (entire) primary key, only scan family 0 (which is
	// always present).
	// TODO(radu): this logic will need to be improved when secondary indexes also
	// conform to families.
	if index.ID == tableDesc.PrimaryIndex.ID && numCols == len(index.ColumnIDs) {
		// This code is equivalent to calling EncodePartialIndexSpan followed by
		// MakeFamilyKey but saves an unnecessary allocation.
		key, _, err := sqlbase.EncodePartialIndexKey(
			tableDesc, index, numCols, colMap, values, keyPrefix,
		)
		if err != nil {
			return roachpb.Span{}, err
		}
		key = keys.MakeFamilyKey(key, 0)
		return roachpb.Span{Key: key, EndKey: roachpb.Key(key).PrefixEnd()}, nil
	}

	span, _, err := sqlbase.EncodePartialIndexSpan(
		tableDesc, index, numCols, colMap, values, keyPrefix,
	)
	return span, err
}
