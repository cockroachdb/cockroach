// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanconfigwatcher

import (
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

// rowDecoder decodes rows from the span configurations table. It's not safe for
// concurrent use.
type rowDecoder struct {
	alloc     rowenc.DatumAlloc
	colIdxMap catalog.TableColMap
}

// newRowDecoder instantiates a rowDecoder for the span configurations table.
func newRowDecoder() *rowDecoder {
	return &rowDecoder{
		colIdxMap: row.ColIDtoRowIndexFromCols(
			systemschema.SpanConfigurationsTable.PublicColumns(),
		),
	}
}

// Decode decodes a span config entry from the system.span_configurations table.
func (rd *rowDecoder) Decode(kv roachpb.KeyValue) (entry roachpb.SpanConfigEntry, _ error) {
	tbl := systemschema.SpanConfigurationsTable
	// First we need to decode the start_key field from the index key.
	{
		types := []*types.T{tbl.PublicColumns()[0].GetType()}
		startKeyRow := make([]rowenc.EncDatum, 1)
		_, matches, _, err := rowenc.DecodeIndexKey(
			keys.SystemSQLCodec, tbl, tbl.GetPrimaryIndex(),
			types, startKeyRow, nil, kv.Key,
		)
		if err != nil {
			return roachpb.SpanConfigEntry{}, errors.Wrap(err, "failed to decode key")
		}
		if !matches {
			return roachpb.SpanConfigEntry{}, errors.AssertionFailedf("system.span_configurations descriptor does not match key: %v", kv.Key)
		}
		if err := startKeyRow[0].EnsureDecoded(types[0], &rd.alloc); err != nil {
			return roachpb.SpanConfigEntry{}, err
		}
		entry.Span.Key = []byte(tree.MustBeDBytes(startKeyRow[0].Datum))
	}
	if !kv.Value.IsPresent() {
		return roachpb.SpanConfigEntry{}, errors.Errorf("missing value for start key: %s", entry.Span.Key)
	}

	// The remaining columns are stored as a family, packed with diff-encoded
	// column IDs followed by their values.
	{
		bytes, err := kv.Value.GetTuple()
		if err != nil {
			return roachpb.SpanConfigEntry{}, err
		}
		var colIDDiff uint32
		var lastColID descpb.ColumnID
		var res tree.Datum
		for len(bytes) > 0 {
			_, _, colIDDiff, _, err = encoding.DecodeValueTag(bytes)
			if err != nil {
				return roachpb.SpanConfigEntry{}, err
			}
			colID := lastColID + descpb.ColumnID(colIDDiff)
			lastColID = colID
			if idx, ok := rd.colIdxMap.Get(colID); ok {
				res, bytes, err = rowenc.DecodeTableValue(&rd.alloc, tbl.PublicColumns()[idx].GetType(), bytes)
				if err != nil {
					return roachpb.SpanConfigEntry{}, err
				}

				switch colID {
				case tbl.PublicColumns()[1].GetID(): // end_key
					entry.Span.EndKey = []byte(tree.MustBeDBytes(res))
				case tbl.PublicColumns()[2].GetID(): // config
					if err := protoutil.Unmarshal([]byte(tree.MustBeDBytes(res)), &entry.Config); err != nil {
						return roachpb.SpanConfigEntry{}, err
					}
				default:
					return roachpb.SpanConfigEntry{}, errors.Errorf("unknown column: %v", colID)
				}
			}
		}
	}

	return entry, nil
}
