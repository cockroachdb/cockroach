// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sqlbase

import (
	"bytes"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/pkg/errors"
)

// TranslateValueEncodingToSpan parses columns (which are a prefix of the
// columns of `idxDesc`) encoded with the "value" encoding and returns the
// parsed datums. It also reencodes them into a key as they would be for
// `idxDesc` (accounting for index dirs, interleaves, subpartitioning, etc).
//
// For a list partitioning, this returned key can be used as a prefix scan to
// select all rows that have the given columns as a prefix (this is true even if
// the list partitioning contains DEFAULT).
//
// Examples of the key returned for a list partitioning:
// - (1, 2) -> /table/index/1/2
// - (1, DEFAULT) -> /table/index/1
// - (DEFAULT, DEFAULT) -> /table/index
//
// For a range partitioning, this returned key can be used as a exclusive end
// key to select all rows strictly less than ones with the given columns as a
// prefix (this is true even if the range partitioning contains MAXVALUE).
//
// Examples of the key returned for a range partitioning:
// - (1, 2) -> /table/index/1/3
// - (1, MAXVALUE) -> /table/index/2
// - (DEFAULT, DEFAULT) -> (/table/index).PrefixEnd()
//
// If DEFAULT or MAXVALUE is encountered, the returned row (both reencoded and
// parsed datums) will be shorter then len(colIDs); specifically it will contain
// all the values up to but not including the special one.
//
// NB: It is required elsewhere and assumed here that if an entry for a list
// partitioning contains DEFAULT, everything in that entry "after" also has to
// be DEFAULT. So, (1, 2, DEFAULT) is valid but (1, DEFAULT, 2) is not.
// Similarly for range partitioning and MAXVALUE.
func TranslateValueEncodingToSpan(
	a *DatumAlloc,
	tableDesc *TableDescriptor,
	idxDesc *IndexDescriptor,
	partDesc *PartitioningDescriptor,
	valueEncBuf []byte,
	prefixDatums []tree.Datum,
) (tree.Datums, []byte, error) {
	if len(prefixDatums)+int(partDesc.NumColumns) > len(idxDesc.ColumnIDs) {
		return nil, nil, fmt.Errorf("not enough columns in index for this partitioning")
	}

	datums := make(tree.Datums, int(partDesc.NumColumns))
	specialIdx := -1

	colIDs := idxDesc.ColumnIDs[len(prefixDatums) : len(prefixDatums)+int(partDesc.NumColumns)]
	for i, colID := range colIDs {
		col, err := tableDesc.FindColumnByID(colID)
		if err != nil {
			return nil, nil, err
		}
		if _, dataOffset, _, typ, err := encoding.DecodeValueTag(valueEncBuf); err != nil {
			return nil, nil, errors.Wrap(err, "decoding")
		} else if typ == encoding.NotNull {
			if specialIdx == -1 {
				specialIdx = i
			}
			valueEncBuf = valueEncBuf[dataOffset:]
			continue
		}
		datums[i], valueEncBuf, err = DecodeTableValue(a, col.Type.ToDatumType(), valueEncBuf)
		if err != nil {
			return nil, nil, errors.Wrap(err, "decoding")
		}
		if specialIdx != -1 {
			if len(partDesc.List) > 0 && len(partDesc.Range) == 0 {
				return nil, nil, errors.Errorf(
					"non-DEFAULT value (%s) not allowed after DEFAULT", datums[i])
			} else if len(partDesc.Range) > 0 && len(partDesc.List) == 0 {
				return nil, nil, errors.Errorf(
					"non-MAXVALUE value (%s) not allowed after MAXVALUE", datums[i])
			} else {
				return nil, nil, errors.Errorf("unknown partition type")
			}
		}
	}
	if len(valueEncBuf) > 0 {
		return nil, nil, fmt.Errorf("superfluous data in encoded value")
	}
	if specialIdx != -1 {
		datums = datums[:specialIdx]
	}

	allDatums := append(prefixDatums, datums...)
	colMap := make(map[ColumnID]int, len(allDatums))
	for i := range allDatums {
		colMap[idxDesc.ColumnIDs[i]] = i
	}

	indexKeyPrefix := MakeIndexKeyPrefix(tableDesc, idxDesc.ID)
	key, _, err := EncodePartialIndexKey(
		tableDesc, idxDesc, len(allDatums), colMap, allDatums, indexKeyPrefix)
	if err != nil {
		return nil, nil, err
	}

	// Currently, key looks something like `/table/index/1`. Given a range
	// partitioning of (1), we're done. This can be used as the exclusive end
	// key of a scan to fetch all rows strictly less than (1).
	//
	// If `specialIdx` is not the sentinel, then we're actually in a case like
	// `(1, MAXVALUE, ..., MAXVALUE)`. Since this index could have a descending
	// nullable column, we can't rely on `/table/index/1/0xff` to be _strictly_
	// larger than everything it should match. Instead, we need `PrefixEnd()`.
	// This also intuitively makes sense; we're essentially a key that is
	// guaranteed to be less than `(2, MINVALUE, ..., MINVALUE)`.
	if specialIdx != -1 && len(partDesc.Range) > 0 {
		key = roachpb.Key(key).PrefixEnd()
	}

	return datums, key, nil
}

func printPartitioningPrefix(datums []tree.Datum, numColumns int, s string) string {
	var buf bytes.Buffer
	PrintPartitioningTuple(&buf, datums, numColumns, s)
	return buf.String()
}

// PrintPartitioningTuple prints the first `numColumns` datums in a partitioning
// tuple to `buf`. If `numColumns >= len(datums)`, then `s` (which is expected
// to be DEFAULT or MAXVALUE) is used to fill.
func PrintPartitioningTuple(buf *bytes.Buffer, datums []tree.Datum, numColumns int, s string) {
	for i := 0; i < numColumns; i++ {
		if i > 0 {
			buf.WriteString(", ")
		}
		if i < len(datums) {
			buf.WriteString(tree.AsString(datums[i]))
		} else {
			buf.WriteString(s)
		}
	}
}
