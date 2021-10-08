// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rowenc

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/errors"
)

// PartitionSpecialValCode identifies a special value.
type PartitionSpecialValCode uint64

const (
	// PartitionDefaultVal represents the special DEFAULT value.
	PartitionDefaultVal PartitionSpecialValCode = 0
	// PartitionMaxVal represents the special MAXVALUE value.
	PartitionMaxVal PartitionSpecialValCode = 1
	// PartitionMinVal represents the special MINVALUE value.
	PartitionMinVal PartitionSpecialValCode = 2
)

func (c PartitionSpecialValCode) String() string {
	switch c {
	case PartitionDefaultVal:
		return (tree.DefaultVal{}).String()
	case PartitionMinVal:
		return (tree.PartitionMinVal{}).String()
	case PartitionMaxVal:
		return (tree.PartitionMaxVal{}).String()
	}
	panic("unreachable")
}

// PartitionTuple represents a tuple in a partitioning specification.
//
// It contains any number of true datums, stored in the Datums field, followed
// by any number of special partitioning values, represented by the Special and
// SpecialCount fields.
type PartitionTuple struct {
	Datums       tree.Datums
	Special      PartitionSpecialValCode
	SpecialCount int
}

func (t *PartitionTuple) String() string {
	f := tree.NewFmtCtx(tree.FmtSimple)
	f.WriteByte('(')
	for i := 0; i < len(t.Datums)+t.SpecialCount; i++ {
		if i > 0 {
			f.WriteString(", ")
		}
		if i < len(t.Datums) {
			f.FormatNode(t.Datums[i])
		} else {
			f.WriteString(t.Special.String())
		}
	}
	f.WriteByte(')')
	return f.CloseAndGetString()
}

// DecodePartitionTuple parses columns (which are a prefix of the columns of
// `idxDesc`) encoded with the "value" encoding and returns the parsed datums.
// It also reencodes them into a key as they would be for `idxDesc` (accounting
// for index dirs, interleaves, subpartitioning, etc).
//
// For a list partitioning, this returned key can be used as a prefix scan to
// select all rows that have the given columns as a prefix (this is true even if
// the list partitioning contains DEFAULT).
//
// Examples of the key returned for a list partitioning:
//   - (1, 2) -> /table/index/1/2
//   - (1, DEFAULT) -> /table/index/1
//   - (DEFAULT, DEFAULT) -> /table/index
//
// For a range partitioning, this returned key can be used as a exclusive end
// key to select all rows strictly less than ones with the given columns as a
// prefix (this is true even if the range partitioning contains MINVALUE or
// MAXVALUE).
//
// Examples of the key returned for a range partitioning:
//   - (1, 2) -> /table/index/1/3
//   - (1, MAXVALUE) -> /table/index/2
//   - (MAXVALUE, MAXVALUE) -> (/table/index).PrefixEnd()
//
// NB: It is checked here that if an entry for a list partitioning contains
// DEFAULT, everything in that entry "after" also has to be DEFAULT. So, (1, 2,
// DEFAULT) is valid but (1, DEFAULT, 2) is not. Similarly for range
// partitioning and MINVALUE/MAXVALUE.
func DecodePartitionTuple(
	a *DatumAlloc,
	codec keys.SQLCodec,
	tableDesc catalog.TableDescriptor,
	index catalog.Index,
	part catalog.Partitioning,
	valueEncBuf []byte,
	prefixDatums tree.Datums,
) (*PartitionTuple, []byte, error) {
	if len(prefixDatums)+part.NumColumns() > index.NumKeyColumns() {
		return nil, nil, fmt.Errorf("not enough columns in index for this partitioning")
	}

	t := &PartitionTuple{
		Datums: make(tree.Datums, 0, part.NumColumns()),
	}

	for i := len(prefixDatums); i < index.NumKeyColumns() && i < len(prefixDatums)+part.NumColumns(); i++ {
		colID := index.GetKeyColumnID(i)
		col, err := tableDesc.FindColumnWithID(colID)
		if err != nil {
			return nil, nil, err
		}
		if _, dataOffset, _, typ, err := encoding.DecodeValueTag(valueEncBuf); err != nil {
			return nil, nil, errors.Wrapf(err, "decoding")
		} else if typ == encoding.NotNull {
			// NOT NULL signals that a PartitionSpecialValCode follows
			var valCode uint64
			valueEncBuf, _, valCode, err = encoding.DecodeNonsortingUvarint(valueEncBuf[dataOffset:])
			if err != nil {
				return nil, nil, err
			}
			nextSpecial := PartitionSpecialValCode(valCode)
			if t.SpecialCount > 0 && t.Special != nextSpecial {
				return nil, nil, errors.Newf("non-%[1]s value (%[2]s) not allowed after %[1]s",
					t.Special, nextSpecial)
			}
			t.Special = nextSpecial
			t.SpecialCount++
		} else {
			var datum tree.Datum
			datum, valueEncBuf, err = DecodeTableValue(a, col.GetType(), valueEncBuf)
			if err != nil {
				return nil, nil, errors.Wrapf(err, "decoding")
			}
			if t.SpecialCount > 0 {
				return nil, nil, errors.Newf("non-%[1]s value (%[2]s) not allowed after %[1]s",
					t.Special, datum)
			}
			t.Datums = append(t.Datums, datum)
		}
	}
	if len(valueEncBuf) > 0 {
		return nil, nil, errors.New("superfluous data in encoded value")
	}

	allDatums := append(prefixDatums, t.Datums...)
	var colMap catalog.TableColMap
	for i := range allDatums {
		colMap.Set(index.GetKeyColumnID(i), i)
	}

	indexKeyPrefix := MakeIndexKeyPrefix(codec, tableDesc, index.GetID())
	key, _, err := EncodePartialIndexKey(
		tableDesc, index, len(allDatums), colMap, allDatums, indexKeyPrefix)
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
	if t.SpecialCount > 0 && t.Special == PartitionMaxVal {
		key = roachpb.Key(key).PrefixEnd()
	}

	return t, key, nil
}
