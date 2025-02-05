// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package valueside

import (
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
)

// encodeTuple produces the value encoding for a tuple.
func encodeTuple(
	t *tree.DTuple, appendTo []byte, colID uint32, scratch []byte,
) (_, newScratch []byte, err error) {
	appendTo = encoding.EncodeValueTag(appendTo, colID, encoding.Tuple)
	return encodeUntaggedTuple(t, appendTo, scratch)
}

// encodeUntaggedTuple produces the value encoding for a tuple without a value tag.
func encodeUntaggedTuple(
	t *tree.DTuple, appendTo []byte, scratch []byte,
) (_, newScratch []byte, err error) {
	appendTo = encoding.EncodeNonsortingUvarint(appendTo, uint64(len(t.D)))
	for _, dd := range t.D {
		appendTo, scratch, err = EncodeWithScratch(appendTo, NoColumnID, dd, scratch[:0])
		if err != nil {
			return nil, nil, err
		}
	}
	return appendTo, scratch, nil
}

// decodeTuple decodes a tuple from its value encoding. It is the
// counterpart of encodeTuple().
func decodeTuple(a *tree.DatumAlloc, tupTyp *types.T, b []byte) (tree.Datum, []byte, error) {
	b, _, _, err := encoding.DecodeNonsortingUvarint(b)
	if err != nil {
		return nil, nil, err
	}

	result := *(tree.NewDTuple(tupTyp))
	result.D = a.NewDatums(len(tupTyp.TupleContents()))
	var datum tree.Datum
	for i := range tupTyp.TupleContents() {
		datum, b, err = Decode(a, tupTyp.TupleContents()[i], b)
		if err != nil {
			return nil, b, err
		}
		result.D[i] = datum
	}
	return a.NewDTuple(result), b, nil
}

func init() {
	encoding.PrettyPrintTupleValueEncoded = func(b []byte) ([]byte, string, error) {
		b, _, l, err := encoding.DecodeNonsortingUvarint(b)
		if err != nil {
			return b, "", err
		}
		result := &tree.DTuple{D: make([]tree.Datum, l)}
		for i := range result.D {
			_, _, _, encType, err := encoding.DecodeValueTag(b)
			if err != nil {
				return b, "", err
			}
			t, err := encodingTypeToDatumType(encType)
			if err != nil {
				return b, "", err
			}
			result.D[i], b, err = Decode(nil /* a */, t, b)
			if err != nil {
				return b, "", err
			}
		}
		return b, result.String(), nil
	}
}
