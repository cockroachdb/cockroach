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
func encodeTuple(t *tree.DTuple, appendTo []byte, colID uint32, scratch []byte) ([]byte, error) {
	appendTo = encoding.EncodeValueTag(appendTo, colID, encoding.Tuple)
	return encodeUntaggedTuple(t, appendTo, colID, scratch)
}

// encodeUntaggedTuple produces the value encoding for a tuple without a value tag.
func encodeUntaggedTuple(
	t *tree.DTuple, appendTo []byte, colID uint32, scratch []byte,
) ([]byte, error) {
	appendTo = encoding.EncodeNonsortingUvarint(appendTo, uint64(len(t.D)))

	var err error
	for _, dd := range t.D {
		appendTo, err = Encode(appendTo, NoColumnID, dd, scratch)
		if err != nil {
			return nil, err
		}
	}
	return appendTo, nil
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
