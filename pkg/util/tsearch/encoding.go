// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tsearch

import (
	"bytes"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/errors"
)

// EncodeTSVector encodes a tsvector into a serialized representation for
// on-disk storage.
func EncodeTSVector(appendTo []byte, vector TSVector) ([]byte, error) {
	appendTo = encoding.EncodeUint32Ascending(appendTo, uint32(len(vector)))
	for _, term := range vector {
		l := term.lexeme
		appendTo = encoding.EncodeUntaggedBytesValue(appendTo, encoding.UnsafeConvertStringToBytes(l))
		if len(term.positions) > maxTSVectorPositions {
			return nil, pgerror.Newf(pgcode.ProgramLimitExceeded,
				"tsvector position list of size %d too large (maximum is %d)", len(term.positions),
				maxTSVectorPositions)
		}
		if len(l) > maxTSVectorLexemeLen {
			return nil, pgerror.Newf(pgcode.ProgramLimitExceeded,
				"tsvector lexeme of size %d too large (maximum is %d)", len(l),
				maxTSVectorLexemeLen)
		}
		appendTo = encoding.EncodeUint16Ascending(appendTo, uint16(len(term.positions)))
		for _, pos := range term.positions {
			weight, err := pos.weight.TSVectorPGEncoding()
			if err != nil {
				return nil, err
			}
			// Clear the 2 most significant bits. These should never be set,
			// as we always make sure that positions are at most 1 << 14, but
			// better an extra check.
			position := pos.position & (^(uint16(3) << 14))
			out := position | (uint16(weight) << 14)
			appendTo = encoding.EncodeUint16Ascending(appendTo, out)
		}
	}
	return appendTo, nil
}

// DecodeTSVector decodes a tsvector in disk-storage representation from the
// input byte slice.
func DecodeTSVector(b []byte) (ret TSVector, err error) {
	var nTerms uint32
	var nPositions, position uint16
	b, nTerms, err = encoding.DecodeUint32Ascending(b)
	if err != nil {
		return nil, err
	}
	ret = make([]tsTerm, nTerms)
	for i := uint32(0); i < nTerms; i++ {
		var lexeme []byte
		b, lexeme, err = encoding.DecodeUntaggedBytesValue(b)
		if err != nil {
			return nil, err
		}
		b, nPositions, err = encoding.DecodeUint16Ascending(b)
		if err != nil {
			return nil, err
		}
		term := &ret[i]
		term.lexeme = string(lexeme)
		term.positions = make([]tsPosition, nPositions)
		for j := uint16(0); j < nPositions; j++ {
			b, position, err = encoding.DecodeUint16Ascending(b)
			if err != nil {
				return nil, err
			}
			encodedWeight := position >> 14
			weight, err := tsWeightFromVectorPGEncoding(byte(encodedWeight))
			if err != nil {
				return nil, err
			}
			// Clear the 2 most significant bits (they were used for the weight).
			position = position & (^(uint16(3) << 14))
			term.positions[j] = tsPosition{position: position, weight: weight}
		}
	}
	return ret, nil
}

// EncodeTSVectorPGBinary encodes a tsvector into a serialized representation
// that's identical to Postgres's wire protocol representation.
//
// The below comment explains the wire protocol representation. It is taken from
// this page: https://www.npgsql.org/dev/types.html
//
// tsvector:
//
//	UInt32 number of lexemes
//	for each lexeme:
//	    lexeme text in client encoding, null-terminated
//	    UInt16 number of positions
//	    for each position:
//	        UInt16 WordEntryPos, where the most significant 2 bits is weight, and the 14 least significant bits is pos (can't be 0). Weights 3,2,1,0 represent A,B,C,D
func EncodeTSVectorPGBinary(appendTo []byte, vector TSVector) ([]byte, error) {
	appendTo = encoding.EncodeUint32Ascending(appendTo, uint32(len(vector)))
	for _, term := range vector {
		l := term.lexeme
		appendTo = append(appendTo, []byte(l)...)
		appendTo = append(appendTo, byte(0))
		i := len(term.positions)
		appendTo = encoding.EncodeUint16Ascending(appendTo, uint16(i))
		for _, pos := range term.positions {
			weight, err := pos.weight.TSVectorPGEncoding()
			if err != nil {
				return nil, err
			}
			out := pos.position | (uint16(weight) << 14)
			appendTo = encoding.EncodeUint16Ascending(appendTo, out)
		}
	}
	return appendTo, nil
}

// DecodeTSVectorPGBinary decodes a tsvector from the input byte slice which is
// formatted in Postgres binary protocol.
func DecodeTSVectorPGBinary(b []byte) (ret TSVector, err error) {
	var nTerms uint32
	var nPositions, position uint16
	b, nTerms, err = encoding.DecodeUint32Ascending(b)
	if err != nil {
		return nil, err
	}
	ret = make([]tsTerm, nTerms)
	for i := uint32(0); i < nTerms; i++ {
		termIndex := bytes.IndexByte(b, byte(0))
		if termIndex == -1 {
			return nil, pgerror.Newf(pgcode.Syntax, "unterminated string while parsing tsvector: %s", b)
		}
		term := &ret[i]
		term.lexeme = string(b[:termIndex])
		b = b[termIndex+1:]
		b, nPositions, err = encoding.DecodeUint16Ascending(b)
		if err != nil {
			return nil, err
		}
		term.positions = make([]tsPosition, nPositions)
		for j := uint16(0); j < nPositions; j++ {
			b, position, err = encoding.DecodeUint16Ascending(b)
			if err != nil {
				return nil, err
			}
			encodedWeight := position >> 14
			weight, err := tsWeightFromVectorPGEncoding(byte(encodedWeight))
			if err != nil {
				return nil, err
			}
			// Clear the 2 most significant bits (they were used for the weight).
			position = position & (^(uint16(3) << 14))
			term.positions[j] = tsPosition{position: position, weight: weight}
		}
	}
	return ret, nil
}

// EncodeTSQuery encodes a tsquery into a serialized representation for on-disk
// storage.
func EncodeTSQuery(appendTo []byte, query TSQuery) ([]byte, error) {
	// First, append a uint32 of the number of nodes in the query. We'll come
	// back and fill this in later.
	lengthIdx := len(appendTo)
	appendTo = encoding.EncodeUint32Ascending(appendTo, 0)
	var encoder tsNodeCodec
	var err error
	appendTo, err = encoder.encodeTSNode(query.root, appendTo)
	if err != nil {
		return nil, err
	}
	return encoding.PutUint32Ascending(appendTo, uint32(encoder.nTokens), lengthIdx), nil
}

// DecodeTSQuery deserializes a serialized TSQuery in on-disk format.
func DecodeTSQuery(b []byte) (ret TSQuery, err error) {
	var nTokens uint32
	b, nTokens, err = encoding.DecodeUint32Ascending(b)
	if err != nil {
		return ret, err
	}
	decoder := tsNodeCodec{nTokens: int(nTokens)}
	_, ret.root, err = decoder.decodeTSNode(b)
	if err != nil {
		return ret, err
	}
	return ret, nil
}

// EncodeTSQueryPGBinary encodes a tsquery into a serialized representation.
//
// The below comment explains the wire protocol representation. It is taken from
// this page: https://www.npgsql.org/dev/types.html
//
//	the tree written in prefix notation:
//	First the number of tokens (a token is an operand or an operator).
//	For each token:
//	  UInt8 type (1 = val, 2 = oper) followed by
//	  For val: UInt8 weight + UInt8 prefix (1 = yes / 0 = no) + null-terminated string,
//	  For oper: UInt8 oper (1 = not, 2 = and, 3 = or, 4 = phrase).
//	  In case of phrase oper code, an additional UInt16 field is sent (distance value of operator). Default is 1 for <->, otherwise the n value in '<n>'.
func EncodeTSQueryPGBinary(appendTo []byte, query TSQuery) []byte {
	// First, append a uint32 of the number of nodes in the query. We'll come
	// back and fill this in later.
	lengthIdx := len(appendTo)
	appendTo = encoding.EncodeUint32Ascending(appendTo, 0)
	var encoder tsNodeCodec
	appendTo = encoder.encodeTSNodePGBinary(query.root, appendTo)
	return encoding.PutUint32Ascending(appendTo, uint32(encoder.nTokens), lengthIdx)
}

// DecodeTSQueryPGBinary deserializes a serialized TSQuery in pgwire format.
func DecodeTSQueryPGBinary(b []byte) (ret TSQuery, err error) {
	var nTokens uint32
	b, nTokens, err = encoding.DecodeUint32Ascending(b)
	if err != nil {
		return ret, err
	}
	decoder := tsNodeCodec{nTokens: int(nTokens)}
	_, ret.root, err = decoder.decodeTSNodePGBinary(b)
	if err != nil {
		return ret, err
	}
	return ret, nil
}

type tsNodeCodec struct {
	nTokens int
}

const (
	tsNodeTypeVal  = 1
	tsNodeTypeOper = 2
)

func (c *tsNodeCodec) encodeTSNode(node *tsNode, appendTo []byte) ([]byte, error) {
	c.nTokens++
	if node.op == invalid {
		appendTo = append(appendTo, byte(tsNodeTypeVal))
		if len(node.term.positions) > 0 {
			weight := byte(node.term.positions[0].weight & (^weightStar))
			appendTo = append(appendTo, weight)
			prefix := byte(node.term.positions[0].weight >> 4)
			appendTo = append(appendTo, prefix)
		} else {
			appendTo = append(appendTo, byte(0), byte(0))
		}
		if len(node.term.lexeme) > maxTSVectorLexemeLen {
			return nil, pgerror.Newf(pgcode.ProgramLimitExceeded,
				"tsvector lexeme of size %d too large (maximum is %d)", len(node.term.lexeme),
				maxTSVectorLexemeLen)
		}
		appendTo = encoding.EncodeUntaggedBytesValue(appendTo, encoding.UnsafeConvertStringToBytes(node.term.lexeme))
		return appendTo, nil
	}
	appendTo = append(appendTo, byte(tsNodeTypeOper))
	appendTo = append(appendTo, node.op.pgwireEncoding())
	if node.op == followedby {
		if node.followedN > maxTSVectorFollowedBy {
			return nil, pgerror.Newf(pgcode.ProgramLimitExceeded,
				"tsvector followed by argument %d too large (maximum is %d)", node.followedN,
				maxTSVectorLexemeLen)
		}
		appendTo = encoding.EncodeUint16Ascending(appendTo, node.followedN)
	}
	var err error
	appendTo, err = c.encodeTSNode(node.l, appendTo)
	if err != nil {
		return nil, err
	}
	if node.r != nil {
		appendTo, err = c.encodeTSNode(node.r, appendTo)
		if err != nil {
			return nil, err
		}
	}
	return appendTo, nil
}

func (c *tsNodeCodec) encodeTSNodePGBinary(node *tsNode, appendTo []byte) []byte {
	c.nTokens++
	if node.op == invalid {
		appendTo = append(appendTo, byte(tsNodeTypeVal))
		if len(node.term.positions) > 0 {
			weight := byte(node.term.positions[0].weight & (^weightStar))
			appendTo = append(appendTo, weight)
			prefix := byte(node.term.positions[0].weight >> 4)
			appendTo = append(appendTo, prefix)
		} else {
			appendTo = append(appendTo, byte(0), byte(0))
		}
		appendTo = append(appendTo, []byte(node.term.lexeme)...)
		appendTo = append(appendTo, byte(0))
		return appendTo
	}
	appendTo = append(appendTo, byte(tsNodeTypeOper))
	appendTo = append(appendTo, node.op.pgwireEncoding())
	if node.op == followedby {
		appendTo = encoding.EncodeUint16Ascending(appendTo, node.followedN)
	}
	if node.r != nil {
		appendTo = c.encodeTSNodePGBinary(node.r, appendTo)
	}
	appendTo = c.encodeTSNodePGBinary(node.l, appendTo)
	return appendTo
}

func getOneByte(b []byte) ([]byte, byte, error) {
	if len(b) == 0 {
		return nil, 0, errors.Errorf("insufficient bytes to decode byte")
	}
	return b[1:], b[0], nil
}

func (c *tsNodeCodec) decodeTSNode(b []byte) ([]byte, *tsNode, error) {
	if c.nTokens == 0 {
		return nil, nil, errors.Errorf("malformed tsquery: too many nodes")
	}
	c.nTokens--
	var err error
	var nodeType byte
	b, nodeType, err = getOneByte(b)
	if err != nil {
		return nil, nil, err
	}
	ret := &tsNode{}
	if nodeType == tsNodeTypeVal {
		// We're at a leaf. Decode and return.
		if len(b) < 2 {
			return nil, nil, errors.Errorf("insufficient bytes to decode value weight")
		}
		weight, prefix := b[0], b[1]
		b = b[2:]
		if weight != 0 || prefix != 0 {
			ret.term.positions = []tsPosition{{weight: tsWeight(weight | (prefix << 4))}}
		}
		// Decode the lexeme.
		var lexeme []byte
		b, lexeme, err = encoding.DecodeUntaggedBytesValue(b)
		if err != nil {
			return nil, nil, err
		}
		ret.term.lexeme = string(lexeme)
		return b, ret, nil
	}

	// We're at an operator.
	var operType byte
	b, operType, err = getOneByte(b)
	if err != nil {
		return nil, nil, err
	}
	oper, err := tsOperatorFromPgwireEncoding(operType)
	if err != nil {
		return nil, nil, err
	}
	ret.op = oper
	if oper == followedby {
		var followedN uint16
		b, followedN, err = encoding.DecodeUint16Ascending(b)
		if err != nil {
			return nil, nil, err
		}
		ret.followedN = followedN
	}
	b, ret.l, err = c.decodeTSNode(b)
	if err != nil {
		return nil, nil, err
	}
	switch oper {
	// Not doesn't have a right argument.
	case and, or, followedby:
		b, ret.r, err = c.decodeTSNode(b)
		if err != nil {
			return nil, nil, err
		}
	}
	return b, ret, nil
}

func (c *tsNodeCodec) decodeTSNodePGBinary(b []byte) ([]byte, *tsNode, error) {
	if c.nTokens == 0 {
		return nil, nil, errors.Errorf("malformed tsquery: too many nodes")
	}
	c.nTokens--
	var err error
	var nodeType byte
	b, nodeType, err = getOneByte(b)
	if err != nil {
		return nil, nil, err
	}
	ret := &tsNode{}
	if nodeType == tsNodeTypeVal {
		// We're at a leaf. Decode and return.
		if len(b) < 2 {
			return nil, nil, errors.Errorf("insufficient bytes to decode value weight")
		}
		weight, prefix := b[0], b[1]
		b = b[2:]
		if weight != 0 || prefix != 0 {
			ret.term.positions = []tsPosition{{weight: tsWeight(weight | (prefix << 4))}}
		}
		// Decode the null-terminated lexeme.
		idx := bytes.IndexByte(b, 0)
		if idx == -1 {
			return nil, nil, errors.Errorf("no null-terminated string in tsnode")
		}
		ret.term.lexeme = string(b[:idx])
		return b[idx+1:], ret, nil
	}

	// We're at an operator.
	var operType byte
	b, operType, err = getOneByte(b)
	if err != nil {
		return nil, nil, err
	}
	oper, err := tsOperatorFromPgwireEncoding(operType)
	if err != nil {
		return nil, nil, err
	}
	ret.op = oper
	if oper == followedby {
		var followedN uint16
		b, followedN, err = encoding.DecodeUint16Ascending(b)
		if err != nil {
			return nil, nil, err
		}
		ret.followedN = followedN
	}
	switch oper {
	// Not doesn't have a right argument.
	case and, or, followedby:
		b, ret.r, err = c.decodeTSNodePGBinary(b)
		if err != nil {
			return nil, nil, err
		}
	}
	b, ret.l, err = c.decodeTSNodePGBinary(b)
	if err != nil {
		return nil, nil, err
	}
	return b, ret, nil
}

// EncodeInvertedIndexKeys returns a slice of byte slices, one per inverted
// index key for the terms in this tsvector.
func EncodeInvertedIndexKeys(inKey []byte, vector TSVector) ([][]byte, error) {
	outKeys := make([][]byte, 0, len(vector))
	// Note that by construction, TSVector contains only unique terms, so we don't
	// need to de-duplicate terms when constructing the inverted index keys.
	for i := range vector {
		newKey := EncodeInvertedIndexKey(inKey, vector[i].lexeme)
		outKeys = append(outKeys, newKey)
	}
	return outKeys, nil
}

// EncodeInvertedIndexKey returns the inverted index key for the input lexeme.
func EncodeInvertedIndexKey(inKey []byte, lexeme string) []byte {
	outKey := make([]byte, len(inKey), len(inKey)+len(lexeme))
	copy(outKey, inKey)
	return encoding.EncodeStringAscending(outKey, lexeme)
}
