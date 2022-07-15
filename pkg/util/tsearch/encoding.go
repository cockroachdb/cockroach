// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tsearch

import (
	"bytes"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/errors"
)

// EncodeTSVector encodes a tsvector into a serialized representation.
func EncodeTSVector(appendTo []byte, vector TSVector) []byte {
	appendTo = encoding.EncodeUint32Ascending(appendTo, uint32(len(vector)))
	for _, term := range vector {
		l := term.lexeme
		appendTo = encoding.EncodeStringAscending(appendTo, l)
		appendTo = encoding.EncodeUint32Ascending(appendTo, uint32(len(term.positions)))
		for _, pos := range term.positions {
			appendTo = encoding.EncodeUint32Ascending(appendTo, uint32(pos.position))
			appendTo = append(appendTo, byte(pos.weight))
		}
	}
	return appendTo
}

// DecodeTSVector decodes a tsvector from the input byte slice.
func DecodeTSVector(b []byte) (ret TSVector, err error) {
	var nTerms, nPositions, position uint32
	b, nTerms, err = encoding.DecodeUint32Ascending(b)
	if err != nil {
		return nil, err
	}
	ret = make([]tsTerm, nTerms)
	for i := uint32(0); i < nTerms; i++ {
		var lexeme string
		b, lexeme, err = encoding.DecodeUnsafeStringAscendingDeepCopy(b, nil)
		if err != nil {
			return nil, err
		}
		b, nPositions, err = encoding.DecodeUint32Ascending(b)
		if err != nil {
			return nil, err
		}
		term := &ret[i]
		term.lexeme = lexeme
		term.positions = make([]tsPosition, nPositions)
		for j := uint32(0); j < nPositions; j++ {
			b, position, err = encoding.DecodeUint32Ascending(b)
			if err != nil {
				return nil, err
			}
			// Decode our weight.
			if len(b) == 0 {
				return nil, errors.Errorf("did not find tsvector position weight in buffer")
			}
			weight := tsWeight(b[0])
			b = b[1:]
			if weight >= invalidWeight {
				return nil, errors.Errorf("invalid tsvector position weight in buffer: %d", weight)
			}
			term.positions[j] = tsPosition{position: int(position), weight: weight}
		}
	}
	return ret, nil
}

// EncodeTSVectorPGBinary encodes a tsvector into a serialized representation
// that's identical to Postgres's wire protocol representation.
//
// tsvector:
//
//	Used for text searching. Example of tsvector: 'a':1,6,10 'on':5 'and':8 'ate':9A 'cat':3 'fat':2,11 'mat':7 'rat':12 'sat':4
//	Max length for each lexeme string is 2046 bytes (excluding the trailing null-char)
//	The words are sorted when parsed, and only written once. Positions are also sorted and only written once.
//	For some reason, the unique check does not seem to be made for binary input, only text input...
//	text: As seen above. ' is escaped with '' and \ is escaped with \\.
//	binary:
//	    UInt32 number of lexemes
//	    for each lexeme:
//	        lexeme text in client encoding, null-terminated
//	        UInt16 number of positions
//	        for each position:
//	            UInt16 WordEntryPos, where the most significant 2 bits is weight, and the 14 least significant bits is pos (can't be 0). Weights 3,2,1,0 represent A,B,C,D
func EncodeTSVectorPGBinary(appendTo []byte, vector TSVector) ([]byte, error) {
	appendTo = encoding.EncodeUint32Ascending(appendTo, uint32(len(vector)))
	for _, term := range vector {
		l := term.lexeme
		appendTo = append(appendTo, []byte(l)...)
		appendTo = append(appendTo, byte(0))
		appendTo = encoding.EncodeUint16Ascending(appendTo, uint16(len(term.positions)))
		for _, pos := range term.positions {

			weight, err := pos.weight.TSVectorPGEncoding()
			if err != nil {
				return nil, err
			}
			// TODO(jordan): do we need to clear the top 2 bits first?
			out := uint16(pos.position) | (uint16(weight) << 14)
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
			// Clear the 2 most significant bits.
			position = position & (^(uint16(3) << 14))
			term.positions[j] = tsPosition{position: int(position), weight: weight}
		}
	}
	return ret, nil
}

// EncodeTSQueryPGBinary encodes a tsquery into a serialized representation.
// A tree with operands and operators (&, |, !). Operands are strings,
// with optional weight (bitmask of ABCD) and prefix search (yes/no, written with *).
// text: the tree written in infix notation. Example: ( 'abc':*B | 'def' ) & !'ghi'
// binary: the tree written in prefix notation:
// First the number of tokens (a token is an operand or an operator).
// For each token:
// UInt8 type (1 = val, 2 = oper) followed by
// For val: UInt8 weight + UInt8 prefix (1 = yes / 0 = no) + null-terminated string,
// For oper: UInt8 oper (1 = not, 2 = and, 3 = or, 4 = phrase).
// In case of phrase oper code, an additional UInt16 field is sent (distance value of operator). Default is 1 for <->, otherwise the n value in '<n>'.
func EncodeTSQueryPGBinary(appendTo []byte, query TSQuery) []byte {
	// First, append a uint32 of the number of nodes in the query. We'll come
	// back and fill this in later.
	lengthIdx := len(appendTo)
	appendTo = encoding.EncodeUint32Ascending(appendTo, 0)
	var encoder tsNodeCodec
	appendTo = encoder.encodeTSNode(query.root, appendTo)
	return encoding.PutUint32Ascending(appendTo, uint32(encoder.nTokens), lengthIdx)
}

type tsNodeCodec struct {
	nTokens int
}

const (
	tsNodeTypeVal  = 1
	tsNodeTypeOper = 2
)

func (c *tsNodeCodec) encodeTSNode(node *tsNode, appendTo []byte) []byte {
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
		appendTo = encoding.EncodeUint16Ascending(appendTo, uint16(node.followedN))
	}
	appendTo = c.encodeTSNode(node.l, appendTo)
	if node.r != nil {
		appendTo = c.encodeTSNode(node.r, appendTo)
	}
	return appendTo
}

// DecodeTSQueryPGBinary deserializes a serialized TSQuery in pgwire format.
func DecodeTSQueryPGBinary(b []byte) (ret TSQuery, err error) {
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
		ret.followedN = int(followedN)
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
