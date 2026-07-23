// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package json

import (
	"bytes"
	"encoding/json"
	"io"
	"strings"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/util/json/tokenizer"
	"github.com/cockroachdb/errors"
)

// parseUsingFastParser parses string as JSON using fast json parser.
func parseUsingFastParser(s string, cfg parseConfig) (JSON, error) {
	input, err := unsafeGetBytes(s)
	if err != nil {
		return nil, err
	}

	p := fastJSONParser{
		parseConfig: cfg,
		decoder:     tokenizer.MakeDecoder(input),
		state:       (*fastJSONParser).parseTopValue,
	}
	defer p.decoder.Release()

	j, err := p.parse()
	if err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) && p.decoder.More() {
			// JSON scanner returns nil token if it encounters an invalid input
			// character.  In such cases, decoder returns io.ErrUnexpectedEOF error.
			// However, we know it's not an EOF because decoder has more data.  So,
			// produce a bit nicer error message.
			return nil, jsonDecodeError(decodeErrorContext(errInvalidInputToken, s, p.decoder.Pos()))
		}
		return nil, jsonDecodeError(decodeErrorContext(err, s, p.decoder.Pos()))
	}

	if j == nil {
		return nil, errors.AssertionFailedf("expected parsed JSON value, got nil")
	}

	if p.decoder.More() {
		return nil, jsonDecodeError(decodeErrorContext(errTrailingCharacters, s, p.decoder.Pos()+1))
	}

	return j, nil
}

// fastJSONParser builds JSON given input string. This implementation uses low level
// API provided by fork of github.com/pkg/json package to implement direct
// string to tree.JSON conversion, while trying to be as close to the
// encoder/json implementation as possible.
type fastJSONParser struct {
	parseConfig
	decoder tokenizer.Decoder

	// state is the method expression for the next
	// state in the state machine.
	state func(*fastJSONParser, []byte) (JSON, error)

	// State machine stack information.
	// kind is the types of objects stored in stack
	// len(kind) == len(arr) + len(obj)
	kind []kind
	arr  []ArrayBuilder  // array builder stack
	obj  []ObjectBuilder // object builder stack
}

// parse runs the parse loop -- reading next token from the
// stream, and decoding it based on the state machine.
func (p *fastJSONParser) parse() (JSON, error) {
	for {
		tok, err := p.decoder.NextToken()
		if err != nil {
			return nil, err
		}

		if len(tok) < 1 {
			return nil, io.ErrUnexpectedEOF
		}

		j, err := p.state(p, tok)
		if err != nil {
			return nil, err
		}
		if j != nil && len(p.kind) == 0 {
			return j, nil
		}
	}
}

// parseTopValue processes top level JSON value.
func (p *fastJSONParser) parseTopValue(tok []byte) (JSON, error) {
	switch tok[0] {
	case tokenizer.ArrayStart:
		p.pushArray()
		return nil, nil
	case tokenizer.ObjectStart:
		p.pushObject()
		return nil, nil
	case tokenizer.Null:
		return NullJSONValue, nil
	case tokenizer.String:
		return jsonString(tok[1 : len(tok)-1]), nil
	case tokenizer.True:
		return TrueJSONValue, nil
	case tokenizer.False:
		return FalseJSONValue, nil
	case '-', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
		return FromNumber(json.Number(tok))
	default:
		return nil, errors.Newf("unexpected token %q", tok)
	}
}

// parseArrayValue processes JSON value inside array.
func (p *fastJSONParser) parseArrayValue(tok []byte) (JSON, error) {
	switch tok[0] {
	case tokenizer.ArrayEnd:
		return p.buildArray()
	case tokenizer.ArrayStart:
		p.pushArray()
	case tokenizer.ObjectStart:
		p.pushObject()
	case tokenizer.Null:
		p.addArrayValue(NullJSONValue)
	case tokenizer.String:
		p.addArrayValue(jsonString(tok[1 : len(tok)-1]))
	case tokenizer.True:
		p.addArrayValue(TrueJSONValue)
	case tokenizer.False:
		p.addArrayValue(FalseJSONValue)
	case '-', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
		n, err := FromNumber(json.Number(tok))
		if err != nil {
			return n, err
		}
		p.addArrayValue(n)
	default:
		return nil, errors.Newf("unexpected array token %q", tok)
	}
	return nil, nil
}

// parseObjectKey processes object key.
func (p *fastJSONParser) parseObjectKey(tok []byte) (JSON, error) {
	switch tok[0] {
	case tokenizer.ObjectEnd:
		return p.buildObject()
	case tokenizer.String:
		p.addObjectKey(string(tok[1 : len(tok)-1]))
		return nil, nil
	default:
		return nil, errors.Newf("expected to read object key (string), found %q", tok)
	}
}

// parseObjectValue processes object value.
func (p *fastJSONParser) parseObjectValue(tok []byte) (JSON, error) {
	switch tok[0] {
	case tokenizer.ArrayStart:
		p.pushArray()
	case tokenizer.ObjectStart:
		p.pushObject()
	case tokenizer.Null:
		p.setObjectValue(NullJSONValue)
	case tokenizer.String:
		p.setObjectValue(jsonString(tok[1 : len(tok)-1]))
	case tokenizer.True:
		p.setObjectValue(TrueJSONValue)
	case tokenizer.False:
		p.setObjectValue(FalseJSONValue)
	case '-', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
		n, err := FromNumber(json.Number(tok))
		if err != nil {
			return n, err
		}
		p.setObjectValue(n)
	default:
		return nil, errors.Newf("unexpected object token %q", tok)
	}
	return nil, nil
}

type kind bool

const (
	kindArray  kind = false
	kindObject kind = true
)

var errUnexpectedState = errors.New("unexpected state machine state")

// pushArray adds array builder and transitions state to read array values.
func (p *fastJSONParser) pushArray() {
	p.arr = append(p.arr, ArrayBuilder{})
	p.kind = append(p.kind, kindArray)
	p.state = (*fastJSONParser).parseArrayValue
}

// addArrayValue adds value to top array.
func (p *fastJSONParser) addArrayValue(j JSON) {
	p.arr[len(p.arr)-1].Add(j)
}

// buildArray builds top array value, and adjusts stack appropriately.
func (p *fastJSONParser) buildArray() (JSON, error) {
	if len(p.kind) == 0 || p.kind[len(p.kind)-1] != kindArray {
		return nil, errUnexpectedState
	}
	j := p.arr[len(p.arr)-1].Build()
	p.pop()
	return p.stackReturn(j)
}

// pushObject adds object builder and transitions state to read object.
func (p *fastJSONParser) pushObject() {
	p.obj = append(p.obj, ObjectBuilder{unordered: p.unordered})
	p.kind = append(p.kind, kindObject)
	p.state = (*fastJSONParser).parseObjectKey
}

// addObjectKey adds key to object builder and transitions state to read object
// value.
func (p *fastJSONParser) addObjectKey(k string) {
	p.obj[len(p.obj)-1].Add(k, nil)
	p.state = (*fastJSONParser).parseObjectValue
}

// setObjectValue sets the value for the previously added object key,
// and transitions state to read the next object key.
func (p *fastJSONParser) setObjectValue(v JSON) {
	pairs := p.obj[len(p.obj)-1].pairs
	pairs[len(pairs)-1].v = v
	p.state = (*fastJSONParser).parseObjectKey
}

// buildObject builds top JSON object.
func (p *fastJSONParser) buildObject() (JSON, error) {
	if len(p.kind) == 0 || p.kind[len(p.kind)-1] != kindObject {
		return nil, errUnexpectedState
	}
	j := p.obj[len(p.obj)-1].Build()
	p.pop()
	return p.stackReturn(j)
}

// pop stack.
func (p *fastJSONParser) pop() {
	top := len(p.kind) - 1
	if p.kind[top] == kindArray {
		p.arr = p.arr[:len(p.arr)-1]
	} else {
		p.obj = p.obj[:len(p.obj)-1]
	}
	p.kind = p.kind[:top]
}

// stackReturn returns json object to the top of the stack
// and transitions state machine to the next state.
func (p *fastJSONParser) stackReturn(j JSON) (JSON, error) {
	// If stack is now empty, we're done -- return JSON.
	if len(p.kind) == 0 {
		return j, nil
	}

	// Add json to array or object; arrange for next state transition.
	if p.kind[len(p.kind)-1] == kindArray {
		p.addArrayValue(j)
		p.state = (*fastJSONParser).parseArrayValue
	} else {
		p.setObjectValue(j)
		p.state = (*fastJSONParser).parseObjectKey
	}
	return nil, nil
}

var errInvalidInputToken = errors.New("invalid JSON token")

// decodeErrorContext returns input context for an error encountered during decoding.
// There is quite a bit of code here, but debugging faulty JSON is hard, so
// take extra care to produce nice error message, with good context information.
func decodeErrorContext(err error, s string, pos int) error {
	if len(s) == 0 {
		return errors.Wrap(err, "while decoding empty string")
	}

	const contextSize = 16
	ctxStart := pos - contextSize
	if ctxStart < 0 {
		ctxStart = 0
	}
	ctxEnd := pos + contextSize
	if ctxEnd > len(s) {
		ctxEnd = len(s)
	}

	var leftPad, rightPad string
	if pos > ctxStart {
		leftPad = strings.Repeat(".", pos-ctxStart)
	}
	if ctxEnd > pos {
		rightPad = strings.Repeat(".", ctxEnd-pos-1)
	}

	return errors.Wrapf(err,
		"while decoding %d bytes at offset %d:\n"+
			"...|%s|...\n"+
			"...|%s^%s|...",
		len(s), pos,
		s[ctxStart:ctxEnd],
		leftPad, rightPad,
	)
}

// unsafeGetBytes returns []byte in the underlying string,
// without incurring copy.
// This unsafe mechanism is safe to use here because, ultimately, every
// JSON object produced from those bytes will copy those bytes anyway
// (i.e. jsonString([]byte)).
func unsafeGetBytes(s string) ([]byte, error) {
	const maxStrLen = 1 << 30 // Really, can't see us supporting input JSONs that big.
	if len(s) > maxStrLen {
		return nil, bytes.ErrTooLarge
	}
	if len(s) == 0 {
		return nil, nil
	}

	return unsafe.Slice(unsafe.StringData(s), len(s)), nil
}
