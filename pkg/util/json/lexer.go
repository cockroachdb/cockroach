// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package json

import (
	"bytes"
	"encoding/json"
	"io"
	"reflect"
	"strings"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/util/json/tokenizer"
	"github.com/cockroachdb/errors"
)

// parseUsingLexer parses string as JSON using JSON lexer.
func parseUsingLexer(s string, cfg parseConfig) (JSON, error) {
	input, err := unsafeGetBytes(s)
	if err != nil {
		return nil, err
	}

	l := jsonLexer{
		parseConfig: cfg,
		decoder:     tokenizer.MakeDecoder(input),
		state:       (*jsonLexer).lexTopValue,
	}

	j, err := l.lex()
	if err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) && l.decoder.More() {
			// JSON scanner returns nil token if it encounters an invalid input
			// character.  In such cases, decoder returns io.ErrUnexpectedEOF error.
			// However, we know it's not an EOF because decoder has more data.  So,
			// produce a bit nicer error message.
			return nil, jsonDecodeError(decodeErrorContext(errInvalidInputToken, s, l.decoder.Pos()))
		}
		return nil, jsonDecodeError(decodeErrorContext(err, s, l.decoder.Pos()))
	}

	if j == nil {
		return nil, errors.AssertionFailedf("expected parsed JSON value, got nil")
	}

	if l.decoder.More() {
		return nil, jsonDecodeError(decodeErrorContext(errTrailingCharacters, s, l.decoder.Pos()+1))
	}

	return j, nil
}

// jsonLexer builds JSON given input string. This implementation uses low level
// API provided by fork of github.com/pkg/json package to implement direct
// string to tree.JSON conversion, while trying to be as close to the
// encoder/json implementation as possible.
type jsonLexer struct {
	parseConfig
	decoder tokenizer.Decoder

	// state is the method expression for the next
	// state in the state machine.
	state func(*jsonLexer, []byte) (JSON, error)

	// State machine stack information.
	// kind is the types of objects stored in stack
	// len(kind) == len(arr) + len(obj)
	kind []kind
	arr  []ArrayBuilder  // array builder stack
	obj  []ObjectBuilder // object builder stack
}

// lex runs the lex loop -- reading next token from the
// stream, and decoding it based on the state machine.
func (l *jsonLexer) lex() (JSON, error) {
	for {
		tok, err := l.decoder.NextToken()
		if err != nil {
			return nil, err
		}

		if len(tok) < 1 {
			return nil, io.ErrUnexpectedEOF
		}

		j, err := l.state(l, tok)
		if err != nil {
			return nil, err
		}
		if j != nil && len(l.kind) == 0 {
			return j, nil
		}
	}
}

// lexTopValue processes top level JSON value.
func (l *jsonLexer) lexTopValue(tok []byte) (JSON, error) {
	switch tok[0] {
	case tokenizer.ArrayStart:
		l.pushArray()
		return nil, nil
	case tokenizer.ObjectStart:
		l.pushObject()
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

// lexArrayValue processes JSON value inside array.
func (l *jsonLexer) lexArrayValue(tok []byte) (JSON, error) {
	switch tok[0] {
	case tokenizer.ArrayEnd:
		return l.buildArray()
	case tokenizer.ArrayStart:
		l.pushArray()
	case tokenizer.ObjectStart:
		l.pushObject()
	case tokenizer.Null:
		l.addArrayValue(NullJSONValue)
	case tokenizer.String:
		l.addArrayValue(jsonString(tok[1 : len(tok)-1]))
	case tokenizer.True:
		l.addArrayValue(TrueJSONValue)
	case tokenizer.False:
		l.addArrayValue(FalseJSONValue)
	case '-', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
		n, err := FromNumber(json.Number(tok))
		if err != nil {
			return n, err
		}
		l.addArrayValue(n)
	default:
		return nil, errors.Newf("unexpected array token %q", tok)
	}
	return nil, nil
}

// lexObjectKey processes object key.
func (l *jsonLexer) lexObjectKey(tok []byte) (JSON, error) {
	switch tok[0] {
	case tokenizer.ObjectEnd:
		return l.buildObject()
	case tokenizer.String:
		l.addObjectKey(string(tok[1 : len(tok)-1]))
		return nil, nil
	default:
		return nil, errors.Newf("expected to read object key (string), found %q", tok)
	}
}

// lexObjectValue processes object value.
func (l *jsonLexer) lexObjectValue(tok []byte) (JSON, error) {
	switch tok[0] {
	case tokenizer.ArrayStart:
		l.pushArray()
	case tokenizer.ObjectStart:
		l.pushObject()
	case tokenizer.Null:
		l.setObjectValue(NullJSONValue)
	case tokenizer.String:
		l.setObjectValue(jsonString(tok[1 : len(tok)-1]))
	case tokenizer.True:
		l.setObjectValue(TrueJSONValue)
	case tokenizer.False:
		l.setObjectValue(FalseJSONValue)
	case '-', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
		n, err := FromNumber(json.Number(tok))
		if err != nil {
			return n, err
		}
		l.setObjectValue(n)
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

// pushArray adds array builder and transition state to
// read array values.
func (l *jsonLexer) pushArray() {
	l.arr = append(l.arr, ArrayBuilder{})
	l.kind = append(l.kind, kindArray)
	l.state = (*jsonLexer).lexArrayValue
}

// addArrayValue adds value to top array.
func (l *jsonLexer) addArrayValue(j JSON) {
	l.arr[len(l.arr)-1].Add(j)
}

// buildArray builds top array value, and adjust stack appropriately.
func (l *jsonLexer) buildArray() (JSON, error) {
	if len(l.kind) == 0 || l.kind[len(l.kind)-1] != kindArray {
		return nil, errUnexpectedState
	}
	j := l.arr[len(l.arr)-1].Build()
	l.pop()
	return l.stackReturn(j)
}

// pushObject adds object builder and transitions state to
// read object.
func (l *jsonLexer) pushObject() {
	l.obj = append(l.obj, ObjectBuilder{unordered: l.unordered})
	l.kind = append(l.kind, kindObject)
	l.state = (*jsonLexer).lexObjectKey
}

// addObjectKey adds key to object builder and transitions state
// to read object value.
func (l *jsonLexer) addObjectKey(k string) {
	l.obj[len(l.obj)-1].Add(k, nil)
	l.state = (*jsonLexer).lexObjectValue
}

// setObjectValue sets the value for the previously added object key,
// and transitions state to read the next object key.
func (l *jsonLexer) setObjectValue(v JSON) {
	p := l.obj[len(l.obj)-1].pairs
	p[len(p)-1].v = v
	l.state = (*jsonLexer).lexObjectKey
}

// buildObject builds top JSON object.
func (l *jsonLexer) buildObject() (JSON, error) {
	if len(l.kind) == 0 || l.kind[len(l.kind)-1] != kindObject {
		return nil, errUnexpectedState
	}
	j := l.obj[len(l.obj)-1].Build()
	l.pop()
	return l.stackReturn(j)
}

// pop stack.
func (l *jsonLexer) pop() {
	top := len(l.kind) - 1
	if l.kind[top] == kindArray {
		l.arr = l.arr[:len(l.arr)-1]
	} else {
		l.obj = l.obj[:len(l.obj)-1]
	}
	l.kind = l.kind[:top]
}

// stackReturn returns json object to the top of the stack
// and transition state machine to the next state.
func (l *jsonLexer) stackReturn(j JSON) (JSON, error) {
	// If stack is now empty, we're done -- return JSON.
	if len(l.kind) == 0 {
		return j, nil
	}

	// Add json to array or object; arrange for next state transition.
	if l.kind[len(l.kind)-1] == kindArray {
		l.addArrayValue(j)
		l.state = (*jsonLexer).lexArrayValue
	} else {
		l.setObjectValue(j)
		l.state = (*jsonLexer).lexObjectKey
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
// See https://groups.google.com/g/golang-nuts/c/Zsfk-VMd_fU/m/O1ru4fO-BgAJ
func unsafeGetBytes(s string) ([]byte, error) {
	const maxStrLen = 1 << 30 // Really, can't see us supporting input JSONs that big.
	if len(s) > maxStrLen {
		return nil, bytes.ErrTooLarge
	}
	if len(s) == 0 {
		return nil, nil
	}
	p := unsafe.Pointer((*reflect.StringHeader)(unsafe.Pointer(&s)).Data)
	return (*[maxStrLen]byte)(p)[:len(s):len(s)], nil
}
