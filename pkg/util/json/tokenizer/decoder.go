// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// This is a fork of pkg/json package.

// Copyright (c) 2020, Dave Cheney <dave@cheney.net>
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//
//   - Redistributions of source code must retain the above copyright notice, this
//     list of conditions and the following disclaimer.
//
//   - Redistributions in binary form must reproduce the above copyright notice,
//     this list of conditions and the following disclaimer in the documentation
//     and/or other materials provided with the distribution.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
// AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
// FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
// DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
// SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
// CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
// OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

package tokenizer

import (
	"fmt"
	"io"
)

// A Decoder decodes JSON values from an input stream.
type Decoder struct {
	scanner Scanner
	state   func(*Decoder) ([]byte, error)

	// mustHaveValue is set when decoder processes
	// array or object -- as indicated by stack state.
	// In those cases, when we see a comma, there *must*
	// be either an array value or a string object key following
	// it; and if array/object terminates without seeing
	// this value, return an error.
	mustHaveValue bool
	stack
}

// MakeDecoder returns decoder for the input data
func MakeDecoder(data []byte) Decoder {
	return Decoder{
		scanner: Scanner{data: data},
		state:   (*Decoder).stateValue,
	}
}

// Pos returns current input position.
func (d *Decoder) Pos() int {
	return d.scanner.offset
}

// More returns true if there is more non-whitespace tokens available.
func (d *Decoder) More() bool {
	return d.scanner.More()
}

// Release releases acquired resources.
func (d *Decoder) Release() {
	d.scanner.Release()
}

type stack []bool

func (s *stack) push(v bool) {
	*s = append(*s, v)
}

func (s *stack) pop() bool {
	*s = (*s)[:len(*s)-1]
	if len(*s) == 0 {
		return false
	}
	return (*s)[len(*s)-1]
}

func (s *stack) len() int { return len(*s) }

// NextToken returns a []byte referencing the next logical token in the stream.
// The []byte is valid until Token is called again.
// At the end of the input stream, Token returns nil, io.EOF.
//
// Token guarantees that the delimiters [ ] { } it returns are properly nested
// and matched: if Token encounters an unexpected delimiter in the input, it
// will return an error.
//
// A valid token begins with one of the following:
//
//	{ Object start
//	[ Array start
//	} Object end
//	] Array End
//	t JSON true
//	f JSON false
//	n JSON null
//	" A string, possibly containing backslash escaped entites.
//	-, 0-9 A number
//
// Commas and colons are elided.
func (d *Decoder) NextToken() ([]byte, error) {
	return d.state(d)
}

func (d *Decoder) stateObjectString() ([]byte, error) {
	tok := d.scanner.Next()
	if len(tok) < 1 {
		return nil, io.ErrUnexpectedEOF
	}
	switch tok[0] {
	case '}':
		if d.mustHaveValue {
			d.scanner.offset -= len(tok) + 1 // Rewind to point to comma.
			return nil, fmt.Errorf("stateObjectString: missing string key")
		}

		inObj := d.pop()
		switch {
		case d.len() == 0:
			d.state = (*Decoder).stateEnd
		case inObj:
			d.state = (*Decoder).stateObjectComma
		case !inObj:
			d.state = (*Decoder).stateArrayComma
		}
		return tok, nil
	case '"':
		d.state = (*Decoder).stateObjectColon
		return tok, nil
	default:
		return nil, fmt.Errorf("stateObjectString: missing string key")
	}
}

func (d *Decoder) stateObjectColon() ([]byte, error) {
	tok := d.scanner.Next()
	if len(tok) < 1 {
		return nil, io.ErrUnexpectedEOF
	}
	switch tok[0] {
	case Colon:
		d.state = (*Decoder).stateObjectValue
		return d.NextToken()
	default:
		return tok, fmt.Errorf("stateObjectColon: expecting colon")
	}
}

func (d *Decoder) stateObjectValue() ([]byte, error) {
	tok := d.scanner.Next()
	if len(tok) < 1 {
		return nil, io.ErrUnexpectedEOF
	}
	switch tok[0] {
	case '{':
		d.state = (*Decoder).stateObjectString
		d.push(true)
		return tok, nil
	case '[':
		d.state = (*Decoder).stateArrayValue
		d.push(false)
		return tok, nil
	default:
		d.state = (*Decoder).stateObjectComma
		return tok, nil
	}
}

func (d *Decoder) stateObjectComma() (_ []byte, err error) {
	tok := d.scanner.Next()
	if len(tok) < 1 {
		return nil, io.ErrUnexpectedEOF
	}
	switch tok[0] {
	case '}':
		inObj := d.pop()
		switch {
		case d.len() == 0:
			d.state = (*Decoder).stateEnd
		case inObj:
			d.state = (*Decoder).stateObjectComma
		case !inObj:
			d.state = (*Decoder).stateArrayComma
		}
		return tok, nil
	case Comma:
		d.mustHaveValue = true
		tok, err = d.stateObjectString()
		d.mustHaveValue = false
		return tok, err
	default:
		return tok, fmt.Errorf("stateObjectComma: expecting comma")
	}
}

func (d *Decoder) stateArrayValue() ([]byte, error) {
	tok := d.scanner.Next()
	if len(tok) < 1 {
		return nil, io.ErrUnexpectedEOF
	}
	switch tok[0] {
	case '{':
		d.state = (*Decoder).stateObjectString
		d.push(true)
		return tok, nil
	case '[':
		d.state = (*Decoder).stateArrayValue
		d.push(false)
		return tok, nil
	case ']':
		if d.mustHaveValue {
			d.scanner.offset -= len(tok) + 1 // Rewind to point to comma.
			return nil, fmt.Errorf("stateArrayValue: unexpected comma")
		}
		inObj := d.pop()
		switch {
		case d.len() == 0:
			d.state = (*Decoder).stateEnd
		case inObj:
			d.state = (*Decoder).stateObjectComma
		case !inObj:
			d.state = (*Decoder).stateArrayComma
		}
		return tok, nil
	case Comma:
		return nil, fmt.Errorf("stateArrayValue: unexpected comma")
	default:
		d.state = (*Decoder).stateArrayComma
		return tok, nil
	}
}

func (d *Decoder) stateArrayComma() (_ []byte, err error) {
	tok := d.scanner.Next()
	if len(tok) < 1 {
		return nil, io.ErrUnexpectedEOF
	}
	switch tok[0] {
	case ']':
		inObj := d.pop()
		switch {
		case d.len() == 0:
			d.state = (*Decoder).stateEnd
		case inObj:
			d.state = (*Decoder).stateObjectComma
		case !inObj:
			d.state = (*Decoder).stateArrayComma
		}
		return tok, nil
	case Comma:
		d.mustHaveValue = true
		tok, err = d.stateArrayValue()
		d.mustHaveValue = false
		return tok, err
	default:
		return nil, fmt.Errorf("stateArrayComma: expected comma, %v", d.stack)
	}
}

func (d *Decoder) stateValue() ([]byte, error) {
	tok := d.scanner.Next()
	if len(tok) < 1 {
		return nil, io.ErrUnexpectedEOF
	}
	switch tok[0] {
	case '{':
		d.state = (*Decoder).stateObjectString
		d.push(true)
		return tok, nil
	case '[':
		d.state = (*Decoder).stateArrayValue
		d.push(false)
		return tok, nil
	case ',':
		return nil, fmt.Errorf("stateValue: unexpected comma")
	default:
		d.state = (*Decoder).stateEnd
		return tok, nil
	}
}

func (d *Decoder) stateEnd() ([]byte, error) { return nil, io.EOF }
