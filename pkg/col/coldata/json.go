// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package coldata

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/util/json"
)

// JSONs is a representation of columnar JSON data. It's simply a wrapper around
// the flat Bytes structure. To pull a JSON out of the structure, we construct
// a new "encodedJSON" object from scratch on demand.
type JSONs struct {
	Bytes
}

// NewJSONs returns a new JSONs presized to n elements.
func NewJSONs(n int) *JSONs {
	return &JSONs{
		Bytes: *NewBytes(n),
	}
}

// Get returns the ith JSON in JSONs. Note that the returned JSON is
// unsafe for reuse if any write operation happens.
// NOTE: if ith element was never set in any way, the behavior of Get is
// undefined.
func (js *JSONs) Get(i int) json.JSON {
	bytes := js.Bytes.Get(i)
	if len(bytes) == 0 {
		return json.NullJSONValue
	}
	ret, err := json.FromEncoding(bytes)
	if err != nil {
		colexecerror.ExpectedError(err)
	}
	return ret
}

// Set sets the ith JSON in JSONs. Overwriting a value that is not at the end
// of the JSONs is not allowed since it complicates memory movement to make/take
// away necessary space in the flat buffer.
func (js *JSONs) Set(i int, j json.JSON) {
	b := &js.Bytes
	appendTo := b.getAppendTo(i)
	var err error
	b.data, err = json.EncodeJSON(appendTo, j)
	if err != nil {
		colexecerror.ExpectedError(err)
	}
	b.offsets[i+1] = int32(len(b.data))
	b.maxSetLength = i + 1
}

// Window creates a "window" into the receiver. It behaves similarly to
// Golang's slice, but the returned object is *not* allowed to be modified - it
// is read-only. Window is a lightweight operation that doesn't involve copying
// the underlying data.
func (js *JSONs) Window(start, end int) *JSONs {
	return &JSONs{
		Bytes: *js.Bytes.Window(start, end),
	}
}

// CopySlice copies srcStartIdx inclusive and srcEndIdx exclusive []byte values
// from src into the receiver starting at destIdx. See Bytes.CopySlice.
func (js *JSONs) CopySlice(src *JSONs, destIdx, srcStartIdx, srcEndIdx int) {
	js.Bytes.CopySlice(&src.Bytes, destIdx, srcStartIdx, srcEndIdx)
}

// AppendSlice appends srcStartIdx inclusive and srcEndIdx exclusive JSON
// values from src into the receiver starting at destIdx.
func (js *JSONs) AppendSlice(src *JSONs, destIdx, srcStartIdx, srcEndIdx int) {
	js.Bytes.AppendSlice(&src.Bytes, destIdx, srcStartIdx, srcEndIdx)
}

// AppendVal appends the given JSON value to the end of the receiver.
func (js *JSONs) AppendVal(j json.JSON) {
	if j == nil {
		// A nil JSON indicates a NULL value in the column. We've got to insert a
		// "zero value" which in this case means an empty byte slice.
		js.Bytes.AppendVal(nil)
		return
	}
	b := &js.Bytes
	appendTo := b.getAppendTo(b.Len())
	var err error
	b.data, err = json.EncodeJSON(appendTo, j)
	if err != nil {
		colexecerror.ExpectedError(err)
	}
	b.offsets = append(b.offsets, int32(len(b.data)))
	b.maxSetLength = b.Len()
}

// String is used for debugging purposes.
func (js *JSONs) String() string {
	var builder strings.Builder
	for i := 0; i < js.Len(); i++ {
		builder.WriteString(
			fmt.Sprintf("%d: %s\n", i, js.Get(i)),
		)
	}
	return builder.String()
}
