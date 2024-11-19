// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	// scratch is a scratch space for encoding a JSON object on demand.
	scratch []byte
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

// Set sets the ith JSON in JSONs.
func (js *JSONs) Set(i int, j json.JSON) {
	var err error
	js.scratch, err = json.EncodeJSON(js.scratch[:0], j)
	if err != nil {
		colexecerror.ExpectedError(err)
	}
	js.Bytes.Set(i, js.scratch)
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

// Copy copies a single value from src at position srcIdx into position destIdx
// of the receiver.
func (js *JSONs) Copy(src *JSONs, destIdx, srcIdx int) {
	js.Bytes.Copy(&src.Bytes, destIdx, srcIdx)
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

// appendSliceWithSel appends all values specified in sel from the source into
// the receiver starting at position destIdx.
func (js *JSONs) appendSliceWithSel(src *JSONs, destIdx int, sel []int) {
	js.Bytes.appendSliceWithSel(&src.Bytes, destIdx, sel)
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
