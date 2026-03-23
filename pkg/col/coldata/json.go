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
// a new JSONEncoded object from scratch on demand.
type JSONs struct {
	Bytes
	// jsonScratch batches allocations of JSONEncoded objects (under the
	// assumption that the vector doesn't have NULLs).
	jsonScratch []json.JSONEncoded
	// scratch is a scratch space for encoding a JSON object on demand.
	scratch []byte
}

// NewJSONs returns a new JSONs presized to n elements.
func NewJSONs(n int) *JSONs {
	return &JSONs{
		Bytes:       *NewBytes(n),
		jsonScratch: make([]json.JSONEncoded, n),
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
	j := &js.jsonScratch[i]
	err := json.FromEncodingInto(bytes, j)
	if err != nil {
		colexecerror.ExpectedError(err)
	}
	return j
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
		// TODO(yuzefovich): in the general case, if we simply reused
		// js.jsonScratch here, it could lead to problems down the line (because
		// Get calls on js and on the window into js would be reusing the same
		// JSONEncoded object). However, in practice the windowed batch should
		// be consumed soon after it's created, without the overlap with Gets on
		// the original batch. Think through this.
		jsonScratch: make([]json.JSONEncoded, end-start),
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
	js.ensureJSONScratch()
}

// appendSliceWithSel appends all values specified in sel from the source into
// the receiver starting at position destIdx.
func (js *JSONs) appendSliceWithSel(src *JSONs, destIdx int, sel []int) {
	js.Bytes.appendSliceWithSel(&src.Bytes, destIdx, sel)
	js.ensureJSONScratch()
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

// Deserialize updates b according to the "arrow-like" format that was produced
// by Serialize.
func (js *JSONs) Deserialize(data []byte, offsets []int32) {
	js.Bytes.Deserialize(data, offsets)
	js.ensureJSONScratch()
}

func (js *JSONs) ensureJSONScratch() {
	if cap(js.jsonScratch) < js.Bytes.Len() {
		js.jsonScratch = make([]json.JSONEncoded, js.Bytes.Len())
	} else {
		js.jsonScratch = js.jsonScratch[:js.Bytes.Len()]
	}
}
