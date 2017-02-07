// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Irfan Sharif (irfansharif@cockroachlabs.com)

package sql

import (
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
)

// RowBuffer is a buffer for rows of DTuples. Rows must be added using
// AddRow(), once the work is done the Close() method must be called to release
// the allocated memory.
//
// This is intended for nodes where it is simpler to compute a batch of rows at
// once instead of maintaining internal state in order to operate correctly
// under the constraints imposed by Next() and Values() under the planNode
// interface.
type RowBuffer struct {
	*RowContainer
	output parser.Datums
}

// Values here is analogous to Values() as defined under planNode.
//
// Available after Next(), result only valid until the next call to Next()
func (rb *RowBuffer) Values() parser.Datums {
	return rb.output
}

// Next here is analogous to Next() as defined under planNode, if no pre-computed
// results were buffered in prior to the call we return false. Else we stage the
// next output value for the subsequent call to Values().
func (rb *RowBuffer) Next() bool {
	if rb.Len() == 0 {
		return false
	}
	rb.output = rb.At(0)
	rb.PopFirst()
	return true
}
