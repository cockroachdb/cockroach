// Copyright 2018 The Cockroach Authors.
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

package physical

import (
	"bytes"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
)

// Provided physical properties of an operator. An operator might be able to
// satisfy a required property in multiple ways, and additional information is
// necessary for execution. For example, the required properties may allow
// multiple ordering choices; the provided properties would describe the
// specific ordering that has to be respected during execution.
//
// Provided properties are derived bottom-up (on the lowest cost tree).
type Provided struct {
	// Ordering is an ordering that needs to be maintained on the rows produced by
	// this operator in order to satisfy its required ordering. This is useful for
	// configuring execution in a distributed setting, where results from multiple
	// nodes may need to be merged. A best-effort attempt is made to have as few
	// columns as possible.
	//
	// The ordering, in conjunction with the functional dependencies (in the
	// logical properties), must intersect the required ordering.
	//
	// See the documentation for the opt/ordering package for some examples.
	Ordering opt.Ordering

	// Note: we store a Provided structure in-place within each group because the
	// struct is very small (see memo.bestProps). If we add more fields here, that
	// decision needs to be revisited.
}

// Equals returns true if the two sets of provided properties are identical.
func (p *Provided) Equals(other *Provided) bool {
	return p.Ordering.Equals(other.Ordering)
}

func (p *Provided) String() string {
	var buf bytes.Buffer

	if len(p.Ordering) > 0 {
		buf.WriteString("[ordering: ")
		p.Ordering.Format(&buf)
		buf.WriteByte(']')
	}

	return buf.String()
}
