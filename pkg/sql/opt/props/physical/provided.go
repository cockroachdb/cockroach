// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
