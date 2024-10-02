// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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

	// Distribution is a distribution that needs to be maintained on the rows
	// produced by this operator in order to satisfy its required distribution. If
	// there is a required distribution, the provided distribution must match it
	// exactly.
	//
	// The provided distribution is not yet used when building the DistSQL plan,
	// but eventually it should inform the decision about whether to plan
	// processors locally or remotely. Currently, it is used to determine whether
	// a Distribute operator is needed between this operator and its parent, which
	// can affect the cost of a plan.
	Distribution Distribution
}

// Equals returns true if the two sets of provided properties are identical.
func (p *Provided) Equals(other *Provided) bool {
	return p.Ordering.Equals(other.Ordering) && p.Distribution.Equals(other.Distribution)
}

func (p *Provided) String() string {
	var buf bytes.Buffer

	if len(p.Ordering) > 0 {
		buf.WriteString("[ordering: ")
		p.Ordering.Format(&buf)
		if p.Distribution.Any() {
			buf.WriteByte(']')
		} else {
			buf.WriteString(", ")
		}
	}

	if !p.Distribution.Any() {
		if len(p.Ordering) == 0 {
			buf.WriteByte('[')
		}
		buf.WriteString("distribution: ")
		p.Distribution.format(&buf)
		buf.WriteByte(']')
	}

	return buf.String()
}
