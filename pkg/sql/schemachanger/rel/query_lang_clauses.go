// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rel

import "gopkg.in/yaml.v3"

// Clauses exists to handle flattening of a slice of clauses before marshaling.
type Clauses []Clause

func flattened(c Clauses) Clauses {
	hasAnd := func() bool {
		for _, cl := range c {
			if _, isAnd := cl.(and); isAnd {
				return true
			}
		}
		return false
	}
	if !hasAnd() {
		return c
	}
	var ret Clauses
	for _, cl := range c {
		switch cl := cl.(type) {
		case and:
			ret = append(ret, flattened(Clauses(cl))...)
		default:
			ret = append(ret, cl)
		}
	}
	return ret
}

// MarshalYAML marshals clauses to yaml.
func (c Clauses) MarshalYAML() (interface{}, error) {
	fc := flattened(c)
	var n yaml.Node
	if err := n.Encode([]Clause(fc)); err != nil {
		return nil, err
	}
	n.Style = yaml.LiteralStyle
	return &n, nil
}
