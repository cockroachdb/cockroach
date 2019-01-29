// Copyright 2019 The Cockroach Authors.
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

package sqlsmith

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

type writability int

const (
	notWritable writability = iota
	writable
)

type column struct {
	name        string
	typ         types.T
	nullable    bool
	writability writability
}

type namedRelation struct {
	cols []column
	name string
}

// namer is a helper to generate names with unique prefixes.
type namer struct {
	counts map[string]int
}

func (n *namer) name(prefix string) string {
	n.counts[prefix] = n.counts[prefix] + 1
	return fmt.Sprintf("%s_%d", prefix, n.counts[prefix])
}

type scope struct {
	schema *schema

	// level is how deep we are in the scope tree - it is used as a heuristic
	// to eventually bottom out recursion (so we don't attempt to construct an
	// infinitely large join, or something).
	level int

	// refs is a slice of "tables" which can be referenced in an expression.
	// They are guaranteed to all have unique aliases.
	refs []tableRef

	// namer is used to generate unique table and column names.
	namer *namer

	// expr is the expression associated with this scope.
	expr relExpr
}

func (s *scope) push() *scope {
	return &scope{
		level:  s.level + 1,
		refs:   append(make([]tableRef, 0, len(s.refs)), s.refs...),
		namer:  s.namer,
		schema: s.schema,
	}
}

func (s *scope) name(prefix string) string {
	return s.namer.name(prefix)
}
