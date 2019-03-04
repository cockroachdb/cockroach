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

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

// colRef refers to a named result column. If it is from a table, def is
// populated.
// TODO(mjibson): wrap this in a type somehow so that makeColRef can do
// better searching.
type colRef struct {
	typ  types.T
	item *tree.ColumnItem
}

type colRefs []*colRef

func (t colRefs) extend(refs ...*colRef) colRefs {
	ret := append(make(colRefs, 0, len(t)+len(refs)), t...)
	ret = append(ret, refs...)
	return ret
}

type scope struct {
	schema *schema

	// level is how deep we are in the scope tree - it is used as a heuristic
	// to eventually bottom out recursion (so we don't attempt to construct an
	// infinitely large join, or something).
	level int

	// namer is used to generate unique table and column names.
	namer *namer
}

func (s *scope) push() *scope {
	return &scope{
		level:  s.level + 1,
		namer:  s.namer,
		schema: s.schema,
	}
}

func (s *scope) name(prefix string) string {
	return s.namer.name(prefix)
}

// namer is a helper to generate names with unique prefixes.
type namer struct {
	counts map[string]int
}

func (n *namer) name(prefix string) string {
	n.counts[prefix] = n.counts[prefix] + 1
	return fmt.Sprintf("%s_%d", prefix, n.counts[prefix])
}
