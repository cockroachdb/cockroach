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

type table struct {
	namedRelation
	schema       string
	isInsertable bool
	isBaseTable  bool
	constraints  []string
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
