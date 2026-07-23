// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package clisqlshell

import (
	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/cockroach/pkg/sql/scanner"
)

// We don't depend on tree in this package, but we do
// have access to the real tokenizer, so this file uses it
// to do some quick client-side parsing of SQL statements.
// Currently it's just testing whether the query about to be
// executed starts with the creation of a sinkless changefeed,
// which is mildly non-trivial to detect because changefeeds
// can contain arbitrary SQL expressions.

// token is the type of constants like lexbase.CREATE.
type token = int32

// statementType defines a specific tree walk that
// constitutes a pattern match. We match when we
// call Scan, it sees no more tokens, and we're
// at a node where matchEOF = true.
// We short-circuit and return false early if
// we get to a node with abortMatch=true or
// an undefined transition: the token is not in
// transitions and defaultTransition is unset.
type statementType struct{ parseNode }

// parseNode is a state we can enter while parsing
// a string to check for a specific match.
type parseNode struct {
	matchEOF          bool
	abortMatch        bool
	transitions       map[token]*parseNode
	defaultTransition *parseNode
}

func (t *parseNode) matches(stmts scanner.SQLScanner) bool {
	if t.abortMatch {
		return false
	}

	// Scan the next token. If there is none, we know
	// whether or not we've matched. Otherwise,
	// perform the state transition corresponding to the
	// token.
	var lval fakeSym
	stmts.Scan(&lval)
	if lval.id == 0 {
		return t.matchEOF
	}
	next, ok := t.transitions[lval.id]
	if ok {
		return next.matches(stmts)
	}
	// If no state transition is defined for this token,
	// do the default state transition if there is one,
	// otherwise this is a failed match.
	if t.defaultTransition != nil {
		return t.defaultTransition.matches(stmts)
	}
	return false
}

// CREATE CHANGEFEED with no INTO clause, or EXPERIMENTAL CHANGEFEED.
var createSinklessChangefeed statementType = statementType{
	parseNode{transitions: map[token]*parseNode{
		lexbase.CREATE:       &sinklessChangefeed,
		lexbase.EXPERIMENTAL: &sinklessChangefeed,
	}},
}

var noMatch parseNode = parseNode{abortMatch: true}

var sinklessChangefeedAnyArg = parseNode{
	matchEOF: true,
}

func init() {
	// Transitions that create a loop are added in init() to avoid a variable dependency cycle.
	sinklessChangefeedAnyArg.defaultTransition = &sinklessChangefeedArgs
}

// Return false on any INTO keyword not preceded by an AS.
// AS INTO is a result column alias, while any other use of
// INTO in a changefeed expression creates a sink.
var sinklessChangefeedArgs parseNode = parseNode{
	matchEOF: true,
	transitions: map[token]*parseNode{
		lexbase.INTO: &noMatch,
		lexbase.AS:   &sinklessChangefeedAnyArg,
	},
}

var sinklessChangefeed parseNode = parseNode{
	transitions: map[token]*parseNode{lexbase.CHANGEFEED: sinklessChangefeedArgs.repeated()},
}

// repeated causes a node to keep consuming tokens until it hits one it recognizes.
func (t *parseNode) repeated() *parseNode {
	t.defaultTransition = t
	return t
}

// fakeSym is a dummy implementation of the
// scanner.ScanSymType interface so that
// we can get token ids out of scanner.Scan().
type fakeSym struct {
	id  int32
	pos int32
	s   string
}

var _ scanner.ScanSymType = (*fakeSym)(nil)

func (s fakeSym) ID() int32                 { return s.id }
func (s *fakeSym) SetID(id int32)           { s.id = id }
func (s fakeSym) Pos() int32                { return s.pos }
func (s *fakeSym) SetPos(p int32)           { s.pos = p }
func (s fakeSym) Str() string               { return s.s }
func (s *fakeSym) SetStr(v string)          { s.s = v }
func (s fakeSym) UnionVal() interface{}     { return nil }
func (s fakeSym) SetUnionVal(v interface{}) {}
