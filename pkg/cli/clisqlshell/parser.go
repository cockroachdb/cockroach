// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package clisqlshell

import (
	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/cockroach/pkg/sql/scanner"
)

// We don't depend on tree in this package, but we do
// have access to the real tokenizer, so this file uses it
// to do some quick client-side parsing of SQL statements.
// Currently it's just testing whether the query about to be
// executed starts with the creation of a sinkless changefeed.

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
type statementType struct {
	matchEOF          bool
	abortMatch        bool
	transitions       map[token]*statementType
	defaultTransition *statementType
}

func (t *statementType) matches(stmts scanner.Scanner) bool {
	if t.abortMatch {
		return false
	}
	var lval fakeSym
	stmts.Scan(&lval)
	if lval.id == 0 {
		return t.matchEOF
	}
	next, ok := t.transitions[lval.id]
	if ok {
		return next.matches(stmts)
	}
	if t.defaultTransition != nil {
		return t.defaultTransition.matches(stmts)
	}
	return false
}

var createSinklessChangefeed statementType = statementType{
	transitions: map[token]*statementType{
		lexbase.CREATE:       &sinklessChangefeed,
		lexbase.EXPERIMENTAL: &sinklessChangefeed,
	},
}

var noMatch statementType = statementType{abortMatch: true}

var sinklessChangefeedAnyArg = statementType{
	matchEOF: true,
}

func init() {
	sinklessChangefeedAnyArg.defaultTransition = &sinklessChangefeedArgs
}

// Return false on any INTO keyword not preceded by an AS.
// AS INTO is a result column alias, while any other use of
// INTO in a changefeed expression creates a sink.
var sinklessChangefeedArgs statementType = statementType{
	matchEOF: true,
	transitions: map[token]*statementType{
		lexbase.INTO: &noMatch,
		lexbase.AS:   &sinklessChangefeedAnyArg,
	},
}

var sinklessChangefeed statementType = statementType{
	transitions: map[token]*statementType{lexbase.CHANGEFEED: sinklessChangefeedArgs.repeated()},
}

func (t *statementType) repeated() *statementType {
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
