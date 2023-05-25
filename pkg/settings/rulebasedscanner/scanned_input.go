// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rulebasedscanner

// ScannedInput represents the result of tokenizing the input
// configuration data.
//
// Inspired from pg's source, file src/backend/libpq/hba.c,
// function tokenize_file.
//
// The scanner tokenizes the input and stores the resulting data into
// three lists: a list of lines, a list of line numbers, and a list of
// raw line contents.
type ScannedInput struct {
	// The list of lines is a triple-nested list structure.  Each line is a list of
	// fields, and each field is a List of tokens.
	Lines   []Line
	Linenos []int
}

type Line struct {
	Input  string
	Tokens [][]String
}

// String is a possibly quoted string.
type String struct {
	Value  string
	Quoted bool
}

// String implements the fmt.Stringer interface.
func (s String) String() string {
	if s.Quoted {
		return `"` + s.Value + `"`
	}
	return s.Value
}

// Empty returns true iff s is the unquoted empty string.
func (s String) Empty() bool { return s.IsKeyword("") }

// IsKeyword returns whether s is the non-quoted string v.
func (s String) IsKeyword(v string) bool {
	return !s.Quoted && s.Value == v
}
