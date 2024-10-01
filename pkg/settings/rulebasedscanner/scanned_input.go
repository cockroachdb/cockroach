// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rulebasedscanner

import "strings"

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

// Join concatenates the elements of its first argument to create a single
// string. The separator string sep is placed between elements in the resulting
// string.
func Join(elems []String, sep string) string {
	values := make([]string, len(elems))
	for idx := range elems {
		values[idx] = elems[idx].Value
	}
	return strings.Join(values, sep)
}
