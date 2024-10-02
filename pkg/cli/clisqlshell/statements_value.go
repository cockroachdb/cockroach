// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package clisqlshell

import "strings"

// StatementsValue is an implementation of pflag.Value that appends any
// argument to a slice.
type StatementsValue []string

// Type implements the pflag.Value interface.
func (s *StatementsValue) Type() string { return "<stmtlist>" }

// String implements the pflag.Value interface.
func (s *StatementsValue) String() string {
	return strings.Join(*s, ";")
}

// Set implements the pflag.Value interface.
func (s *StatementsValue) Set(value string) error {
	*s = append(*s, value)
	return nil
}
