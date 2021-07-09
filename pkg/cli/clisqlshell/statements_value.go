// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
