// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package exit

import (
	"fmt"
	"os"

	"github.com/cockroachdb/redact"
)

// Code represents an exit code.
type Code struct {
	code int
}

// String implements the fmt.Stringer interface.
func (c Code) String() string { return fmt.Sprint(c.code) }

// Format implements the fmt.Formatter interface.
func (c Code) Format(s fmt.State, verb rune) {
	_, f := redact.MakeFormat(s, verb)
	fmt.Fprintf(s, f, c.code)
}

// SafeValue implements the redact.SafeValue interface.
func (c Code) SafeValue() {}

var _ redact.SafeValue = Code{}

// WithCode terminates the process and sets its exit status code to
// the provided code.
func WithCode(code Code) {
	os.Exit(code.code)
}
