// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
