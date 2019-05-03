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

package sqlutil

import (
	"bytes"
	goErr "errors"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/errors/errpgcode"
	"github.com/cockroachdb/cockroach/pkg/errors/errutil"
	"github.com/cockroachdb/cockroach/pkg/errors/safedetails"
	"github.com/cockroachdb/cockroach/pkg/util/pgcode"
)

// NewWithCode is like New except it also proposes a pg code.
func NewWithCode(code string, msg string) error {
	err := errutil.NewWithDepth(1, msg)
	err = errpgcode.WithCandidateCode(err, code)
	return err
}

// WrapWithCode is like Wrap except it also proposes a pg code.
func WrapWithCode(err error, code string, msg string) error {
	err = errutil.WrapWithDepth(1, err, msg)
	err = errpgcode.WithCandidateCode(err, code)
	return err
}

// NewWithCodef is like Newf except it also proposes a pg code.
func NewWithCodef(code string, format string, args ...interface{}) error {
	return NewWithCodeDepthf(1, code, format, args...)
}

// WrapWithCodef is like Wrapf except it also proposes a pg code.
func WrapWithCodef(err error, code string, format string, args ...interface{}) error {
	return WrapWithCodeDepthf(1, err, code, format, args...)
}

// NewWithCodeDepthf is like NewWithDepthfd except
// it also proposes a pg code.
func NewWithCodeDepthf(depth int, code string, format string, args ...interface{}) error {
	err := errutil.NewWithDepthf(1+depth, format, args...)
	err = errpgcode.WithCandidateCode(err, code)
	return err
}

// WrapWithCodeDepthf is like Wrapf except it also proposes a pg code.
func WrapWithCodeDepthf(
	depth int, err error, code string, format string, args ...interface{},
) error {
	err = errutil.WrapWithDepthf(1+depth, err, format, args...)
	err = errpgcode.WithCandidateCode(err, code)
	return err
}

// DangerousStatementf creates a new error for "rejected dangerous
// statements".
func DangerousStatementf(format string, args ...interface{}) error {
	var buf bytes.Buffer
	buf.WriteString("rejected: ")
	fmt.Fprintf(&buf, format, args...)
	buf.WriteString(" (sql_safe_updates = true)")
	err := goErr.New(buf.String())
	err = safedetails.WithSafeDetails(err, format, args...)
	err = errpgcode.WithCandidateCode(err, pgcode.Warning)
	return err
}
