// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package pgerror

import "github.com/cockroachdb/errors"

// Wrapf wraps an error and adds a pg error code. See
// the doc on WrapWithDepthf for details.
func Wrapf(err error, code, format string, args ...interface{}) error {
	return WrapWithDepthf(1, err, code, format, args...)
}

// WrapWithDepthf wraps an error. It also annotates the provided
// pg code as new candidate code, to be used if the underlying
// error does not have one already.
func WrapWithDepthf(depth int, err error, code, format string, args ...interface{}) error {
	err = errors.WrapWithDepthf(1+depth, err, format, args...)
	err = WithCandidateCode(err, code)
	return err
}

// Wrap wraps an error and adds a pg error code. Only the code
// is added if the message is empty.
func Wrap(err error, code, msg string) error {
	if msg == "" {
		return WithCandidateCode(err, code)
	}
	return WrapWithDepthf(1, err, code, "%s", msg)
}
