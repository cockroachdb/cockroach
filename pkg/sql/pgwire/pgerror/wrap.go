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

package pgerror

import "github.com/cockroachdb/cockroach/pkg/errors"

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
	err = errors.WithCandidateCode(err, code)
	return err
}

// Wrap wraps an error and adds a pg error code. Only the code
// is added if the message is empty.
func Wrap(err error, code, msg string) error {
	if msg == "" {
		return errors.WithCandidateCode(err, code)
	}
	return WrapWithDepthf(1, err, code, "%s", msg)
}
