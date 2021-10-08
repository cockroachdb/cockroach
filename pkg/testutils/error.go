// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package testutils

import (
	"regexp"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
)

// IsError returns true if the error string matches the supplied regex.
// An empty regex is interpreted to mean that a nil error is expected.
func IsError(err error, re string) bool {
	if err == nil && re == "" {
		return true
	}
	if err == nil || re == "" {
		return false
	}
	errString := pgerror.FullError(err)
	matched, merr := regexp.MatchString(re, errString)
	if merr != nil {
		return false
	}
	return matched
}

// IsPError returns true if pErr's message matches the supplied regex.
// An empty regex is interpreted to mean that a nil error is expected.
func IsPError(pErr *roachpb.Error, re string) bool {
	if pErr == nil && re == "" {
		return true
	}
	if pErr == nil || re == "" {
		return false
	}
	matched, merr := regexp.MatchString(re, pErr.String())
	if merr != nil {
		return false
	}
	return matched
}
