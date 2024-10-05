// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package testutils

import (
	"regexp"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
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
func IsPError(pErr *kvpb.Error, re string) bool {
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
