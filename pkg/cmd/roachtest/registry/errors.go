// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package registry

import "fmt"

type (
	ErrorWithOwnership struct {
		// TitleOverride allows errors to overwrite the "{testName}
		// failed" title used in issues. This allows issues to be grouped
		// even if they happen in different tests.
		TitleOverride string
		// InfraFlake indicates that this error is an infrastructure
		// flake, and the issue will be labeled accordingly.
		InfraFlake bool
		Owner      Owner
		Err        error
	}

	errorOption func(*ErrorWithOwnership)
)

func (ewo ErrorWithOwnership) Error() string {
	return fmt.Sprintf("%s [owner=%s]", ewo.Err.Error(), ewo.Owner)
}

func WithTitleOverride(title string) errorOption {
	return func(ewo *ErrorWithOwnership) {
		ewo.TitleOverride = title
	}
}

func InfraFlake(ewo *ErrorWithOwnership) {
	ewo.InfraFlake = true
}

// ErrorWithOwner allows an the caller to label that the given `err`
// should be triaged to the given `owner`. When `t.Fatal` is called
// with an error of this type, the resulting GitHub issue is created
// and assigned to the team corresponding to `owner`.
func ErrorWithOwner(owner Owner, err error, opts ...errorOption) ErrorWithOwnership {
	result := ErrorWithOwnership{Owner: owner, Err: err}
	for _, opt := range opts {
		opt(&result)
	}

	return result
}
