// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package registry

import (
	"errors"
	"fmt"
)

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

func (ewo ErrorWithOwnership) Is(target error) bool {
	return errors.Is(ewo.Err, target)
}

func (ewo ErrorWithOwnership) As(reference interface{}) bool {
	return errors.As(ewo.Err, reference)
}

func WithTitleOverride(title string) errorOption {
	return func(ewo *ErrorWithOwnership) {
		ewo.TitleOverride = title
	}
}

func InfraFlake(ewo *ErrorWithOwnership) {
	ewo.InfraFlake = true
}

// ErrorWithOwner allows the caller to associate `err` with
// `owner`. When `t.Fatal` is called with an error of this type, the
// resulting GitHub issue is created and assigned to the team
// corresponding to `owner`.
func ErrorWithOwner(owner Owner, err error, opts ...errorOption) ErrorWithOwnership {
	result := ErrorWithOwnership{Owner: owner, Err: err}
	for _, opt := range opts {
		opt(&result)
	}

	return result
}
