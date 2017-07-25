// Copyright 2017 The Cockroach Authors.
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

package sql

import (
	"errors"
)

// Interval of rows to wait between cancellation checks.
const cancelCheckInterval int64 = 1000

// CancelChecker is an interface for an abstract cancellation checker,
// implemented by cancelChecker and nullCancelChecker.
type CancelChecker interface {
	Check() error
}

// Cancellation checker for internal planners (does nothing).
type nullCancelChecker struct{}

func (c *nullCancelChecker) Check() error {
	return nil
}

// Helper object for repeatedly checking whether an associated query has been
// cancelled or not. Encapsulates all logic for waiting for cancelCheckInterval
// rows before actually checking the cancellation flag.
type cancelChecker struct {
	// Reference to associated query's metadata object.
	queryMeta *queryMeta

	// Rows elapsed since the last time the cancellation check was made.
	rowsSinceLastCheck int64

	// Last returned cancellation value.
	isCancelled bool
}

// makeCancelChecker returns a new CancelChecker.
func makeCancelChecker(stmt *Statement) CancelChecker {
	// Return a nullCancelChecker for internal planners.
	if stmt == nil || stmt.queryMeta == nil {
		return &nullCancelChecker{}
	}

	return &cancelChecker{
		queryMeta: stmt.queryMeta,
	}
}

// check returns an error if the associated query has been cancelled.
func (c *cancelChecker) Check() error {
	if !c.isCancelled {
		c.isCancelled = c.queryMeta.isQueryCancelled(c.rowsSinceLastCheck%cancelCheckInterval == 0)
	}

	c.rowsSinceLastCheck++

	if c.isCancelled {
		return errors.New("query execution cancelled")
	}
	return nil
}

var _ CancelChecker = &cancelChecker{}
var _ CancelChecker = &nullCancelChecker{}
