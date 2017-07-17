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
//
// Author: Bilal Akhtar (bilal@cockroachlabs.com)

package sql

import (
	"errors"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// Interval of rows to wait between cancellation checks.
const cancelCheckInterval int64 = 100000

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

// makeCancelChecker returns a new cancelChecker.
func makeCancelChecker(p *planner) *cancelChecker {
	// Internal planners have no associated statement.
	if p.stmt == nil {
		return &cancelChecker{}
	}

	return &cancelChecker{
		queryMeta: p.stmt.queryMeta,
	}
}

// check returns an error if the associated query has been cancelled.
func (c *cancelChecker) Check() error {
	// No need to check anything for internal planners.
	if c.queryMeta == nil {
		return nil
	}

	if !c.isCancelled && c.rowsSinceLastCheck%cancelCheckInterval == 0 {
		c.isCancelled = atomic.LoadInt32(&c.queryMeta.isCancelled) == 1
	}

	c.rowsSinceLastCheck++

	if c.isCancelled {
		return errors.New("query cancelled by user")
	}
	return nil
}

var _ sqlbase.CancelChecker = &cancelChecker{}
