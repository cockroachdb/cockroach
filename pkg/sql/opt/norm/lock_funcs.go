// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package norm

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// LockUsesSkipLocked returns true if the Lock expression uses the SKIP LOCKED
// wait policy.
func (c *CustomFuncs) LockUsesSkipLocked(private *memo.LockPrivate) bool {
	return private.Locking.WaitPolicy == tree.LockWaitSkipLocked
}
