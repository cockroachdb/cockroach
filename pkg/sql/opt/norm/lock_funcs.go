// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
