// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package mon

import (
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/errors"
)

// Resource indicates the type of resource a BytesMonitor is tracking.
type Resource int8

const (
	MemoryResource Resource = iota
	DiskResource
)

func NewMemoryBudgetExceededError(
	requestedBytes int64, reservedBytes int64, budgetBytes int64,
) error {
	return pgerror.WithCandidateCode(
		errors.Newf(
			"memory budget exceeded: %d bytes requested, %d currently allocated, %d bytes in budget",
			errors.Safe(requestedBytes),
			errors.Safe(reservedBytes),
			errors.Safe(budgetBytes),
		),
		pgcode.OutOfMemory,
	)
}

func newDiskBudgetExceededError(
	requestedBytes int64, reservedBytes int64, budgetBytes int64,
) error {
	return pgerror.WithCandidateCode(
		errors.Newf(
			"disk budget exceeded: %d bytes requested, %d currently allocated, %d bytes in budget",
			errors.Safe(requestedBytes),
			errors.Safe(reservedBytes),
			errors.Safe(budgetBytes),
		),
		pgcode.DiskFull,
	)
}

func newRootSQLMemoryMonitorBudgetExceededError(
	requestedBytes int64, reservedBytes int64, budgetBytes int64,
) error {
	return errors.WithHint(
		NewMemoryBudgetExceededError(requestedBytes, reservedBytes, budgetBytes),
		"Consider increasing --max-sql-memory startup parameter.", /* hint */
	)
}
