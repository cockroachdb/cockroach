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

// Resource is an interface used to abstract the specifics of tracking bytes
// usage by different types of resources.
type Resource interface {
	NewBudgetExceededError(requestedBytes int64, reservedBytes int64, budgetBytes int64) error
}

// memoryResource is a Resource that represents memory.
type memoryResource struct{}

// MemoryResource is a utility singleton used as an argument when creating a
// BytesMonitor to indicate that the monitor will be tracking memory usage.
var MemoryResource Resource = memoryResource{}

// NewBudgetExceededError implements the Resource interface.
func (m memoryResource) NewBudgetExceededError(
	requestedBytes int64, reservedBytes int64, budgetBytes int64,
) error {
	return pgerror.WithCandidateCode(
		errors.Newf(
			"memory budget exceeded: %d bytes requested, %d currently allocated, %d bytes in budget",
			errors.Safe(requestedBytes),
			errors.Safe(reservedBytes),
			errors.Safe(budgetBytes),
		), pgcode.OutOfMemory)
}

// diskResource is a Resource that represents disk.
type diskResource struct{}

// DiskResource is a utility singleton used as an argument when creating a
// BytesMonitor to indicate that the monitor will be tracking disk usage.
var DiskResource Resource = diskResource{}

// NewBudgetExceededError implements the Resource interface.
func (d diskResource) NewBudgetExceededError(
	requestedBytes int64, reservedBytes int64, budgetBytes int64,
) error {
	return pgerror.WithCandidateCode(
		errors.Newf(
			"disk budget exceeded: %d bytes requested, %d currently allocated, %d bytes in budget",
			errors.Safe(requestedBytes),
			errors.Safe(reservedBytes),
			errors.Safe(budgetBytes),
		), pgcode.DiskFull)
}
