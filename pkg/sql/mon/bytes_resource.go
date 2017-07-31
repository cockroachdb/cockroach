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
// Author: Alfonso Subiotto Marqués

package mon

import "github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"

// BytesResource is an interface used to abstract the specifics of tracking
// bytes usage by different types of resources.
type BytesResource interface {
	NewBudgetExceededError(requested int64, budget int64) error
}

// MemoryResource is a BytesResource that represents memory.
type MemoryResource struct{}

var _ BytesResource = MemoryResource{}

// NewBudgetExceededError implements the BytesResource interface.
func (m MemoryResource) NewBudgetExceededError(requested int64, budget int64) error {
	return pgerror.NewErrorf(
		pgerror.CodeOutOfMemoryError,
		"memory budget exceeded: %d bytes requested, %d bytes in budget",
		requested,
		budget,
	)
}

// DiskResource is a BytesResource that represents disk.
type DiskResource struct{}

var _ BytesResource = DiskResource{}

// NewBudgetExceededError implements the BytesResource interface.
func (d DiskResource) NewBudgetExceededError(requested int64, budget int64) error {
	return pgerror.NewErrorf(
		pgerror.CodeDiskFullError,
		"disk budget exceeded: %d bytes requested, %d bytes in budget",
		requested,
		budget,
	)
}
