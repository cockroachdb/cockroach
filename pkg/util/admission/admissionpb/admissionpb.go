// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package admissionpb

import "math"

// WorkPriority represents the priority of work. In an WorkQueue, it is only
// used for ordering within a tenant. High priority work can starve lower
// priority work.
type WorkPriority int8

const (
	// LowPri is low priority work.
	LowPri WorkPriority = math.MinInt8
	// TTLLowPri is low priority work from TTL internal submissions.
	TTLLowPri WorkPriority = -100
	// UserLowPri is low priority work from user submissions (SQL).
	UserLowPri WorkPriority = -50
	// BulkNormalPri is bulk priority work from bulk jobs, which could be run due
	// to user submissions or be automatic.
	BulkNormalPri WorkPriority = -30
	// NormalPri is normal priority work.
	NormalPri WorkPriority = 0
	// UserHighPri is high priority work from user submissions (SQL).
	UserHighPri WorkPriority = 50
	// InternalNormalPri is normal priority work that originates from verified
	// internal sources. The work maybe from a user submission (SQL) but the
	// user will be an non-standard SQL user with a specific need to have
	// elevated priority. For example, a CC created user that exists for the
	// sole purpose of sending SQL probe queries that validate the availability
	// of the cluster could create work with this priority level.
	InternalNormalPri WorkPriority = 75
	// LockingPri is for transactions that are acquiring locks.
	LockingPri WorkPriority = 100
	// HighPri is high priority work.
	HighPri WorkPriority = math.MaxInt8
	// OneAboveHighPri is one priority level above the highest priority.
	OneAboveHighPri int = int(HighPri) + 1
)

// Prevent the linter from emitting unused warnings.
var _ = LowPri
var _ = TTLLowPri
var _ = UserLowPri
var _ = NormalPri
var _ = UserHighPri
var _ = InternalNormalPri
var _ = LockingPri
var _ = HighPri
