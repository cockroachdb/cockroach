// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvflowsequencer

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// Sequencer issues monotonic sequencing timestamps derived from observed
// CreateTimes. This is a purpose-built data structure for replication admission
// control where we want to assign each AC-queued work below-raft a "sequence
// number" for FIFO ordering within a <tenant,priority>. We ensure timestamps
// are roughly monotonic with respect to log positions of replicated work[1] by
// sequencing work in log position order at the caller[2]. We also want it to
// track actual CreateTimes (similar to an HLC) for the global-FIFO ordering
// reasons explained in [1].
//
// It's not safe for concurrent access.
//
// [1]: See I12 from kvflowcontrol/doc.go.
// [2]: See kvflowhandle.Handle.
type Sequencer struct {
	// maxCreateTime ratchets to the highest observed CreateTime. If sequencing
	// work with lower CreateTimes, we continue generating monotonic sequence
	// numbers by incrementing it for every such sequencing attempt. Provided
	// work is sequenced in log position order, the sequencing timestamps
	// generated are also roughly monotonic with respect to log positions.
	maxCreateTime int64
}

// New returns a new Sequencer.
func New() *Sequencer {
	return &Sequencer{}
}

// Sequence returns a monotonically increasing timestamps derived from the
// provided CreateTime.
func (s *Sequencer) Sequence(ct time.Time) time.Time {
	createTime := ct.UnixNano()
	if createTime <= s.maxCreateTime {
		createTime = s.maxCreateTime + 1
	}
	s.maxCreateTime = createTime
	return timeutil.FromUnixNanos(createTime)
}
