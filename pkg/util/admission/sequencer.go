// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

import "sync/atomic"

// sequencer issues monotonic sequencing timestamps derived from observed
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
// ----
//
// Aside: Why not do this CreateTime-generation above raft? This is because these
// sequence numbers are encoded as part of the raft proposal[3], and at
// encode-time, we don't actually know what log position the proposal is going
// to end up in. It's hard to explicitly guarantee that a proposal with
// log-position P1 will get encoded before another with log position P2, where
// P1 < P2.
//
// If we tried to "approximate" CreateTimes at proposal-encode-time,
// approximating log position order, it could result in over-admission. This is
// because of how we return flow tokens -- up to some log index[4], and how use
// these sequence numbers in below-raft WorkQueues. If P2 ends up with a lower
// sequence number/CreateTime, it would get admitted first, and when returning
// flow tokens by log position, in specifying up-to-P2, we'll early return P1's
// flow tokens despite it not being admitted. So we'd over-admit at the sender.
// This is all within a <tenant,priority> pair.
//
// [1]: See I12 from kvflowcontrol/doc.go.
// [2]: See kvadmission.AdmitRaftEntry.
// [3]: In kvflowcontrolpb.RaftAdmissionMeta.
// [4]: See kvflowcontrolpb.AdmittedRaftLogEntries.
type sequencer struct {
	// maxCreateTime ratchets to the highest observed CreateTime. If sequencing
	// work with lower CreateTimes, we continue generating monotonic sequence
	// numbers by incrementing it for every such sequencing attempt. Provided
	// work is sequenced in log position order, the sequencing timestamps
	// generated are also roughly monotonic with respect to log positions.
	maxCreateTime int64
}

// sequence returns a monotonically increasing timestamps derived from the
// provided CreateTime.
func (s *sequencer) sequence(createTime int64) int64 {
	if createTime <= s.maxCreateTime {
		createTime = s.maxCreateTime + 1
	}
	// This counter is read concurrently in gcSequencers. We use an atomic store
	// here and an atomic load there, to avoid tripping up the race detector.
	atomic.StoreInt64(&s.maxCreateTime, createTime)
	return createTime
}
