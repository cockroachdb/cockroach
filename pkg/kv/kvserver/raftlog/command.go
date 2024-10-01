// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package raftlog

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/apply"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/errors"
)

// ReplicatedCmd is the part of kvserver.replicatedCmd relevant for stand-alone log
// application, i.e. without having a surrounding *Replica present. In that
// case, it is assumed that only one replica is processed at a given time[^1], no
// in-memory state is kept, and no clients are waiting for responses to the
// commands. This obviates the need for concurrency control, in-memory state
// updates, reproposals, etc.
//
// [^1]: at least when the current command includes a split or a merge, i.e.
// changes replica boundaries.
type ReplicatedCmd struct {
	*Entry
	// The following struct is populated in shouldApplyCommand when we validate
	// that a command applies given the current lease and GC threshold. The
	// process of populating it is what transforms an apply.Command into an
	// apply.CheckedCommand.
	kvserverbase.ForcedErrResult
}

var _ apply.Command = (*ReplicatedCmd)(nil)
var _ apply.CheckedCommand = (*ReplicatedCmd)(nil)
var _ apply.AppliedCommand = (*ReplicatedCmd)(nil)

// Decode populates the receiver from the provided entry.
func (c *ReplicatedCmd) Decode(e *raftpb.Entry) error {
	if c.Entry != nil {
		c.Entry.Release()
		c.Entry = nil
	}

	var err error
	c.Entry, err = NewEntry(*e)
	if err != nil {
		return errors.Wrapf(err, "while decoding raft entry")
	}
	return nil
}

// Index implements apply.Command. It returns the index of the log entry that
// this Command applies.
func (c *ReplicatedCmd) Index() kvpb.RaftIndex {
	return kvpb.RaftIndex(c.Entry.Index)
}

// IsTrivial implements apply.Command. Trivial commands may be batched in an
// apply.Batch.
//
// See ReplicatedEvalResult.IsTrivial.
func (c *ReplicatedCmd) IsTrivial() bool {
	return c.ReplicatedResult().IsTrivial()
}

// IsLocal implements apply.Command.
//
// This base implementation returns false, since it doesn't keep track
// of whether a client is waiting for this Command.
func (c *ReplicatedCmd) IsLocal() bool {
	return false
}

// Ctx implements apply.Command. It returns context.Background().
func (c *ReplicatedCmd) Ctx() context.Context {
	return context.Background()
}

// AckErrAndFinish implements apply.Command.
//
// This base implementation is a no-op, since it doesn't keep track
// of whether a client is waiting for this Command.
func (c *ReplicatedCmd) AckErrAndFinish(context.Context, error) error {
	return nil
}

// AckOutcomeAndFinish implements apply.AppliedCommand.
//
// This base implementation is a no-op, since it doesn't keep track
// of whether a client is waiting for this Command.
func (c *ReplicatedCmd) AckOutcomeAndFinish(context.Context) error { return nil }

// Rejected implements apply.CheckedCommand.
//
// A command is rejected if it has a ForcedErr, i.e. if the state machines
// (deterministically) apply the associated entry as a no-op. See
// kvserverbase.CheckForcedErr.
func (c *ReplicatedCmd) Rejected() bool { return c.ForcedError != nil }

// CanAckBeforeApplication implements apply.CheckedCommand.
//
// This base implementation return false, since it doesn't keep track
// of whether a client is waiting for this Command.
func (c *ReplicatedCmd) CanAckBeforeApplication() bool { return false }

// AckSuccess implements apply.CheckedCommand.
//
// This base implementation return false, since it doesn't keep track
// of whether a client is waiting for this Command.
func (c *ReplicatedCmd) AckSuccess(context.Context) error { return nil }

// ReplicatedResult is a shorthand for the contained *ReplicatedEvalResult.
func (c *ReplicatedCmd) ReplicatedResult() *kvserverpb.ReplicatedEvalResult {
	return &c.Entry.Cmd.ReplicatedEvalResult
}
