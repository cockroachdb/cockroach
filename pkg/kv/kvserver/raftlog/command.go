// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
//

package raftlog

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/apply"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

// ReplicatedCmdBase is the part of a replicatedCmd relevant for stand-alone log
// application, i.e. without having a surrounding *Replica present. In that
// case, it is assumed that only one replica is processed at a given time[^1], no
// in-memory state is kept, and no clients are waiting for responses to the
// commands. This obviates the need for concurrency control, in-memory state
// updates, reproposals, etc.
//
// [^1]: at least when the current command includes a split or a merge, i.e.
// changes replica boundaries.
type ReplicatedCmdBase struct {
	*Entry
	// The following fields are set in shouldApplyCommand when we validate that
	// a command applies given the current lease and GC threshold. The process
	// of setting these fields is what transforms an apply.Command into an
	// apply.CheckedCommand.
	LeaseIndex uint64
	ForcedErr  *roachpb.Error
	Rejection  kvserverbase.ProposalRejectionType
}

var _ apply.Command = (*ReplicatedCmdBase)(nil)
var _ apply.CheckedCommand = (*ReplicatedCmdBase)(nil)
var _ apply.AppliedCommand = (*ReplicatedCmdBase)(nil)

// Decode populates the receiver from the provided entry.
func (c *ReplicatedCmdBase) Decode(e *raftpb.Entry) error {
	if c.Entry != nil {
		c.Entry.Release()
		c.Entry = nil
	}

	var err error
	c.Entry, err = NewEntry(*e)
	if err != nil {
		return kvserverbase.NonDeterministicFailureErrorWrapf(err, "while decoding raft entry")
	}
	return nil
}

// Index implements apply.Command.
func (c *ReplicatedCmdBase) Index() uint64 {
	return c.Entry.Index
}

// IsTrivial implements apply.Command.
func (c *ReplicatedCmdBase) IsTrivial() bool {
	return c.ReplicatedResult().IsTrivial()
}

// IsLocal implements apply.Command.
func (c *ReplicatedCmdBase) IsLocal() bool {
	return false
}

// Ctx implements apply.Command.
func (c *ReplicatedCmdBase) Ctx() context.Context {
	return context.Background()
}

// AckErrAndFinish implements apply.Command.
func (c *ReplicatedCmdBase) AckErrAndFinish(context.Context, error) error {
	return nil
}

// AckOutcomeAndFinish implements apply.AppliedCommand.
func (c *ReplicatedCmdBase) AckOutcomeAndFinish(context.Context) error { return nil }

// Rejected implements apply.CheckedCommand.
func (c *ReplicatedCmdBase) Rejected() bool { return c.ForcedErr != nil }

// CanAckBeforeApplication implements apply.CheckedCommand.
func (c *ReplicatedCmdBase) CanAckBeforeApplication() bool { return false }

// AckSuccess implements apply.CheckedCommand.
func (c *ReplicatedCmdBase) AckSuccess(context.Context) error { return nil }

// ReplicatedResult is a shorthand for the contained *ReplicatedEvalResult.
func (c *ReplicatedCmdBase) ReplicatedResult() *kvserverpb.ReplicatedEvalResult {
	return &c.Entry.Cmd.ReplicatedEvalResult
}
