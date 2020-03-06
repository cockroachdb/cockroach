// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package result

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/storagepb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/kr/pretty"
	"github.com/pkg/errors"
)

// LocalResult is data belonging to an evaluated command that is
// only used on the node on which the command was proposed. Note that
// the proposing node may die before the local results are processed,
// so any side effects here are only best-effort.
type LocalResult struct {
	Reply *roachpb.BatchResponse

	// EncounteredIntents stores any intents from other transactions that the
	// request encountered but did not conflict with. They should be handed off
	// to asynchronous intent processing on the proposer, so that an attempt to
	// resolve them is made.
	EncounteredIntents []roachpb.Intent
	// AcquiredLocks stores any newly acquired or re-acquired locks.
	AcquiredLocks []roachpb.LockUpdate
	// ResolvedLocks stores any resolved lock spans, either with finalized or
	// pending statuses. Unlike AcquiredLocks and EncounteredIntents, values in
	// this slice will represent spans of locks that were resolved.
	ResolvedLocks []roachpb.LockUpdate
	// UpdatedTxns stores transaction records that have been updated by
	// calls to EndTxn, PushTxn, and RecoverTxn.
	UpdatedTxns []*roachpb.Transaction
	// EndTxns stores completed transactions. If the transaction
	// contains unresolved intents, they should be handed off for
	// asynchronous intent resolution. A bool in each EndTxnIntents
	// indicates whether or not the intents must be left alone if the
	// corresponding command/proposal didn't succeed. For example,
	// resolving intents of a committing txn should not happen if the
	// commit fails, or we may accidentally make uncommitted values
	// live.
	EndTxns []EndTxnIntents

	// When set (in which case we better be the first range), call
	// GossipFirstRange if the Replica holds the lease.
	GossipFirstRange bool
	// Call MaybeGossipSystemConfig.
	MaybeGossipSystemConfig bool
	// Call MaybeAddToSplitQueue.
	MaybeAddToSplitQueue bool
	// Call MaybeGossipNodeLiveness with the specified Span, if set.
	MaybeGossipNodeLiveness *roachpb.Span
	// Call maybeWatchForMerge.
	MaybeWatchForMerge bool

	// Metrics contains counters which are to be passed to the
	// metrics subsystem.
	Metrics *Metrics
}

// IsZero reports whether lResult is the zero value.
func (lResult *LocalResult) IsZero() bool {
	// NB: keep in order.
	return lResult.Reply == nil &&
		lResult.EncounteredIntents == nil &&
		lResult.AcquiredLocks == nil &&
		lResult.ResolvedLocks == nil &&
		lResult.UpdatedTxns == nil &&
		lResult.EndTxns == nil &&
		!lResult.GossipFirstRange &&
		!lResult.MaybeGossipSystemConfig &&
		lResult.MaybeGossipNodeLiveness == nil &&
		!lResult.MaybeWatchForMerge &&
		lResult.Metrics == nil
}

func (lResult *LocalResult) String() string {
	if lResult == nil {
		return "LocalResult: nil"
	}
	return fmt.Sprintf("LocalResult (reply: %v, "+
		"#encountered intents: %d, #acquired locks: %d, #resolved locks: %d"+
		"#updated txns: %d #end txns: %d, "+
		"GossipFirstRange:%t MaybeGossipSystemConfig:%t MaybeAddToSplitQueue:%t "+
		"MaybeGossipNodeLiveness:%s MaybeWatchForMerge:%t",
		lResult.Reply,
		len(lResult.EncounteredIntents), len(lResult.AcquiredLocks), len(lResult.ResolvedLocks),
		len(lResult.UpdatedTxns), len(lResult.EndTxns),
		lResult.GossipFirstRange, lResult.MaybeGossipSystemConfig, lResult.MaybeAddToSplitQueue,
		lResult.MaybeGossipNodeLiveness, lResult.MaybeWatchForMerge)
}

// DetachEncounteredIntents returns (and removes) those encountered
// intents from the LocalEvalResult which are supposed to be handled.
func (lResult *LocalResult) DetachEncounteredIntents() []roachpb.Intent {
	if lResult == nil {
		return nil
	}
	r := lResult.EncounteredIntents
	lResult.EncounteredIntents = nil
	return r
}

// DetachEndTxns returns (and removes) the EndTxnIntent objects from
// the local result. If alwaysOnly is true, the slice is filtered to
// include only those which have specified returnAlways=true, meaning
// the intents should be resolved regardless of whether the
// EndTxn command succeeded.
func (lResult *LocalResult) DetachEndTxns(alwaysOnly bool) []EndTxnIntents {
	if lResult == nil {
		return nil
	}
	r := lResult.EndTxns
	if alwaysOnly {
		// If alwaysOnly, filter away any !Always EndTxnIntents.
		r = r[:0]
		for _, eti := range lResult.EndTxns {
			if eti.Always {
				r = append(r, eti)
			}
		}
	}
	lResult.EndTxns = nil
	return r
}

// Result is the result of evaluating a KV request. That is, the
// proposer (which holds the lease, at least in the case in which the command
// will complete successfully) has evaluated the request and is holding on to:
//
// a) changes to be written to disk when applying the command
// b) changes to the state which may require special handling (i.e. code
//    execution) on all Replicas
// c) data which isn't sent to the followers but the proposer needs for tasks
//    it must run when the command has applied (such as resolving intents).
type Result struct {
	Local        LocalResult
	Replicated   storagepb.ReplicatedEvalResult
	WriteBatch   *storagepb.WriteBatch
	LogicalOpLog *storagepb.LogicalOpLog
}

// IsZero reports whether p is the zero value.
func (p *Result) IsZero() bool {
	if !p.Local.IsZero() {
		return false
	}
	if !p.Replicated.Equal(storagepb.ReplicatedEvalResult{}) {
		return false
	}
	if p.WriteBatch != nil {
		return false
	}
	if p.LogicalOpLog != nil {
		return false
	}
	return true
}

// coalesceBool ORs rhs into lhs and then zeroes rhs.
func coalesceBool(lhs *bool, rhs *bool) {
	*lhs = *lhs || *rhs
	*rhs = false
}

// MergeAndDestroy absorbs the supplied EvalResult while validating that the
// resulting EvalResult makes sense. For example, it is forbidden to absorb
// two lease updates or log truncations, or multiple splits and/or merges.
//
// The passed EvalResult must not be used once passed to Merge.
func (p *Result) MergeAndDestroy(q Result) error {
	if q.Replicated.State != nil {
		if q.Replicated.State.RaftAppliedIndex != 0 {
			return errors.New("must not specify RaftApplyIndex")
		}
		if q.Replicated.State.LeaseAppliedIndex != 0 {
			return errors.New("must not specify RaftApplyIndex")
		}
		if p.Replicated.State == nil {
			p.Replicated.State = &storagepb.ReplicaState{}
		}
		if p.Replicated.State.Desc == nil {
			p.Replicated.State.Desc = q.Replicated.State.Desc
		} else if q.Replicated.State.Desc != nil {
			return errors.New("conflicting RangeDescriptor")
		}
		q.Replicated.State.Desc = nil

		if p.Replicated.State.Lease == nil {
			p.Replicated.State.Lease = q.Replicated.State.Lease
		} else if q.Replicated.State.Lease != nil {
			return errors.New("conflicting Lease")
		}
		q.Replicated.State.Lease = nil

		if p.Replicated.State.TruncatedState == nil {
			p.Replicated.State.TruncatedState = q.Replicated.State.TruncatedState
		} else if q.Replicated.State.TruncatedState != nil {
			return errors.New("conflicting TruncatedState")
		}
		q.Replicated.State.TruncatedState = nil

		if q.Replicated.State.GCThreshold != nil {
			if p.Replicated.State.GCThreshold == nil {
				p.Replicated.State.GCThreshold = q.Replicated.State.GCThreshold
			} else {
				p.Replicated.State.GCThreshold.Forward(*q.Replicated.State.GCThreshold)
			}
			q.Replicated.State.GCThreshold = nil
		}

		if q.Replicated.State.Stats != nil {
			return errors.New("must not specify Stats")
		}
		if (*q.Replicated.State != storagepb.ReplicaState{}) {
			log.Fatalf(context.TODO(), "unhandled EvalResult: %s",
				pretty.Diff(*q.Replicated.State, storagepb.ReplicaState{}))
		}
		q.Replicated.State = nil
	}

	if p.Replicated.Split == nil {
		p.Replicated.Split = q.Replicated.Split
	} else if q.Replicated.Split != nil {
		return errors.New("conflicting Split")
	}
	q.Replicated.Split = nil

	if p.Replicated.Merge == nil {
		p.Replicated.Merge = q.Replicated.Merge
	} else if q.Replicated.Merge != nil {
		return errors.New("conflicting Merge")
	}
	q.Replicated.Merge = nil

	if p.Replicated.ChangeReplicas == nil {
		p.Replicated.ChangeReplicas = q.Replicated.ChangeReplicas
	} else if q.Replicated.ChangeReplicas != nil {
		return errors.New("conflicting ChangeReplicas")
	}
	q.Replicated.ChangeReplicas = nil

	if p.Replicated.ComputeChecksum == nil {
		p.Replicated.ComputeChecksum = q.Replicated.ComputeChecksum
	} else if q.Replicated.ComputeChecksum != nil {
		return errors.New("conflicting ComputeChecksum")
	}
	q.Replicated.ComputeChecksum = nil

	if p.Replicated.RaftLogDelta == 0 {
		p.Replicated.RaftLogDelta = q.Replicated.RaftLogDelta
	} else if q.Replicated.RaftLogDelta != 0 {
		return errors.New("conflicting RaftLogDelta")
	}
	q.Replicated.RaftLogDelta = 0

	if p.Replicated.AddSSTable == nil {
		p.Replicated.AddSSTable = q.Replicated.AddSSTable
	} else if q.Replicated.AddSSTable != nil {
		return errors.New("conflicting AddSSTable")
	}
	q.Replicated.AddSSTable = nil

	if q.Replicated.SuggestedCompactions != nil {
		if p.Replicated.SuggestedCompactions == nil {
			p.Replicated.SuggestedCompactions = q.Replicated.SuggestedCompactions
		} else {
			p.Replicated.SuggestedCompactions = append(p.Replicated.SuggestedCompactions, q.Replicated.SuggestedCompactions...)
		}
	}
	q.Replicated.SuggestedCompactions = nil

	if p.Replicated.PrevLeaseProposal == nil {
		p.Replicated.PrevLeaseProposal = q.Replicated.PrevLeaseProposal
	} else if q.Replicated.PrevLeaseProposal != nil {
		return errors.New("conflicting lease expiration")
	}
	q.Replicated.PrevLeaseProposal = nil

	if p.Local.EncounteredIntents == nil {
		p.Local.EncounteredIntents = q.Local.EncounteredIntents
	} else {
		p.Local.EncounteredIntents = append(p.Local.EncounteredIntents, q.Local.EncounteredIntents...)
	}
	q.Local.EncounteredIntents = nil

	if p.Local.AcquiredLocks == nil {
		p.Local.AcquiredLocks = q.Local.AcquiredLocks
	} else {
		p.Local.AcquiredLocks = append(p.Local.AcquiredLocks, q.Local.AcquiredLocks...)
	}
	q.Local.AcquiredLocks = nil

	if p.Local.ResolvedLocks == nil {
		p.Local.ResolvedLocks = q.Local.ResolvedLocks
	} else {
		p.Local.ResolvedLocks = append(p.Local.ResolvedLocks, q.Local.ResolvedLocks...)
	}
	q.Local.ResolvedLocks = nil

	if p.Local.UpdatedTxns == nil {
		p.Local.UpdatedTxns = q.Local.UpdatedTxns
	} else {
		p.Local.UpdatedTxns = append(p.Local.UpdatedTxns, q.Local.UpdatedTxns...)
	}
	q.Local.UpdatedTxns = nil

	if p.Local.EndTxns == nil {
		p.Local.EndTxns = q.Local.EndTxns
	} else {
		p.Local.EndTxns = append(p.Local.EndTxns, q.Local.EndTxns...)
	}
	q.Local.EndTxns = nil

	if p.Local.MaybeGossipNodeLiveness == nil {
		p.Local.MaybeGossipNodeLiveness = q.Local.MaybeGossipNodeLiveness
	} else if q.Local.MaybeGossipNodeLiveness != nil {
		return errors.New("conflicting MaybeGossipNodeLiveness")
	}
	q.Local.MaybeGossipNodeLiveness = nil

	coalesceBool(&p.Local.GossipFirstRange, &q.Local.GossipFirstRange)
	coalesceBool(&p.Local.MaybeGossipSystemConfig, &q.Local.MaybeGossipSystemConfig)
	coalesceBool(&p.Local.MaybeAddToSplitQueue, &q.Local.MaybeAddToSplitQueue)
	coalesceBool(&p.Local.MaybeWatchForMerge, &q.Local.MaybeWatchForMerge)

	if p.Local.Metrics == nil {
		p.Local.Metrics = q.Local.Metrics
	} else if q.Local.Metrics != nil {
		p.Local.Metrics.Add(*q.Local.Metrics)
	}
	q.Local.Metrics = nil

	if q.LogicalOpLog != nil {
		if p.LogicalOpLog == nil {
			p.LogicalOpLog = q.LogicalOpLog
		} else {
			p.LogicalOpLog.Ops = append(p.LogicalOpLog.Ops, q.LogicalOpLog.Ops...)
		}
	}
	q.LogicalOpLog = nil

	if !q.IsZero() {
		log.Fatalf(context.TODO(), "unhandled EvalResult: %s", pretty.Diff(q, Result{}))
	}

	return nil
}
