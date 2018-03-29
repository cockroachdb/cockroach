// Copyright 2014 The Cockroach Authors.
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

package result

import (
	"context"

	"github.com/kr/pretty"
	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// LocalResult is data belonging to an evaluated command that is
// only used on the node on which the command was proposed. Note that
// the proposing node may die before the local results are processed,
// so any side effects here are only best-effort.
type LocalResult struct {
	Reply *roachpb.BatchResponse

	// Intents stores any intents encountered but not conflicted with.
	// They should be handed off to asynchronous intent processing on the
	// proposer, so that an attempt to resolve them is made.
	//
	// This is a pointer to allow the zero (and as an unwelcome side effect,
	// all) values to be compared.
	Intents *[]IntentsWithArg
	// EndTxns stores completed transactions. If the transaction
	// contains unresolved intents, they should be handed off for
	// asynchronous intent resolution. A bool in each EndTxnIntents
	// indicates whether or not the intents must be left alone if the
	// corresponding command/proposal didn't succeed. For example,
	// resolving intents of a committing txn should not happen if the
	// commit fails, or we may accidentally make uncommitted values
	// live.
	EndTxns *[]EndTxnIntents
	// Whether we successfully or non-successfully requested a lease.
	//
	// TODO(tschottdorf): Update this counter correctly with prop-eval'ed KV
	// in the following case:
	// - proposal does not fail fast and goes through Raft
	// - downstream-of-Raft logic identifies a conflict and returns an error
	// The downstream-of-Raft logic does not exist at time of writing.
	LeaseMetricsResult *LeaseMetricsType

	// When set (in which case we better be the first range), call
	// GossipFirstRange if the Replica holds the lease.
	GossipFirstRange bool
	// Call MaybeGossipSystemConfig.
	MaybeGossipSystemConfig bool
	// Call MaybeAddToSplitQueue.
	MaybeAddToSplitQueue bool
	// Call MaybeGossipNodeLiveness with the specified Span, if set.
	MaybeGossipNodeLiveness *roachpb.Span

	// Set when transaction record(s) are updated, after calls to
	// EndTransaction or PushTxn. This is a pointer to allow the zero
	// (and as an unwelcome side effect, all) values to be compared.
	UpdatedTxns *[]*roachpb.Transaction
}

// DetachIntents returns (and removes) those intents from the
// LocalEvalResult which are supposed to be handled.
func (lResult *LocalResult) DetachIntents() []IntentsWithArg {
	if lResult == nil {
		return nil
	}
	var r []IntentsWithArg
	if lResult.Intents != nil {
		r = *lResult.Intents
	}
	lResult.Intents = nil
	return r
}

// DetachEndTxns returns (and removes) the EndTxnIntent objects from
// the local result. If alwaysOnly is true, the slice is filtered to
// include only those which have specified returnAlways=true, meaning
// the intents should be resolved regardless of whether the
// EndTransaction command succeeded.
func (lResult *LocalResult) DetachEndTxns(alwaysOnly bool) []EndTxnIntents {
	if lResult == nil {
		return nil
	}
	var r []EndTxnIntents
	if lResult.EndTxns != nil {
		for _, eti := range *lResult.EndTxns {
			if !alwaysOnly || eti.Always {
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
	Local      LocalResult
	Replicated storagebase.ReplicatedEvalResult
	WriteBatch *storagebase.WriteBatch
}

// IsZero reports whether p is the zero value.
func (p *Result) IsZero() bool {
	if p.Local != (LocalResult{}) {
		return false
	}
	if !p.Replicated.Equal(storagebase.ReplicatedEvalResult{}) {
		return false
	}
	if p.WriteBatch != nil {
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
			p.Replicated.State = &storagebase.ReplicaState{}
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
		if q.Replicated.State.TxnSpanGCThreshold != nil {
			if p.Replicated.State.TxnSpanGCThreshold == nil {
				p.Replicated.State.TxnSpanGCThreshold = q.Replicated.State.TxnSpanGCThreshold
			} else {
				p.Replicated.State.TxnSpanGCThreshold.Forward(*q.Replicated.State.TxnSpanGCThreshold)
			}
			q.Replicated.State.TxnSpanGCThreshold = nil
		}

		if q.Replicated.State.Stats != nil {
			return errors.New("must not specify Stats")
		}
		if (*q.Replicated.State != storagebase.ReplicaState{}) {
			log.Fatalf(context.TODO(), "unhandled EvalResult: %s",
				pretty.Diff(*q.Replicated.State, storagebase.ReplicaState{}))
		}
		q.Replicated.State = nil
	}

	p.Replicated.BlockReads = p.Replicated.BlockReads || q.Replicated.BlockReads
	q.Replicated.BlockReads = false

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

	if q.Local.Intents != nil {
		if p.Local.Intents == nil {
			p.Local.Intents = q.Local.Intents
		} else {
			*p.Local.Intents = append(*p.Local.Intents, *q.Local.Intents...)
		}
	}
	q.Local.Intents = nil

	if q.Local.EndTxns != nil {
		if p.Local.EndTxns == nil {
			p.Local.EndTxns = q.Local.EndTxns
		} else {
			*p.Local.EndTxns = append(*p.Local.EndTxns, *q.Local.EndTxns...)
		}
	}
	q.Local.EndTxns = nil

	if p.Local.LeaseMetricsResult == nil {
		p.Local.LeaseMetricsResult = q.Local.LeaseMetricsResult
	} else if q.Local.LeaseMetricsResult != nil {
		return errors.New("conflicting LeaseMetricsResult")
	}
	q.Local.LeaseMetricsResult = nil

	if p.Local.MaybeGossipNodeLiveness == nil {
		p.Local.MaybeGossipNodeLiveness = q.Local.MaybeGossipNodeLiveness
	} else if q.Local.MaybeGossipNodeLiveness != nil {
		return errors.New("conflicting MaybeGossipNodeLiveness")
	}
	q.Local.MaybeGossipNodeLiveness = nil

	coalesceBool(&p.Local.GossipFirstRange, &q.Local.GossipFirstRange)
	coalesceBool(&p.Local.MaybeGossipSystemConfig, &q.Local.MaybeGossipSystemConfig)
	coalesceBool(&p.Local.MaybeAddToSplitQueue, &q.Local.MaybeAddToSplitQueue)

	if q.Local.UpdatedTxns != nil {
		if p.Local.UpdatedTxns == nil {
			p.Local.UpdatedTxns = q.Local.UpdatedTxns
		} else {
			*p.Local.UpdatedTxns = append(*p.Local.UpdatedTxns, *q.Local.UpdatedTxns...)
		}
	}
	q.Local.UpdatedTxns = nil

	if !q.IsZero() {
		log.Fatalf(context.TODO(), "unhandled EvalResult: %s", pretty.Diff(q, Result{}))
	}

	return nil
}
