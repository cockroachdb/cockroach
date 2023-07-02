// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/apply"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logcrash"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"go.etcd.io/raft/v3/raftpb"
)

// replica_application_*.go files provide concrete implementations of
// the interfaces defined in the storage/apply package:
//
// replica_application_state_machine.go  ->  apply.StateMachine
// replica_application_decoder.go        ->  apply.Decoder
// replica_application_cmd.go            ->  apply.Command         (and variants)
// replica_application_cmd_buf.go        ->  apply.CommandIterator (and variants)
// replica_application_cmd_buf.go        ->  apply.CommandList     (and variants)
//
// These allow Replica to interface with the storage/apply package.

// replicaDecoder implements the apply.Decoder interface.
//
// The object is capable of decoding committed raft entries into a list of
// replicatedCmd objects (which implement all variants of apply.Command), binding
// these commands to their local proposals, and providing an iterator over these
// commands.
type replicaDecoder struct {
	r      *Replica
	cmdBuf replicatedCmdBuf
}

// getDecoder returns the Replica's apply.Decoder. The Replica's raftMu
// is held for the entire lifetime of the replicaDecoder.
func (r *Replica) getDecoder() *replicaDecoder {
	d := &r.raftMu.decoder
	d.r = r
	return d
}

// DecodeAndBind implements the apply.Decoder interface.
func (d *replicaDecoder) DecodeAndBind(ctx context.Context, ents []raftpb.Entry) (bool, error) {
	if err := d.decode(ctx, ents); err != nil {
		return false, err
	}
	anyLocal := d.retrieveLocalProposals(ctx)
	d.createTracingSpans(ctx)
	return anyLocal, nil
}

// decode decodes the provided entries into the decoder.
func (d *replicaDecoder) decode(ctx context.Context, ents []raftpb.Entry) error {
	for i := range ents {
		ent := &ents[i]
		if err := d.cmdBuf.allocate().Decode(ent); err != nil {
			return err
		}
	}
	return nil
}

// retrieveLocalProposalsV2 is used with useReproposalsV2, replacing a call
// to retrieveLocalProposals. The V2 implementation is simpler because a log
// entry that comes up for a local proposal can always consume that proposal
// from the map because V2 never mutates the MaxLeaseIndex for the same proposal.
// In contrast, with V1, we can only remove the proposal from the map once we
// have found a log entry that had a matching MaxLeaseIndex. This lead to the
// complexity of having multiple entries associated to the same proposal during
// application.
func (d *replicaDecoder) retrieveLocalProposalsV2() (anyLocal bool) {
	d.r.mu.Lock()
	defer d.r.mu.Unlock()

	var it replicatedCmdBufSlice

	for it.init(&d.cmdBuf); it.Valid(); it.Next() {
		cmd := it.cur()
		cmd.proposal = d.r.mu.proposals[cmd.ID]
		var alloc *quotapool.IntAlloc
		if cmd.proposal != nil {
			// INVARIANT: a proposal is consumed (i.e. removed from the proposals map)
			// the first time it comes up for application. (If the proposal fails due
			// to an illegal LeaseAppliedIndex, a new proposal might be spawned to
			// retry but that proposal will be unrelated as far as log application is
			// concerned).
			//
			// INVARIANT: local proposals are not in the proposals map while being
			// applied, and they never re-enter the proposals map either during or
			// afterwards.
			//
			// (propBuf.{Insert,ReinsertLocked} ignores proposals that have
			// v2SeenDuringApplicationSet to make this true).
			if cmd.proposal.v2SeenDuringApplication {
				err := errors.AssertionFailedf("ProposalData seen twice during application: %+v", cmd.proposal)
				logcrash.ReportOrPanic(d.r.AnnotateCtx(cmd.ctx), &d.r.store.ClusterSettings().SV, "%v", err)
				// If we didn't panic, treat the proposal as non-local. This makes sure
				// we don't repropose it under a new lease index.
				cmd.proposal = nil
			} else {
				cmd.proposal.v2SeenDuringApplication = true
				anyLocal = true
				delete(d.r.mu.proposals, cmd.ID)
				if d.r.mu.proposalQuota != nil {
					alloc = cmd.proposal.quotaAlloc
					cmd.proposal.quotaAlloc = nil
				}
			}
		}

		// NB: this may append nil. It's intentional. The quota release queue
		// needs to have one slice entry per entry applied (even if the entry
		// is rejected).
		//
		// TODO(tbg): there used to be an optimization where we'd elide mutating
		// this slice until we saw a local proposal under a populated
		// b.r.mu.proposalQuota. We can bring it back.
		if d.r.mu.proposalQuota != nil {
			d.r.mu.quotaReleaseQueue = append(d.r.mu.quotaReleaseQueue, alloc)
		}
	}
	return anyLocal
}

// retrieveLocalProposals binds each of the decoder's commands to their local
// proposals if they were proposed locally. The method also sets the ctx fields
// on all commands.
func (d *replicaDecoder) retrieveLocalProposals(ctx context.Context) (anyLocal bool) {
	if useReproposalsV2 {
		// NB: we *must* use this new code for correctness, since we have an invariant
		// described within.
		return d.retrieveLocalProposalsV2()
	}
	d.r.mu.Lock()
	defer d.r.mu.Unlock()
	// Assign all the local proposals first then delete all of them from the map
	// in a second pass. This ensures that we retrieve all proposals correctly
	// even if the applier has multiple entries for the same proposal, in which
	// case the proposal was reproposed (either under its original or a new
	// MaxLeaseIndex) which we handle in a second pass below.
	var it replicatedCmdBufSlice
	for it.init(&d.cmdBuf); it.Valid(); it.Next() {
		cmd := it.cur()
		cmd.proposal = d.r.mu.proposals[cmd.ID]
		anyLocal = anyLocal || cmd.IsLocal()
	}
	if !anyLocal && d.r.mu.proposalQuota == nil {
		// Fast-path.
		return false
	}
	for it.init(&d.cmdBuf); it.Valid(); it.Next() {
		cmd := it.cur()
		var toRelease *quotapool.IntAlloc
		shouldRemove := cmd.IsLocal() &&
			// If this entry does not have the most up-to-date view of the
			// corresponding proposal's maximum lease index then the proposal
			// must have been reproposed with a higher lease index. (see
			// tryReproposeWithNewLeaseIndex). In that case, there's a newer
			// version of the proposal in the pipeline, so don't remove the
			// proposal from the map. We expect this entry to be rejected by
			// checkForcedErr.
			//
			// Note that lease proposals always use a MaxLeaseIndex of zero (since
			// they have their own replay protection), so they always meet this
			// criterion. While such proposals can be reproposed, only the first
			// instance that gets applied matters and so removing the command is
			// always what we want to happen.
			cmd.Cmd.MaxLeaseIndex == cmd.proposal.command.MaxLeaseIndex
		if shouldRemove {
			// Delete the proposal from the proposals map. There may be reproposals
			// of the proposal in the pipeline, but those will all have the same max
			// lease index, meaning that they will all be rejected after this entry
			// applies (successfully or otherwise). If tryReproposeWithNewLeaseIndex
			// picks up the proposal on failure, it will re-add the proposal to the
			// proposal map, but this won't affect this replicaApplier.
			//
			// While here, add the proposal's quota size to the quota release queue.
			// We check the proposal map again first to avoid double free-ing quota
			// when reproposals from the same proposal end up in the same entry
			// application batch.
			delete(d.r.mu.proposals, cmd.ID)
			toRelease = cmd.proposal.quotaAlloc
			cmd.proposal.quotaAlloc = nil
		}
		// At this point we're not guaranteed to have proposalQuota initialized,
		// the same is true for quotaReleaseQueues. Only queue the proposal's
		// quota for release if the proposalQuota is initialized.
		if d.r.mu.proposalQuota != nil {
			d.r.mu.quotaReleaseQueue = append(d.r.mu.quotaReleaseQueue, toRelease)
		}
	}
	return anyLocal
}

// createTracingSpans creates and assigns a new tracing span for each decoded
// command. If a command was proposed locally, it will be given a tracing span
// that follows from its proposal's span.
func (d *replicaDecoder) createTracingSpans(ctx context.Context) {
	const opName = "raft application"
	var it replicatedCmdBufSlice
	for it.init(&d.cmdBuf); it.Valid(); it.Next() {
		cmd := it.cur()

		if cmd.IsLocal() {
			// We intentionally don't propagate the client's cancellation policy (in
			// cmd.ctx) onto the request. See #75656.
			propCtx := ctx // raft scheduler's ctx
			var propSp *tracing.Span
			// If the client has a trace, put a child into propCtx.
			if sp := tracing.SpanFromContext(cmd.proposal.ctx); sp != nil {
				propCtx, propSp = sp.Tracer().StartSpanCtx(
					propCtx, "local proposal", tracing.WithParent(sp),
				)
			}
			cmd.ctx, cmd.sp = propCtx, propSp
		} else if cmd.Cmd.TraceData != nil {
			// The proposal isn't local, and trace data is available. Extract
			// the remote span and start a server-side span that follows from it.
			spanMeta, err := d.r.AmbientContext.Tracer.ExtractMetaFrom(tracing.MapCarrier{
				Map: cmd.Cmd.TraceData,
			})
			if err != nil {
				log.Errorf(ctx, "unable to extract trace data from raft command: %s", err)
			} else {
				cmd.ctx, cmd.sp = d.r.AmbientContext.Tracer.StartSpanCtx(
					ctx,
					opName,
					// NB: Nobody is collecting the recording of this span; we have no
					// mechanism for it.
					tracing.WithRemoteParentFromSpanMeta(spanMeta),
					tracing.WithFollowsFrom(),
				)
			}
		} else {
			cmd.ctx, cmd.sp = tracing.ChildSpan(ctx, opName)
		}

		if util.RaceEnabled && cmd.ctx.Done() != nil {
			panic(fmt.Sprintf("cancelable context observed during raft application: %+v", cmd))
		}
	}
}

// NewCommandIter implements the apply.Decoder interface.
func (d *replicaDecoder) NewCommandIter() apply.CommandIterator {
	it := d.cmdBuf.newIter()
	it.init(&d.cmdBuf)
	return it
}

// Reset implements the apply.Decoder interface.
func (d *replicaDecoder) Reset() {
	d.cmdBuf.clear()
}
