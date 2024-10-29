// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/apply"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logcrash"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
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
	anyLocal := d.retrieveLocalProposals()
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

// retrieveLocalProposals removes all proposals which have a log entry pending
// immediate application from the proposals map. The entries which are paired up
// with a proposal in that way are considered "local", meaning a client is
// waiting on their result, and may be reproposed (as a new proposal) with a new
// lease index in case they apply with an illegal lease index (see
// tryReproposeWithNewLeaseIndexRaftMuLocked).
func (d *replicaDecoder) retrieveLocalProposals() (anyLocal bool) {
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
			if sp := tracing.SpanFromContext(cmd.proposal.Context()); sp != nil {
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
