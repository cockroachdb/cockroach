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

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/apply"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"go.etcd.io/etcd/raft/v3/raftpb"
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
		if err := d.cmdBuf.allocate().decode(ctx, ent); err != nil {
			return err
		}
	}
	return nil
}

// retrieveLocalProposals binds each of the decoder's commands to their local
// proposals if they were proposed locally. The method also sets the ctx fields
// on all commands.
func (d *replicaDecoder) retrieveLocalProposals(ctx context.Context) (anyLocal bool) {
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
		cmd.proposal = d.r.mu.proposals[cmd.idKey]
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
			cmd.raftCmd.MaxLeaseIndex == cmd.proposal.command.MaxLeaseIndex
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
			delete(d.r.mu.proposals, cmd.idKey)
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
			cmd.ctx, cmd.sp = tracing.ForkSpan(cmd.proposal.ctx, opName)
		} else if cmd.raftCmd.TraceData != nil {
			// The proposal isn't local, and trace data is available. Extract
			// the remote span and start a server-side span that follows from it.
			spanMeta, err := d.r.AmbientContext.Tracer.ExtractMetaFrom(tracing.MapCarrier{
				Map: cmd.raftCmd.TraceData,
			})
			if err != nil {
				log.Errorf(ctx, "unable to extract trace data from raft command: %s", err)
			} else {
				cmd.ctx, cmd.sp = d.r.AmbientContext.Tracer.StartSpanCtx(
					ctx,
					opName,
					// NB: we are lying here - we are not actually going to propagate
					// the recording towards the root. That seems ok.
					tracing.WithParentAndManualCollection(spanMeta),
					tracing.WithFollowsFrom(),
				)
			}
		} else {
			cmd.ctx, cmd.sp = tracing.ForkSpan(ctx, opName)
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
