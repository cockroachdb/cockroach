// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backupccl

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/kv"
	roachpb "github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

var splitAndScatterOutputTypes = []*types.T{
	types.Bytes, // RestoreDataEntry bytes
	types.Bytes, // Span key for the range router
}

// TODO(pbardea): Document.
type splitAndScatterProcessor struct {
	flowCtx *execinfra.FlowCtx
	spec    execinfrapb.SplitAndScatterSpec
	output  execinfra.RowReceiver
}

var _ execinfra.Processor = &splitAndScatterProcessor{}

func (ssp *splitAndScatterProcessor) OutputTypes() []*types.T {
	return splitAndScatterOutputTypes
}

func newSplitAndScatterProcessor(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec execinfrapb.SplitAndScatterSpec,
	output execinfra.RowReceiver,
) (execinfra.Processor, error) {
	ssp := &splitAndScatterProcessor{
		flowCtx: flowCtx,
		spec:    spec,
		output:  output,
	}
	return ssp, nil
}

type EntryNode struct {
	entry execinfrapb.RestoreSpanEntry
	node  roachpb.NodeID
}

func (ssp *splitAndScatterProcessor) Run(ctx context.Context) {
	ctx, span := tracing.ChildSpan(ctx, "backupDataProcessor")
	defer tracing.FinishSpan(span)
	defer ssp.output.ProducerDone()

	// TODO(pbardea): We probably want to make this buffer big enough to never block.
	// TODO: This channel should also be a struct that tells us how to update the
	// output router.
	doneScatterCh := make(chan EntryNode)

	var err error
	// We don't have to worry about this go routine leaking because next we loop over progCh
	// which is closed only after the go routine returns.
	go func() {
		defer close(doneScatterCh)
		err = runSplitAndScatter(ctx, ssp.flowCtx, &ssp.spec, doneScatterCh)
	}()

	for scatteredEntry := range doneScatterCh {
		// Take a copy so that we can send the progress address to the output processor.
		entry := scatteredEntry.entry

		// Update the output router to go to the right processor.
		_ = scatteredEntry.node

		entryBytes, err := protoutil.Marshal(&entry)
		if err != nil {
			ssp.output.Push(nil, &execinfrapb.ProducerMetadata{Err: err})
			return
		}
		ssp.output.Push(sqlbase.EncDatumRow{
			sqlbase.DatumToEncDatum(types.Bytes, tree.NewDBytes(tree.DBytes(entryBytes))),
			sqlbase.DatumToEncDatum(types.Bytes, tree.NewDBytes(tree.DBytes(entry.Span.Key))),
		}, nil)
	}

	if err != nil {
		ssp.output.Push(nil, &execinfrapb.ProducerMetadata{Err: err})
		return
	}
}

func runSplitAndScatter(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	spec *execinfrapb.SplitAndScatterSpec,
	doneScatterCh chan EntryNode,
) error {

	g := ctxgroup.WithContext(ctx)
	db := flowCtx.Cfg.DB
	kr, err := storageccl.MakeKeyRewriterFromRekeys(spec.Rekeys)
	if err != nil {
		return err
	}

	importSpanChunksCh := make(chan []execinfrapb.RestoreSpanEntry)
	g.GoCtx(func(ctx context.Context) error {
		defer close(importSpanChunksCh)
		for _, importSpanChunk := range spec.Chunks {
			_, err := splitAndScatterKey(ctx, db, kr, importSpanChunk.Entries[0].Span.Key)
			if err != nil {
				return err
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case importSpanChunksCh <- importSpanChunk.Entries:
				// TODO: It might make more sense to pass pointers to chunks.
			}
		}
		return nil
	})

	// TODO(dan): This tries to cover for a bad scatter by having 2 * the number
	// of nodes in the cluster. Is it necessary?
	splitScatterWorkers := 2
	for worker := 0; worker < splitScatterWorkers; worker++ {
		g.GoCtx(func(ctx context.Context) error {
			for importSpanChunk := range importSpanChunksCh {
				for _, importSpan := range importSpanChunk {
					destination, err := splitAndScatterKey(ctx, db, kr, importSpan.Span.Key)
					if err != nil {
						return err
					}

					entryNode := EntryNode{
						entry: importSpan,
						node: destination,
					}

					select {
					case <-ctx.Done():
						return ctx.Err()
					case doneScatterCh <- entryNode:
					}
				}
			}
			return nil
		})
	}

	return g.Wait()
}

// SplitAndScatterSpan issues a split request at a given key and then scatters
// the range around the cluster. It returns the node ID of the leaseholder of
// the span after the scatter.
// TODO(pbardea): Add disclaimers about what we actually do.
func splitAndScatterKey(
	ctx context.Context,
	db *kv.DB,
	kr *storageccl.KeyRewriter,
	key roachpb.Key,
) (roachpb.NodeID, error) {
	expirationTime := db.Clock().Now().Add(time.Hour.Nanoseconds(), 0)
	newSpanKey, err := rewriteBackupSpanKey(kr, key)
	if err != nil {
		return 0, err
	}

	// TODO(dan): Really, this should be splitting the Key of
	// the _next_ entry.
	log.VEventf(ctx, 1, "presplitting new key %+v", newSpanKey)
	if err := db.AdminSplit(ctx, newSpanKey, expirationTime); err != nil {
		return 0, err
	}

	log.VEventf(ctx, 1, "scattering new key %+v", newSpanKey)
	scatterReq := &roachpb.AdminScatterRequest{
		RequestHeader: roachpb.RequestHeaderFromSpan(roachpb.Span{
			Key:    newSpanKey,
			EndKey: newSpanKey.Next(),
		}),
	}
	if _, pErr := kv.SendWrapped(ctx, db.NonTransactionalSender(), scatterReq); pErr != nil {
		// TODO(dan): Unfortunately, Scatter is still too unreliable to
		// fail the RESTORE when Scatter fails. I'm uncomfortable that
		// this could break entirely and not start failing the tests,
		// but on the bright side, it doesn't affect correctness, only
		// throughput.
		// TODO: Think more about logging here.
		log.Errorf(ctx, "failed to scatter span [%s,%s): %+v",
			newSpanKey, newSpanKey.Next(), pErr.GoError())
		return 0, nil
	}

	// TODO(pbardea): Actually compute the node ID.
	destination := roachpb.NodeID(0)
	return destination, nil
}

func init() {
	rowexec.NewSplitAndScatterProcessor = newSplitAndScatterProcessor
}
