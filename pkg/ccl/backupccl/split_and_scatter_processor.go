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
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/rowflow"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

var splitAndScatterOutputTypes = []*types.T{
	types.Bytes, // Span key for the range router
	types.Bytes, // RestoreDataEntry bytes
}

type splitAndScatterer interface {
	// splitAndScatterSpan issues a split request at a given key and then scatters
	// the range around the cluster. It returns the node ID of the leaseholder of
	// the span after the scatter.
	splitAndScatterKey(ctx context.Context, db *kv.DB, kr *storageccl.KeyRewriter, key roachpb.Key) (roachpb.NodeID, error)
}

// dbSplitAndScatter is the production implementation of this processor's
// scatterer. It actually issues the split and scatter requests for KV. This is
// mocked out in some tests.
type dbSplitAndScatterer struct{}

// splitAndScatterKey implements the splitAndScatterer interface.
func (s *dbSplitAndScatterer) splitAndScatterKey(
	ctx context.Context, db *kv.DB, kr *storageccl.KeyRewriter, key roachpb.Key,
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
		log.Errorf(ctx, "failed to scatter span [%s,%s): %+v",
			newSpanKey, newSpanKey.Next(), pErr.GoError())
		return 0, nil
	}

	// TODO(pbardea): Actually compute the node ID when it is returned from the
	// adminScatter request.
	destination := roachpb.NodeID(0)
	return destination, nil
}

// splitAndScatterProcessor is given a set of spans (specified as
// RestoreSpanEntry's) to distribute across the cluster. Depending on which node
// the span ends up on, it forwards RestoreSpanEntry as bytes along with the key
// of the span on a row. It expects an output RangeRouter and before it emits
// each row, it updates the entry in the RangeRouter's map with the destination
// of the scatter.
type splitAndScatterProcessor struct {
	flowCtx   *execinfra.FlowCtx
	spec      execinfrapb.SplitAndScatterSpec
	output    execinfra.RowReceiver
	scatterer splitAndScatterer
}

var _ execinfra.Processor = &splitAndScatterProcessor{}

func (ssp *splitAndScatterProcessor) OutputTypes() []*types.T {
	return splitAndScatterOutputTypes
}

func newSplitAndScatterProcessor(
	flowCtx *execinfra.FlowCtx,
	_ int32,
	spec execinfrapb.SplitAndScatterSpec,
	output execinfra.RowReceiver,
) (execinfra.Processor, error) {
	ssp := &splitAndScatterProcessor{
		flowCtx:   flowCtx,
		spec:      spec,
		output:    output,
		scatterer: &dbSplitAndScatterer{},
	}
	return ssp, nil
}

type EntryNode struct {
	entry execinfrapb.RestoreSpanEntry
	node  roachpb.NodeID
}

func (ssp *splitAndScatterProcessor) Run(ctx context.Context) {
	ctx, span := tracing.ChildSpan(ctx, "splitAndScatterProcessor")
	defer tracing.FinishSpan(span)
	defer ssp.output.ProducerDone()

	router, ok := ssp.output.(rowflow.ControlledRouter)
	if !ok {
		// TODO: Also consider panicing here.
		err := errors.New("expected processor output to be a ControlledRouter (e.g. range router)")
		ssp.output.Push(nil, &execinfrapb.ProducerMetadata{Err: err})
		return
	}

	numEntries := 0
	for _, chunk := range ssp.spec.Chunks {
		numEntries += len(chunk.Entries)
	}
	// Large enough so that it never blocks.
	doneScatterCh := make(chan EntryNode, numEntries)

	var err error
	splitAndScatterCtx, cancelSplitAndScatter := context.WithCancel(ctx)
	defer cancelSplitAndScatter()
	// Note that the loop over doneScatterCh should prevent this goroutine from
	// leaking when there are no errors. However, if that loop needs to exit
	// early, runSplitAndScatter's context will be cancelled.
	go func() {
		defer close(doneScatterCh)
		err = runSplitAndScatter(splitAndScatterCtx, ssp.flowCtx, &ssp.spec, &ssp.scatterer, doneScatterCh)
		if err != nil {
			log.Errorf(ctx, "error while running split and scatter: %+v", err)
		}
	}()

	for scatteredEntry := range doneScatterCh {
		entry := scatteredEntry.entry
		entryBytes, err := protoutil.Marshal(&entry)
		if err != nil {
			// If we get an error Marshalling the entry, there doesn't seem to be much
			// point sending this error through the stream. Something bad has
			// happened.
			// TODO: Consider if we just want to panic here.
			ssp.output.Push(nil, &execinfrapb.ProducerMetadata{Err: err})
			break
		}

		row := sqlbase.EncDatumRow{
			sqlbase.DatumToEncDatum(types.Bytes, tree.NewDBytes(tree.DBytes(entry.Span.Key))),
			sqlbase.DatumToEncDatum(types.Bytes, tree.NewDBytes(tree.DBytes(entryBytes))),
		}
		if err := updateRouter(router, row, scatteredEntry, ssp.spec.NodeToStream); err != nil {
			ssp.output.Push(nil, &execinfrapb.ProducerMetadata{Err: err})
			break
		}

		ssp.output.Push(row, nil)
	}

	if err != nil {
		ssp.output.Push(nil, &execinfrapb.ProducerMetadata{Err: err})
		return
	}
}

func updateRouter(
	router rowflow.ControlledRouter,
	row sqlbase.EncDatumRow,
	scatteredEntry EntryNode,
	nodeToStream map[roachpb.NodeID]int32,
) error {
	endDatum := sqlbase.DatumToEncDatum(types.Bytes, tree.NewDBytes(tree.DBytes(scatteredEntry.entry.Span.EndKey)))

	var alloc sqlbase.DatumAlloc
	startBytes, endBytes := make([]byte, 0), make([]byte, 0)
	startBytes, err := row[0].Encode(splitAndScatterOutputTypes[0], &alloc, sqlbase.DatumEncoding_ASCENDING_KEY, startBytes)
	if err != nil {
		return err
	}
	endBytes, err = endDatum.Encode(splitAndScatterOutputTypes[0], &alloc, sqlbase.DatumEncoding_ASCENDING_KEY, endBytes)
	if err != nil {
		return err
	}

	span := execinfrapb.OutputRouterSpec_RangeRouterSpec_Span{
		Start:  startBytes,
		End:    endBytes,
		Stream: nodeToStream[scatteredEntry.node],
	}

	router.Direct(span)
	return nil
}

func runSplitAndScatter(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	spec *execinfrapb.SplitAndScatterSpec,
	scatterer *splitAndScatterer,
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
			_, err := (*scatterer).splitAndScatterKey(ctx, db, kr, importSpanChunk.Entries[0].Span.Key)
			if err != nil {
				return err
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case importSpanChunksCh <- importSpanChunk.Entries:
			}
		}
		return nil
	})

	// TODO(dan): This tries to cover for a bad scatter by having 2 * the number
	// of nodes in the cluster. Is it necessary?
	// TODO(pbardea): Run some experiments to tune this knob.
	splitScatterWorkers := 2
	for worker := 0; worker < splitScatterWorkers; worker++ {
		g.GoCtx(func(ctx context.Context) error {
			for importSpanChunk := range importSpanChunksCh {
				log.Infof(ctx, "processing a chunk")
				for _, importSpan := range importSpanChunk {
					log.Infof(ctx, "processing a span")
					destination, err := (*scatterer).splitAndScatterKey(ctx, db, kr, importSpan.Span.Key)
					if err != nil {
						return err
					}

					entryNode := EntryNode{
						entry: importSpan,
						node:  destination,
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

func init() {
	rowexec.NewSplitAndScatterProcessor = newSplitAndScatterProcessor
}
