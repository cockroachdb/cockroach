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
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
)

type splitAndScatterer interface {
	// split issues a split request at the given key, which may be rewritten to
	// the RESTORE keyspace.
	split(ctx context.Context, codec keys.SQLCodec, splitKey roachpb.Key) error
	// scatter issues a scatter request at the given key. It returns the node ID
	// of where the range was scattered to.
	scatter(ctx context.Context, codec keys.SQLCodec, scatterKey roachpb.Key) (roachpb.NodeID, error)
}

type noopSplitAndScatterer struct{}

var _ splitAndScatterer = noopSplitAndScatterer{}

// split implements splitAndScatterer.
func (n noopSplitAndScatterer) split(_ context.Context, _ keys.SQLCodec, _ roachpb.Key) error {
	return nil
}

// scatter implements splitAndScatterer.
func (n noopSplitAndScatterer) scatter(
	_ context.Context, _ keys.SQLCodec, _ roachpb.Key,
) (roachpb.NodeID, error) {
	return 0, nil
}

// dbSplitAndScatter is the production implementation of this processor's
// scatterer. It actually issues the split and scatter requests against the KV
// layer.
type dbSplitAndScatterer struct {
	db *kv.DB
	kr *KeyRewriter
}

var _ splitAndScatterer = dbSplitAndScatterer{}

func makeSplitAndScatterer(db *kv.DB, kr *KeyRewriter) splitAndScatterer {
	return dbSplitAndScatterer{db: db, kr: kr}
}

// split implements splitAndScatterer.
func (s dbSplitAndScatterer) split(
	ctx context.Context, codec keys.SQLCodec, splitKey roachpb.Key,
) error {
	if s.kr == nil {
		return errors.AssertionFailedf("KeyRewriter was not set when expected to be")
	}
	if s.db == nil {
		return errors.AssertionFailedf("split and scatterer's database was not set when expected")
	}

	expirationTime := s.db.Clock().Now().Add(time.Hour.Nanoseconds(), 0)
	newSplitKey, err := rewriteBackupSpanKey(codec, s.kr, splitKey)
	if err != nil {
		return err
	}
	if splitAt, err := keys.EnsureSafeSplitKey(newSplitKey); err != nil {
		// Ignore the error, not all keys are table keys.
	} else if len(splitAt) != 0 {
		newSplitKey = splitAt
	}
	log.VEventf(ctx, 1, "presplitting new key %+v", newSplitKey)
	if err := s.db.AdminSplit(ctx, newSplitKey, expirationTime); err != nil {
		return errors.Wrapf(err, "splitting key %s", newSplitKey)
	}

	return nil
}

// scatter implements splitAndScatterer.
func (s dbSplitAndScatterer) scatter(
	ctx context.Context, codec keys.SQLCodec, scatterKey roachpb.Key,
) (roachpb.NodeID, error) {
	if s.kr == nil {
		return 0, errors.AssertionFailedf("KeyRewriter was not set when expected to be")
	}
	if s.db == nil {
		return 0, errors.AssertionFailedf("split and scatterer's database was not set when expected")
	}

	newScatterKey, err := rewriteBackupSpanKey(codec, s.kr, scatterKey)
	if err != nil {
		return 0, err
	}
	if scatterAt, err := keys.EnsureSafeSplitKey(newScatterKey); err != nil {
		// Ignore the error, not all keys are table keys.
	} else if len(scatterAt) != 0 {
		newScatterKey = scatterAt
	}

	log.VEventf(ctx, 1, "scattering new key %+v", newScatterKey)
	req := &roachpb.AdminScatterRequest{
		RequestHeader: roachpb.RequestHeaderFromSpan(roachpb.Span{
			Key:    newScatterKey,
			EndKey: newScatterKey.Next(),
		}),
		// This is a bit of a hack, but it seems to be an effective one (see #36665
		// for graphs). As of the commit that added this, scatter is not very good
		// at actually balancing leases. This is likely for two reasons: 1) there's
		// almost certainly some regression in scatter's behavior, it used to work
		// much better and 2) scatter has to operate by balancing leases for all
		// ranges in a cluster, but in RESTORE, we really just want it to be
		// balancing the span being restored into.
		RandomizeLeases: true,
	}

	res, pErr := kv.SendWrapped(ctx, s.db.NonTransactionalSender(), req)
	if pErr != nil {
		// TODO(pbardea): Unfortunately, Scatter is still too unreliable to
		// fail the RESTORE when Scatter fails. I'm uncomfortable that
		// this could break entirely and not start failing the tests,
		// but on the bright side, it doesn't affect correctness, only
		// throughput.
		log.Errorf(ctx, "failed to scatter span [%s,%s): %+v",
			newScatterKey, newScatterKey.Next(), pErr.GoError())
		return 0, nil
	}

	return s.findDestination(res.(*roachpb.AdminScatterResponse)), nil
}

// findDestination returns the node ID of the node of the destination of the
// AdminScatter request. If the destination cannot be found, 0 is returned.
func (s dbSplitAndScatterer) findDestination(res *roachpb.AdminScatterResponse) roachpb.NodeID {
	// A request from a 20.1 node will not have a RangeInfos with a lease.
	// For this mixed-version state, we'll report the destination as node 0
	// and suffer a bit of inefficiency.
	if len(res.RangeInfos) > 0 {
		// If the lease is not populated, we return the 0 value anyway. We receive 1
		// RangeInfo per range that was scattered. Since we send a scatter request
		// to each range that we make, we are only interested in the first range,
		// which contains the key at which we're splitting and scattering.
		return res.RangeInfos[0].Lease.Replica.NodeID
	}

	return roachpb.NodeID(0)
}

const splitAndScatterProcessorName = "splitAndScatter"

var splitAndScatterOutputTypes = []*types.T{
	types.Bytes, // Span key for the range router
	types.Bytes, // RestoreDataEntry bytes
}

// splitAndScatterProcessor is given a set of spans (specified as
// RestoreSpanEntry's) to distribute across the cluster. Depending on which node
// the span ends up on, it forwards RestoreSpanEntry as bytes along with the key
// of the span on a row. It expects an output RangeRouter and before it emits
// each row, it updates the entry in the RangeRouter's map with the destination
// of the scatter.
type splitAndScatterProcessor struct {
	execinfra.ProcessorBase

	flowCtx *execinfra.FlowCtx
	spec    execinfrapb.SplitAndScatterSpec
	output  execinfra.RowReceiver

	scatterer splitAndScatterer
	// cancelScatterAndWaitForWorker cancels the scatter goroutine and waits for
	// it to finish.
	cancelScatterAndWaitForWorker func()

	doneScatterCh chan entryNode
	// A cache for routing datums, so only 1 is allocated per node.
	routingDatumCache map[roachpb.NodeID]rowenc.EncDatum
	scatterErr        error
}

var _ execinfra.Processor = &splitAndScatterProcessor{}

func newSplitAndScatterProcessor(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec execinfrapb.SplitAndScatterSpec,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
) (execinfra.Processor, error) {
	numEntries := 0
	for _, chunk := range spec.Chunks {
		numEntries += len(chunk.Entries)
	}

	db := flowCtx.Cfg.DB
	kr, err := makeKeyRewriterFromRekeys(flowCtx.Codec(), spec.TableRekeys, spec.TenantRekeys)
	if err != nil {
		return nil, err
	}

	scatterer := makeSplitAndScatterer(db, kr)
	ssp := &splitAndScatterProcessor{
		flowCtx:   flowCtx,
		spec:      spec,
		output:    output,
		scatterer: scatterer,
		// Large enough so that it never blocks.
		doneScatterCh:     make(chan entryNode, numEntries),
		routingDatumCache: make(map[roachpb.NodeID]rowenc.EncDatum),
	}
	if err := ssp.Init(ssp, post, splitAndScatterOutputTypes, flowCtx, processorID, output, nil, /* memMonitor */
		execinfra.ProcStateOpts{
			InputsToDrain: nil, // there are no inputs to drain
			TrailingMetaCallback: func() []execinfrapb.ProducerMetadata {
				ssp.close()
				return nil
			},
		}); err != nil {
		return nil, err
	}
	return ssp, nil
}

// Start is part of the RowSource interface.
func (ssp *splitAndScatterProcessor) Start(ctx context.Context) {
	ctx = logtags.AddTag(ctx, "job", ssp.spec.JobID)
	ctx = ssp.StartInternal(ctx, splitAndScatterProcessorName)
	// Note that the loop over doneScatterCh in Next should prevent the goroutine
	// below from leaking when there are no errors. However, if that loop needs to
	// exit early, runSplitAndScatter's context will be canceled.
	scatterCtx, cancel := context.WithCancel(ctx)
	workerDone := make(chan struct{})
	ssp.cancelScatterAndWaitForWorker = func() {
		cancel()
		<-workerDone
	}
	if err := ssp.flowCtx.Stopper().RunAsyncTaskEx(scatterCtx, stop.TaskOpts{
		TaskName: "splitAndScatter-worker",
		SpanOpt:  stop.ChildSpan,
	}, func(ctx context.Context) {
		ssp.scatterErr = runSplitAndScatter(scatterCtx, ssp.flowCtx, &ssp.spec, ssp.scatterer, ssp.doneScatterCh)
		cancel()
		close(ssp.doneScatterCh)
		close(workerDone)
	}); err != nil {
		ssp.scatterErr = err
		cancel()
		close(workerDone)
	}
}

type entryNode struct {
	entry execinfrapb.RestoreSpanEntry
	node  roachpb.NodeID
}

// Next implements the execinfra.RowSource interface.
func (ssp *splitAndScatterProcessor) Next() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
	if ssp.State != execinfra.StateRunning {
		return nil, ssp.DrainHelper()
	}

	scatteredEntry, ok := <-ssp.doneScatterCh
	if ok {
		entry := scatteredEntry.entry
		entryBytes, err := protoutil.Marshal(&entry)
		if err != nil {
			ssp.MoveToDraining(err)
			return nil, ssp.DrainHelper()
		}

		// The routing datums informs the router which output stream should be used.
		routingDatum, ok := ssp.routingDatumCache[scatteredEntry.node]
		if !ok {
			routingDatum, _ = routingDatumsForSQLInstance(base.SQLInstanceID(scatteredEntry.node))
			ssp.routingDatumCache[scatteredEntry.node] = routingDatum
		}

		row := rowenc.EncDatumRow{
			routingDatum,
			rowenc.DatumToEncDatum(types.Bytes, tree.NewDBytes(tree.DBytes(entryBytes))),
		}
		return row, nil
	}

	if ssp.scatterErr != nil {
		ssp.MoveToDraining(ssp.scatterErr)
		return nil, ssp.DrainHelper()
	}

	ssp.MoveToDraining(nil /* error */)
	return nil, ssp.DrainHelper()
}

// ConsumerClosed is part of the RowSource interface.
func (ssp *splitAndScatterProcessor) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	ssp.close()
}

// close stops the production workers. This needs to be called if the consumer
// runs into an error and stops consuming scattered entries to make sure we
// don't leak goroutines.
func (ssp *splitAndScatterProcessor) close() {
	ssp.cancelScatterAndWaitForWorker()
	ssp.InternalClose()
}

// scatteredChunk is the entries of a chunk of entries to process along with the
// node the chunk was scattered to.
type scatteredChunk struct {
	destination roachpb.NodeID
	entries     []execinfrapb.RestoreSpanEntry
}

func runSplitAndScatter(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	spec *execinfrapb.SplitAndScatterSpec,
	scatterer splitAndScatterer,
	doneScatterCh chan<- entryNode,
) error {
	g := ctxgroup.WithContext(ctx)

	importSpanChunksCh := make(chan scatteredChunk)
	g.GoCtx(func(ctx context.Context) error {
		// Chunks' leaseholders should be randomly placed throughout the
		// cluster.
		defer close(importSpanChunksCh)
		for i, importSpanChunk := range spec.Chunks {
			scatterKey := importSpanChunk.Entries[0].Span.Key
			if i+1 < len(spec.Chunks) {
				// Split at the start of the next chunk, to partition off a
				// prefix of the space to scatter.
				splitKey := spec.Chunks[i+1].Entries[0].Span.Key
				if err := scatterer.split(ctx, flowCtx.Codec(), splitKey); err != nil {
					return err
				}
			}
			chunkDestination, err := scatterer.scatter(ctx, flowCtx.Codec(), scatterKey)
			if err != nil {
				return err
			}

			sc := scatteredChunk{
				destination: chunkDestination,
				entries:     importSpanChunk.Entries,
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case importSpanChunksCh <- sc:
			}
		}
		return nil
	})

	// TODO(pbardea): This tries to cover for a bad scatter by having 2 * the
	// number of nodes in the cluster. Is it necessary?
	splitScatterWorkers := 2
	for worker := 0; worker < splitScatterWorkers; worker++ {
		g.GoCtx(func(ctx context.Context) error {
			for importSpanChunk := range importSpanChunksCh {
				chunkDestination := importSpanChunk.destination
				for i, importEntry := range importSpanChunk.entries {
					nextChunkIdx := i + 1

					log.VInfof(ctx, 2, "processing a span [%s,%s)", importEntry.Span.Key, importEntry.Span.EndKey)
					var splitKey roachpb.Key
					if nextChunkIdx < len(importSpanChunk.entries) {
						// Split at the next entry.
						splitKey = importSpanChunk.entries[nextChunkIdx].Span.Key
						if err := scatterer.split(ctx, flowCtx.Codec(), splitKey); err != nil {
							return err
						}
					}

					scatteredEntry := entryNode{
						entry: importEntry,
						node:  chunkDestination,
					}

					select {
					case <-ctx.Done():
						return ctx.Err()
					case doneScatterCh <- scatteredEntry:
					}
				}
			}
			return nil
		})
	}

	return g.Wait()
}

func routingDatumsForSQLInstance(
	sqlInstanceID base.SQLInstanceID,
) (rowenc.EncDatum, rowenc.EncDatum) {
	routingBytes := roachpb.Key(fmt.Sprintf("node%d", sqlInstanceID))
	startDatum := rowenc.DatumToEncDatum(types.Bytes, tree.NewDBytes(tree.DBytes(routingBytes)))
	endDatum := rowenc.DatumToEncDatum(types.Bytes, tree.NewDBytes(tree.DBytes(routingBytes.Next())))
	return startDatum, endDatum
}

// routingSpanForSQLInstance provides the mapping to be used during distsql planning
// when setting up the output router.
func routingSpanForSQLInstance(sqlInstanceID base.SQLInstanceID) ([]byte, []byte, error) {
	var alloc tree.DatumAlloc
	startDatum, endDatum := routingDatumsForSQLInstance(sqlInstanceID)

	startBytes, endBytes := make([]byte, 0), make([]byte, 0)
	startBytes, err := startDatum.Encode(splitAndScatterOutputTypes[0], &alloc, descpb.DatumEncoding_ASCENDING_KEY, startBytes)
	if err != nil {
		return nil, nil, err
	}
	endBytes, err = endDatum.Encode(splitAndScatterOutputTypes[0], &alloc, descpb.DatumEncoding_ASCENDING_KEY, endBytes)
	if err != nil {
		return nil, nil, err
	}
	return startBytes, endBytes, nil
}

func init() {
	rowexec.NewSplitAndScatterProcessor = newSplitAndScatterProcessor
}
