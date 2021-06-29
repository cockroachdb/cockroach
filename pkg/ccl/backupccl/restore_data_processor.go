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

	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/kv/bulk"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	gogotypes "github.com/gogo/protobuf/types"
)

// Progress is streamed to the coordinator through metadata.
var restoreDataOutputTypes = []*types.T{}

type restoreDataProcessor struct {
	execinfra.ProcessorBase

	flowCtx *execinfra.FlowCtx
	spec    execinfrapb.RestoreDataSpec
	input   execinfra.RowSource
	output  execinfra.RowReceiver

	kr *KeyRewriter

	// phaseGroup manages the phases of the restore:
	// 1) reading entries from the input
	// 2) ingesting the data associated with those entries in the concurrent
	// restore data workers.
	phaseGroup ctxgroup.Group

	// Metas from the input are forwarded to the output of this processor.
	metaCh chan *execinfrapb.ProducerMetadata
	// progress updates are accumulated on this channel. It is populated by the
	// concurrent workers and sent down the flow by the processor.
	progCh chan RestoreProgress
}

var _ execinfra.Processor = &restoreDataProcessor{}
var _ execinfra.RowSource = &restoreDataProcessor{}

const restoreDataProcName = "restoreDataProcessor"

func newRestoreDataProcessor(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec execinfrapb.RestoreDataSpec,
	post *execinfrapb.PostProcessSpec,
	input execinfra.RowSource,
	output execinfra.RowReceiver,
) (execinfra.Processor, error) {
	rd := &restoreDataProcessor{
		flowCtx: flowCtx,
		input:   input,
		spec:    spec,
		output:  output,
		progCh:  make(chan RestoreProgress, 1),
		metaCh:  make(chan *execinfrapb.ProducerMetadata, 1),
	}

	var err error
	rd.kr, err = makeKeyRewriterFromRekeys(flowCtx.Codec(), rd.spec.Rekeys)
	if err != nil {
		return nil, err
	}

	if err := rd.Init(rd, post, restoreDataOutputTypes, flowCtx, processorID, output, nil, /* memMonitor */
		execinfra.ProcStateOpts{
			InputsToDrain: []execinfra.RowSource{input},
			TrailingMetaCallback: func() []execinfrapb.ProducerMetadata {
				rd.ConsumerClosed()
				return nil
			},
		}); err != nil {
		return nil, err
	}
	return rd, nil
}

// Start is part of the RowSource interface.
func (rd *restoreDataProcessor) Start(ctx context.Context) {
	ctx = rd.StartInternal(ctx, restoreDataProcName)
	rd.input.Start(ctx)

	rd.phaseGroup = ctxgroup.WithContext(ctx)
	entries := make(chan execinfrapb.RestoreSpanEntry, 1)
	rd.phaseGroup.GoCtx(func(ctx context.Context) error {
		defer close(entries)
		return inputReader(ctx, rd.input, entries, rd.metaCh)
	})

	rd.phaseGroup.GoCtx(func(ctx context.Context) error {
		defer close(rd.progCh)
		return rd.runRestoreWorkers(entries)
	})
}

// inputReader reads the rows from its input in a single thread and converts the
// rows into either `entries` which are passed to the restore workers or
// ProducerMetadata which is passed to `Next`.
//
// The contract of Next does not guarantee that the EncDatumRow returned by Next
// remains valid after the following call to Next. This is why the input is
// consumed on a single thread, rather than consumed by each worker.
func inputReader(
	ctx context.Context,
	input execinfra.RowSource,
	entries chan execinfrapb.RestoreSpanEntry,
	metaCh chan *execinfrapb.ProducerMetadata,
) error {
	var alloc rowenc.DatumAlloc

	for {
		// We read rows from the SplitAndScatter processor. We expect each row to
		// contain 2 columns. The first is used to route the row to this processor,
		// and the second contains the RestoreSpanEntry that we're interested in.
		row, meta := input.Next()
		if meta != nil {
			if meta.Err != nil {
				return meta.Err
			}

			select {
			case metaCh <- meta:
			case <-ctx.Done():
				return ctx.Err()
			}
			continue
		}

		if row == nil {
			// Consumed all rows.
			return nil
		}

		if len(row) != 2 {
			return errors.New("expected input rows to have exactly 2 columns")
		}
		if err := row[1].EnsureDecoded(types.Bytes, &alloc); err != nil {
			return err
		}
		datum := row[1].Datum
		entryDatumBytes, ok := datum.(*tree.DBytes)
		if !ok {
			return errors.AssertionFailedf(`unexpected datum type %T: %+v`, datum, row)
		}

		var entry execinfrapb.RestoreSpanEntry
		if err := protoutil.Unmarshal([]byte(*entryDatumBytes), &entry); err != nil {
			return errors.Wrap(err, "un-marshaling restore span entry")
		}

		select {
		case entries <- entry:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (rd *restoreDataProcessor) runRestoreWorkers(entries chan execinfrapb.RestoreSpanEntry) error {
	ctx := rd.Ctx
	doneCh := ctx.Done()
	for {
		done, err := func() (done bool, _ error) {
			entry, ok := <-entries
			if !ok {
				done = true
				return done, nil
			}

			summary, err := rd.processRestoreSpanEntry(entry)
			if err != nil {
				return done, err
			}

			select {
			case rd.progCh <- makeProgressUpdate(summary, entry, rd.spec.PKIDs):
			case <-doneCh:
				return done, ctx.Err()
			}

			return done, nil
		}()

		if err != nil {
			return err
		}

		if done {
			return nil
		}
	}
}

func (rd *restoreDataProcessor) processRestoreSpanEntry(
	entry execinfrapb.RestoreSpanEntry,
) (roachpb.BulkOpSummary, error) {
	db := rd.flowCtx.Cfg.DB
	ctx := rd.Ctx
	evalCtx := rd.EvalCtx
	var summary roachpb.BulkOpSummary

	// The sstables only contain MVCC data and no intents, so using an MVCC
	// iterator is sufficient.
	var iters []storage.SimpleMVCCIterator

	log.VEventf(rd.Ctx, 1 /* level */, "ingesting span [%s-%s)", entry.Span.Key, entry.Span.EndKey)

	for _, file := range entry.Files {
		log.VEventf(ctx, 2, "import file %s which starts at %s", file.Path, entry.Span.Key)

		dir, err := rd.flowCtx.Cfg.ExternalStorage(ctx, file.Dir)
		if err != nil {
			return summary, err
		}
		defer func() {
			if err := dir.Close(); err != nil {
				log.Warningf(ctx, "close export storage failed %v", err)
			}
		}()
		iter, err := storageccl.ExternalSSTReader(ctx, dir, file.Path, rd.spec.Encryption)
		if err != nil {
			return summary, err
		}
		defer iter.Close()
		iters = append(iters, iter)
	}

	batcher, err := bulk.MakeSSTBatcher(ctx, db, evalCtx.Settings,
		func() int64 { return storageccl.MaxIngestBatchSize(evalCtx.Settings) })
	if err != nil {
		return summary, err
	}
	defer batcher.Close()

	startKeyMVCC, endKeyMVCC := storage.MVCCKey{Key: entry.Span.Key},
		storage.MVCCKey{Key: entry.Span.EndKey}
	iter := storage.MakeMultiIterator(iters)
	defer iter.Close()
	var keyScratch, valueScratch []byte

	for iter.SeekGE(startKeyMVCC); ; {
		ok, err := iter.Valid()
		if err != nil {
			return summary, err
		}
		if !ok {
			break
		}

		if !rd.spec.RestoreTime.IsEmpty() {
			// TODO(dan): If we have to skip past a lot of versions to find the
			// latest one before args.EndTime, then this could be slow.
			if rd.spec.RestoreTime.Less(iter.UnsafeKey().Timestamp) {
				iter.Next()
				continue
			}
		}

		if !ok || !iter.UnsafeKey().Less(endKeyMVCC) {
			break
		}
		if len(iter.UnsafeValue()) == 0 {
			// Value is deleted.
			iter.NextKey()
			continue
		}

		keyScratch = append(keyScratch[:0], iter.UnsafeKey().Key...)
		valueScratch = append(valueScratch[:0], iter.UnsafeValue()...)
		key := storage.MVCCKey{Key: keyScratch, Timestamp: iter.UnsafeKey().Timestamp}
		value := roachpb.Value{RawBytes: valueScratch}
		iter.NextKey()

		key.Key, ok, err = rd.kr.RewriteKey(key.Key, false /* isFromSpan */)
		if err != nil {
			return summary, err
		}
		if !ok {
			// If the key rewriter didn't match this key, it's not data for the
			// table(s) we're interested in.
			if log.V(5) {
				log.Infof(ctx, "skipping %s %s", key.Key, value.PrettyPrint())
			}
			continue
		}

		// Rewriting the key means the checksum needs to be updated.
		value.ClearChecksum()
		value.InitChecksum(key.Key)

		if log.V(5) {
			log.Infof(ctx, "Put %s -> %s", key.Key, value.PrettyPrint())
		}
		if err := batcher.AddMVCCKey(ctx, key, value.RawBytes); err != nil {
			return summary, errors.Wrapf(err, "adding to batch: %s -> %s", key, value.PrettyPrint())
		}
	}
	// Flush out the last batch.
	if err := batcher.Flush(ctx); err != nil {
		return summary, err
	}

	if restoreKnobs, ok := rd.flowCtx.TestingKnobs().BackupRestoreTestingKnobs.(*sql.BackupRestoreTestingKnobs); ok {
		if restoreKnobs.RunAfterProcessingRestoreSpanEntry != nil {
			restoreKnobs.RunAfterProcessingRestoreSpanEntry(ctx)
		}
	}

	return batcher.GetSummary(), nil
}

func makeProgressUpdate(
	summary roachpb.BulkOpSummary, entry execinfrapb.RestoreSpanEntry, pkIDs map[uint64]bool,
) (progDetails RestoreProgress) {
	progDetails.Summary = countRows(summary, pkIDs)
	progDetails.ProgressIdx = entry.ProgressIdx
	progDetails.DataSpan = entry.Span
	return
}

// Next is part of the RowSource interface.
func (rd *restoreDataProcessor) Next() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
	if rd.State != execinfra.StateRunning {
		return nil, rd.DrainHelper()
	}

	var prog execinfrapb.RemoteProducerMetadata_BulkProcessorProgress

	select {
	case progDetails, ok := <-rd.progCh:
		if !ok {
			// Done. Check if any phase exited early with an error.
			err := rd.phaseGroup.Wait()
			rd.MoveToDraining(err)
			return nil, rd.DrainHelper()
		}

		details, err := gogotypes.MarshalAny(&progDetails)
		if err != nil {
			rd.MoveToDraining(err)
			return nil, rd.DrainHelper()
		}
		prog.ProgressDetails = *details
	case meta := <-rd.metaCh:
		return nil, meta
	case <-rd.Ctx.Done():
		rd.MoveToDraining(rd.Ctx.Err())
		return nil, rd.DrainHelper()
	}

	return nil, &execinfrapb.ProducerMetadata{BulkProcessorProgress: &prog}
}

// ConsumerClosed is part of the RowSource interface.
func (rd *restoreDataProcessor) ConsumerClosed() {
	if rd.InternalClose() {
		if rd.metaCh != nil {
			close(rd.metaCh)
		}
	}
}

func init() {
	rowexec.NewRestoreDataProcessor = newRestoreDataProcessor
}
