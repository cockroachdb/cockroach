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
	"io/ioutil"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/kv/bulk"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/errors"
	gogotypes "github.com/gogo/protobuf/types"
)

// Progress is streamed to the coordinator through metadata.
var restoreDataOutputTypes = []*types.T{}

var importLeadMultiplier = settings.RegisterIntSetting(
	"bulkio.import_lead_multiplier",
	"a description",
	2,
)

type restoreDataProcessor struct {
	execinfra.ProcessorBase

	flowCtx *execinfra.FlowCtx
	spec    execinfrapb.RestoreDataSpec
	input   execinfra.RowSource
	output  execinfra.RowReceiver

	progCh     chan *RestoreProgress
	restoreErr error

	alloc rowenc.DatumAlloc
	kr    *storageccl.KeyRewriter

	totalNext map[int]time.Duration

	totalIngest map[int]time.Duration

	totalProgSend map[int]time.Duration
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
	}

	var err error
	rd.kr, err = storageccl.MakeKeyRewriterFromRekeys(rd.spec.Rekeys)
	if err != nil {
		return nil, err
	}

	if err := rd.Init(rd, post, restoreDataOutputTypes, flowCtx, processorID, output, nil, /* memMonitor */
		execinfra.ProcStateOpts{
			InputsToDrain: []execinfra.RowSource{input},
		}); err != nil {
		return nil, err
	}
	mult := int(importLeadMultiplier.Get(&rd.EvalCtx.Settings.SV))
	numWorkers := int(kvserver.ImportRequestsLimit.Get(&rd.EvalCtx.Settings.SV)) * mult
	rd.progCh = make(chan *RestoreProgress, numWorkers)
	return rd, nil
}

// Start is part of the RowSource interface.
func (rd *restoreDataProcessor) Start(ctx context.Context) context.Context {
	rd.input.Start(ctx)
	ctx = rd.StartInternal(ctx, restoreDataProcName)
	go func() {
		defer close(rd.progCh)
		rd.restoreErr = rd.runRestore()
	}()
	return ctx
}

// Next is part of the RowSource interface.
func (rd *restoreDataProcessor) Next() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
	if rd.State != execinfra.StateRunning {
		return nil, rd.DrainHelper()
	}

	for progDetails := range rd.progCh {
		var prog execinfrapb.RemoteProducerMetadata_BulkProcessorProgress
		details, err := gogotypes.MarshalAny(progDetails)
		if err != nil {
			rd.MoveToDraining(err)
			return nil, rd.DrainHelper()
		}
		prog.ProgressDetails = *details
		return nil, &execinfrapb.ProducerMetadata{BulkProcessorProgress: &prog}
	}

	if rd.restoreErr != nil {
		rd.MoveToDraining(rd.restoreErr)
		return nil, rd.DrainHelper()
	}

	rd.MoveToDraining(nil /* err */)
	return nil, rd.DrainHelper()
}

// ConsumerClosed is part of the RowSource interface.
func (rd *restoreDataProcessor) ConsumerClosed() {
	rd.InternalClose()
}

func (rd *restoreDataProcessor) runRestore() error {
	mult := int(importLeadMultiplier.Get(&rd.EvalCtx.Settings.SV))
	numWorkers := int(kvserver.ImportRequestsLimit.Get(&rd.EvalCtx.Settings.SV)) * mult
	return ctxgroup.GroupWorkers(rd.Ctx, numWorkers, func(ctx context.Context, workerID int) error {
		for {
			// We read rows from the SplitAndScatter processor. We expect each row to
			// contain 2 columns. The first is used to route the row to this processor,
			// and the second contains the RestoreSpanEntry that we're interested in.
			// beforeNext := timeutil.Now()
			row, meta := rd.input.Next()
			// rd.totalNext[workerID] += timeutil.Since(beforeNext)
			if meta != nil {
				if meta.Err != nil {
					// We got an error.
					return meta.Err
				}

				// TODO: This currently ignores all metadata.
				// TODO: Should this be fwding metadata?
			}
			if row == nil {
				// Done consuming rows.
				return nil
			}

			if len(row) != 2 {
				return errors.New("expected input rows to have exactly 2 columns")
			}
			if err := row[1].EnsureDecoded(types.Bytes, &rd.alloc); err != nil {
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

			newSpanKey, err := rewriteBackupSpanKey(rd.flowCtx.Codec(), rd.kr, entry.Span.Key)
			if err != nil {
				return errors.Wrap(err, "re-writing span key to import")
			}

			// beforeImport := timeutil.Now()
			log.VEventf(rd.Ctx, 1 /* level */, "importing span %v", newSpanKey)
			summary, err := rd.processRestoreSpanEntry(entry, entry.Span.Key)
			if err != nil {
				return err
			}
			// rd.totalIngest[workerID] += timeutil.Since(beforeImport)

			progDetails := &RestoreProgress{}
			progDetails.Summary = countRows(summary, rd.spec.PKIDs)
			progDetails.ProgressIdx = entry.ProgressIdx
			progDetails.DataSpan = entry.Span

			//beforeProgSend := timeutil.Now()
			rd.progCh <- progDetails
			// rd.totalProgSend[workerID] += timeutil.Since(beforeProgSend)

			log.Infof(ctx, "restore timing: spent %+v on waiting for next row, %+v on processing the import and %+v on waiting for the progCh for worker %d", rd.totalNext[workerID], rd.totalIngest[workerID], rd.totalProgSend[workerID], workerID)
		}
	})
}

func (rd *restoreDataProcessor) processRestoreSpanEntry(
	entry execinfrapb.RestoreSpanEntry, newSpanKey roachpb.Key,
) (roachpb.BulkOpSummary, error) {
	db := rd.flowCtx.Cfg.DB
	ctx := rd.Ctx
	evalCtx := rd.EvalCtx
	var summary roachpb.BulkOpSummary

	// The sstables only contain MVCC data and no intents, so using an MVCC
	// iterator is sufficient.
	var iters []storage.SimpleMVCCIterator
	for _, file := range entry.Files {
		log.VEventf(ctx, 2, "import file %s %s", file.Path, newSpanKey)

		dir, err := rd.flowCtx.Cfg.ExternalStorage(ctx, file.Dir)
		if err != nil {
			return summary, err
		}
		defer func() {
			if err := dir.Close(); err != nil {
				log.Warningf(ctx, "close export storage failed %v", err)
			}
		}()

		const maxAttempts = 3
		var fileContents []byte
		if err := retry.WithMaxAttempts(ctx, base.DefaultRetryOptions(), maxAttempts, func() error {
			f, err := dir.ReadFile(ctx, file.Path)
			if err != nil {
				return err
			}
			defer f.Close()
			fileContents, err = ioutil.ReadAll(f)
			return err
		}); err != nil {
			return summary, errors.Wrapf(err, "fetching %q", file.Path)
		}
		dataSize := int64(len(fileContents))
		log.Eventf(ctx, "fetched file (%s)", humanizeutil.IBytes(dataSize))

		if rd.spec.Encryption != nil {
			fileContents, err = storageccl.DecryptFile(fileContents, rd.spec.Encryption.Key)
			if err != nil {
				return summary, err
			}
		}

		iter, err := storage.NewMemSSTIterator(fileContents, false)
		if err != nil {
			return summary, err
		}

		defer iter.Close()
		iters = append(iters, iter)
	}

	batcher, err := bulk.MakeSSTBatcher(ctx, db, evalCtx.Settings,
		func() int64 { return storageccl.MaxImportBatchSize(evalCtx.Settings) })
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
			if log.V(3) {
				log.Infof(ctx, "skipping %s %s", key.Key, value.PrettyPrint())
			}
			continue
		}

		// Rewriting the key means the checksum needs to be updated.
		value.ClearChecksum()
		value.InitChecksum(key.Key)

		if log.V(3) {
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
	log.Event(ctx, "done")

	if restoreKnobs, ok := rd.flowCtx.TestingKnobs().BackupRestoreTestingKnobs.(*sql.BackupRestoreTestingKnobs); ok {
		if restoreKnobs.RunAfterProcessingRestoreSpanEntry != nil {
			restoreKnobs.RunAfterProcessingRestoreSpanEntry(ctx)
		}
	}

	return batcher.GetSummary(), nil
}

func init() {
	rowexec.NewRestoreDataProcessor = newRestoreDataProcessor
}
