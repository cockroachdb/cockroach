// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backupccl

import (
	"bytes"
	"context"
	"crypto/sha512"
	"io"
	"io/ioutil"

	"github.com/cockroachdb/cockroach/pkg/base"
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
	"github.com/cockroachdb/cockroach/pkg/storage/cloudimpl"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/vfs"
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

	alloc rowenc.DatumAlloc
	kr    *storageccl.KeyRewriter
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
	return rd, nil
}

// Start is part of the RowSource interface.
func (rd *restoreDataProcessor) Start(ctx context.Context) context.Context {
	rd.input.Start(ctx)
	return rd.StartInternal(ctx, restoreDataProcName)
}

// Next is part of the RowSource interface.
func (rd *restoreDataProcessor) Next() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
	if rd.State != execinfra.StateRunning {
		return nil, rd.DrainHelper()
	}
	// We read rows from the SplitAndScatter processor. We expect each row to
	// contain 2 columns. The first is used to route the row to this processor,
	// and the second contains the RestoreSpanEntry that we're interested in.
	row, meta := rd.input.Next()
	if meta != nil {
		if meta.Err != nil {
			rd.MoveToDraining(nil /* err */)
		}
		return nil, meta
	}
	if row == nil {
		rd.MoveToDraining(nil /* err */)
		return nil, rd.DrainHelper()
	}

	if len(row) != 2 {
		rd.MoveToDraining(errors.New("expected input rows to have exactly 2 columns"))
		return nil, rd.DrainHelper()
	}
	if err := row[1].EnsureDecoded(types.Bytes, &rd.alloc); err != nil {
		rd.MoveToDraining(err)
		return nil, rd.DrainHelper()
	}
	datum := row[1].Datum
	entryDatumBytes, ok := datum.(*tree.DBytes)
	if !ok {
		rd.MoveToDraining(errors.AssertionFailedf(`unexpected datum type %T: %+v`, datum, row))
		return nil, rd.DrainHelper()
	}

	var entry execinfrapb.RestoreSpanEntry
	if err := protoutil.Unmarshal([]byte(*entryDatumBytes), &entry); err != nil {
		rd.MoveToDraining(errors.Wrap(err, "un-marshaling restore span entry"))
		return nil, rd.DrainHelper()
	}

	newSpanKey, err := rewriteBackupSpanKey(rd.flowCtx.Codec(), rd.kr, entry.Span.Key)
	if err != nil {
		rd.MoveToDraining(errors.Wrap(err, "re-writing span key to import"))
		return nil, rd.DrainHelper()
	}

	log.VEventf(rd.Ctx, 1 /* level */, "importing span %v", entry.Span)
	summary, err := rd.processRestoreSpanEntry(entry, newSpanKey)
	if err != nil {
		rd.MoveToDraining(err)
		return nil, rd.DrainHelper()
	}

	var prog execinfrapb.RemoteProducerMetadata_BulkProcessorProgress
	progDetails := RestoreProgress{}
	progDetails.Summary = countRows(summary, rd.spec.PKIDs)
	progDetails.ProgressIdx = entry.ProgressIdx
	progDetails.DataSpan = entry.Span
	details, err := gogotypes.MarshalAny(&progDetails)
	if err != nil {
		rd.MoveToDraining(err)
		return nil, rd.DrainHelper()
	}
	prog.ProgressDetails = *details
	return nil, &execinfrapb.ProducerMetadata{BulkProcessorProgress: &prog}
}

// ConsumerClosed is part of the RowSource interface.
func (rd *restoreDataProcessor) ConsumerClosed() {
	rd.InternalClose()
}

func init() {
	rowexec.NewRestoreDataProcessor = newRestoreDataProcessor
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

	// In the loop below, each file is opened, some checks are done and then an
	// iterator is created for that file and added to iters. Once the iterator is
	// created, it owns the file and will Close it, but until then we need to
	// ensure we close it (e.g. if we return early before handing it to our iter).
	// Thus the loop below stores its intermediate file in `f` and we defer a func
	// closing over f that can close if if we return mid-loop.
	var f vfs.File
	defer func() {
		if f != nil {
			f.Close()
		}
	}()

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

		if err := retry.WithMaxAttempts(ctx, base.DefaultRetryOptions(), maxAttempts, func() error {
			var err error
			f, err = cloudimpl.NewFileWrapper(ctx, dir, file.Path)
			return err
		}); err != nil {
			return summary, errors.Wrapf(err, "fetching %q", file.Path)
		}

		// If the file is encrypted, download and decrypt it and make a new file on
		// the decrypted content. In either case, keep the original/underlying raw
		// file reader if we need to check checksums.
		// TODO(dt): use streaming decryption on the fly wrapper from #58181.
		var raw io.Reader = f
		if rd.spec.Encryption != nil {
			ciphertext, err := ioutil.ReadAll(f)
			if err != nil {
				return summary, err
			}
			f.Close()
			content, err := storageccl.DecryptFile(ciphertext, rd.spec.Encryption.Key)
			if err != nil {
				return summary, err
			}
			f = vfs.NewMemFile(content)
			raw = bytes.NewReader(ciphertext)
		}

		if len(file.Sha512) > 0 {
			h := sha512.New()
			if _, err := io.Copy(h, raw); err != nil {
				return summary, err
			}
			if !bytes.Equal(h.Sum(nil), file.Sha512) {
				return summary, errors.Errorf("checksum mismatch for %s", file.Path)
			}
		}

		iter, err := storage.NewSSTIterator(f)
		if err != nil {
			return summary, err
		}
		defer iter.Close()
		f = nil // prevent double-Close now that the iter owns it.
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
