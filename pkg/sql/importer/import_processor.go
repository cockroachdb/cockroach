// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package importer

import (
	"context"
	"runtime"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
)

var csvOutputTypes = []*types.T{
	types.Bytes,
	types.Bytes,
}

const readImportDataProcessorName = "readImportDataProcessor"

var importPKAdderBufferSize = func() *settings.ByteSizeSetting {
	s := settings.RegisterByteSizeSetting(
		settings.TenantWritable,
		"kv.bulk_ingest.pk_buffer_size",
		"the initial size of the BulkAdder buffer handling primary index imports",
		32<<20,
	)
	return s
}()

var importPKAdderMaxBufferSize = func() *settings.ByteSizeSetting {
	s := settings.RegisterByteSizeSetting(
		settings.TenantWritable,
		"kv.bulk_ingest.max_pk_buffer_size",
		"the maximum size of the BulkAdder buffer handling primary index imports",
		128<<20,
	)
	return s
}()

var importIndexAdderBufferSize = func() *settings.ByteSizeSetting {
	s := settings.RegisterByteSizeSetting(
		settings.TenantWritable,
		"kv.bulk_ingest.index_buffer_size",
		"the initial size of the BulkAdder buffer handling secondary index imports",
		32<<20,
	)
	return s
}()

var importIndexAdderMaxBufferSize = func() *settings.ByteSizeSetting {
	s := settings.RegisterByteSizeSetting(
		settings.TenantWritable,
		"kv.bulk_ingest.max_index_buffer_size",
		"the maximum size of the BulkAdder buffer handling secondary index imports",
		512<<20,
	)
	return s
}()

var importAtNow = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"bulkio.import_at_current_time.enabled",
	"write imported data at the current timestamp, when each batch is flushed",
	true,
)

var readerParallelismSetting = settings.RegisterIntSetting(
	settings.TenantWritable,
	"bulkio.import.reader_parallelism",
	"number of parallel workers to use to convert read data for formats that support parallel conversion; 0 indicates number of cores",
	0,
	settings.NonNegativeInt,
)

// ImportBufferConfigSizes determines the minimum, maximum and step size for the
// BulkAdder buffer used in import.
func importBufferConfigSizes(st *cluster.Settings, isPKAdder bool) (int64, func() int64) {
	if isPKAdder {
		return importPKAdderBufferSize.Get(&st.SV),
			func() int64 { return importPKAdderMaxBufferSize.Get(&st.SV) }
	}
	return importIndexAdderBufferSize.Get(&st.SV),
		func() int64 { return importIndexAdderMaxBufferSize.Get(&st.SV) }
}

// readImportDataProcessor is a processor that does not take any inputs. It
// starts a worker goroutine in Start(), which emits progress updates over an
// internally maintained channel. Next() will read from this channel until
// exhausted and then emit the summary that the worker goroutine returns. The
// processor is built this way in order to manage parallelism internally.
type readImportDataProcessor struct {
	execinfra.ProcessorBase

	flowCtx *execinfra.FlowCtx
	spec    execinfrapb.ReadImportDataSpec
	output  execinfra.RowReceiver

	progCh chan execinfrapb.RemoteProducerMetadata_BulkProcessorProgress

	seqChunkProvider *row.SeqChunkProvider

	importErr error
	summary   *roachpb.BulkOpSummary
}

var (
	_ execinfra.Processor = &readImportDataProcessor{}
	_ execinfra.RowSource = &readImportDataProcessor{}
)

func newReadImportDataProcessor(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec execinfrapb.ReadImportDataSpec,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
) (execinfra.Processor, error) {
	cp := &readImportDataProcessor{
		flowCtx: flowCtx,
		spec:    spec,
		output:  output,
		progCh:  make(chan execinfrapb.RemoteProducerMetadata_BulkProcessorProgress),
	}
	if err := cp.Init(cp, post, csvOutputTypes, flowCtx, processorID, output, nil, /* memMonitor */
		execinfra.ProcStateOpts{
			// This processor doesn't have any inputs to drain.
			InputsToDrain: nil,
		}); err != nil {
		return nil, err
	}

	// Load the import job running the import in case any of the columns have a
	// default expression which uses sequences. In this case we need to update the
	// job progress within the import processor.
	if cp.flowCtx.Cfg.JobRegistry != nil {
		cp.seqChunkProvider = &row.SeqChunkProvider{
			JobID:    cp.spec.Progress.JobID,
			Registry: cp.flowCtx.Cfg.JobRegistry,
		}
	}

	return cp, nil
}

// Start is part of the RowSource interface.
func (idp *readImportDataProcessor) Start(ctx context.Context) {
	ctx = logtags.AddTag(ctx, "job", idp.spec.JobID)
	ctx = idp.StartInternal(ctx, readImportDataProcessorName)
	log.Infof(ctx, "starting read import")
	// We don't have to worry about this go routine leaking because next we loop over progCh
	// which is closed only after the go routine returns.
	go func() {
		defer close(idp.progCh)
		idp.summary, idp.importErr = runImport(ctx, idp.flowCtx, &idp.spec, idp.progCh,
			idp.seqChunkProvider)
	}()
}

// Next is part of the RowSource interface.
func (idp *readImportDataProcessor) Next() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
	if idp.State != execinfra.StateRunning {
		return nil, idp.DrainHelper()
	}

	for prog := range idp.progCh {
		p := prog
		return nil, &execinfrapb.ProducerMetadata{BulkProcessorProgress: &p}
	}

	if idp.importErr != nil {
		idp.MoveToDraining(idp.importErr)
		return nil, idp.DrainHelper()
	}

	if idp.summary == nil {
		err := errors.Newf("no summary generated by %s", readImportDataProcessorName)
		idp.MoveToDraining(err)
		return nil, idp.DrainHelper()
	}

	// Once the import is done, send back to the controller the serialized
	// summary of the import operation. For more info see roachpb.BulkOpSummary.
	countsBytes, err := protoutil.Marshal(idp.summary)
	idp.MoveToDraining(err)
	if err != nil {
		return nil, idp.DrainHelper()
	}

	return rowenc.EncDatumRow{
		rowenc.DatumToEncDatum(types.Bytes, tree.NewDBytes(tree.DBytes(countsBytes))),
		rowenc.DatumToEncDatum(types.Bytes, tree.NewDBytes(tree.DBytes([]byte{}))),
	}, nil
}

func injectTimeIntoEvalCtx(ctx *tree.EvalContext, walltime int64) {
	sec := walltime / int64(time.Second)
	nsec := walltime % int64(time.Second)
	unixtime := timeutil.Unix(sec, nsec)
	ctx.StmtTimestamp = unixtime
	ctx.TxnTimestamp = unixtime
}

func makeInputConverter(
	ctx context.Context,
	semaCtx *tree.SemaContext,
	spec *execinfrapb.ReadImportDataSpec,
	evalCtx *tree.EvalContext,
	kvCh chan row.KVBatch,
	seqChunkProvider *row.SeqChunkProvider,
) (inputConverter, error) {
	injectTimeIntoEvalCtx(evalCtx, spec.WalltimeNanos)
	var singleTable catalog.TableDescriptor
	var singleTableTargetCols tree.NameList
	if len(spec.Tables) == 1 {
		for _, table := range spec.Tables {
			singleTable = tabledesc.NewBuilder(table.Desc).BuildImmutableTable()
			singleTableTargetCols = make(tree.NameList, len(table.TargetCols))
			for i, colName := range table.TargetCols {
				singleTableTargetCols[i] = tree.Name(colName)
			}
		}
	}

	if format := spec.Format.Format; singleTable == nil && !isMultiTableFormat(format) {
		return nil, errors.Errorf("%s only supports reading a single, pre-specified table", format.String())
	}

	if singleTable != nil {
		if idx := catalog.FindDeletableNonPrimaryIndex(singleTable, func(idx catalog.Index) bool {
			return idx.IsPartial()
		}); idx != nil {
			return nil, unimplemented.NewWithIssue(50225, "cannot import into table with partial indexes")
		}

		// If we're using a format like CSV where data columns are not "named", and
		// therefore cannot be mapped to schema columns, then require the user to
		// use IMPORT INTO.
		//
		// We could potentially do something smarter here and check that only a
		// suffix of the columns are computed, and then expect the data file to have
		// #(visible columns) - #(computed columns).
		if len(singleTableTargetCols) == 0 && !formatHasNamedColumns(spec.Format.Format) {
			for _, col := range singleTable.VisibleColumns() {
				if col.IsComputed() {
					return nil, unimplemented.NewWithIssueDetail(56002, "import.computed",
						"to use computed columns, use IMPORT INTO")
				}
			}
		}
	}

	readerParallelism := int(spec.ReaderParallelism)
	if readerParallelism <= 0 {
		readerParallelism = int(readerParallelismSetting.Get(&evalCtx.Settings.SV))
	}
	if readerParallelism <= 0 {
		readerParallelism = runtime.GOMAXPROCS(0)
	}

	switch spec.Format.Format {
	case roachpb.IOFileFormat_CSV:
		isWorkload := true
		for _, file := range spec.Uri {
			if _, err := parseWorkloadConfig(file); err != nil {
				isWorkload = false
				break
			}
		}
		if isWorkload {
			return newWorkloadReader(semaCtx, evalCtx, singleTable, kvCh, readerParallelism), nil
		}
		return newCSVInputReader(
			semaCtx, kvCh, spec.Format.Csv, spec.WalltimeNanos, readerParallelism,
			singleTable, singleTableTargetCols, evalCtx, seqChunkProvider), nil
	case roachpb.IOFileFormat_MysqlOutfile:
		return newMysqloutfileReader(
			semaCtx, spec.Format.MysqlOut, kvCh, spec.WalltimeNanos,
			readerParallelism, singleTable, singleTableTargetCols, evalCtx)
	case roachpb.IOFileFormat_Mysqldump:
		return newMysqldumpReader(ctx, semaCtx, kvCh, spec.WalltimeNanos, spec.Tables, evalCtx,
			spec.Format.MysqlDump)
	case roachpb.IOFileFormat_PgCopy:
		return newPgCopyReader(semaCtx, spec.Format.PgCopy, kvCh, spec.WalltimeNanos,
			readerParallelism, singleTable, singleTableTargetCols, evalCtx)
	case roachpb.IOFileFormat_PgDump:
		return newPgDumpReader(ctx, semaCtx, int64(spec.Progress.JobID), kvCh, spec.Format.PgDump,
			spec.WalltimeNanos, spec.Tables, evalCtx)
	case roachpb.IOFileFormat_Avro:
		return newAvroInputReader(
			semaCtx, kvCh, singleTable, spec.Format.Avro, spec.WalltimeNanos,
			readerParallelism, evalCtx)
	default:
		return nil, errors.Errorf(
			"Requested IMPORT format (%d) not supported by this node", spec.Format.Format)
	}
}

type tableAndIndex struct {
	tableID catid.DescID
	indexID catid.IndexID
}

// ingestKvs drains kvs from the channel until it closes, ingesting them using
// the BulkAdder. It handles the required buffering/sorting/etc.
func ingestKvs(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	spec *execinfrapb.ReadImportDataSpec,
	kvCh <-chan row.KVBatch,
	pkIndexAdder, indexAdder kvserverbase.BulkAdder,
	updateBuffered func(int32, int64, float32),
	pushProgress func(),
) (*roachpb.BulkOpSummary, error) {
	ctx, span := tracing.ChildSpan(ctx, "import-ingest-kvs")
	defer span.Finish()

	isPK := make(map[tableAndIndex]bool, len(spec.Tables))
	for _, t := range spec.Tables {
		isPK[tableAndIndex{tableID: t.Desc.ID, indexID: t.Desc.PrimaryIndex.ID}] = true
	}

	// stopProgress will be closed when there is no more progress to report.
	stopProgress := make(chan struct{})
	g := ctxgroup.WithContext(ctx)
	g.GoCtx(func(ctx context.Context) error {
		tick := time.NewTicker(time.Second * 10)
		defer tick.Stop()
		done := ctx.Done()
		for {
			select {
			case <-done:
				return ctx.Err()
			case <-stopProgress:
				return nil
			case <-tick.C:
				pushProgress()
			}
		}
	})

	g.GoCtx(func(ctx context.Context) error {
		defer close(stopProgress)

		// We insert splits at every index span of the table above. Since the
		// BulkAdder is split aware when constructing SSTs, there is no risk of worst
		// case overlap behavior in the resulting AddSSTable calls.
		//
		// NB: We are getting rid of the pre-buffering stage which constructed
		// separate buckets for each table's primary data, and flushed to the
		// BulkAdder when the bucket was full. This is because, a tpcc 1k IMPORT would
		// OOM when maintaining this buffer. Two big wins we got from this
		// pre-buffering stage were:
		//
		// 1. We avoided worst case overlapping behavior in the AddSSTable calls as a
		// result of flushing keys with the same TableIDIndexID prefix, together.
		//
		// 2. Secondary index KVs which were few and filled the bucket infrequently
		// were flushed rarely, resulting in fewer L0 (and total) files.
		//
		// While we continue to achieve the first property as a result of the splits
		// mentioned above, the KVs sent to the BulkAdder are no longer grouped which
		// results in flushing a much larger number of small SSTs. This increases the
		// number of L0 (and total) files, but with a lower memory usage.
		for kvBatch := range kvCh {
			for _, kv := range kvBatch.KVs {
				_, tableID, indexID, indexErr := flowCtx.Codec().DecodeIndexPrefix(kv.Key)
				if indexErr != nil {
					return indexErr
				}

				// Decide which adder to send the KV to by extracting its index id.
				//
				// TODO(adityamaru): There is a potential optimization of plumbing the
				// different putters, and differentiating based on their type. It might be
				// more efficient than parsing every kv.
				if isPK[tableAndIndex{tableID: catid.DescID(tableID), indexID: catid.IndexID(indexID)}] {
					if err := pkIndexAdder.Add(ctx, kv.Key, kv.Value.RawBytes); err != nil {
						if errors.HasType(err, (*kvserverbase.DuplicateKeyError)(nil)) {
							return errors.Wrap(err, "duplicate key in primary index")
						}
						return err
					}
				} else {
					if err := indexAdder.Add(ctx, kv.Key, kv.Value.RawBytes); err != nil {
						if errors.HasType(err, (*kvserverbase.DuplicateKeyError)(nil)) {
							return errors.Wrap(err, "duplicate key in index")
						}
						return err
					}
				}
			}
			updateBuffered(kvBatch.Source, kvBatch.LastRow, kvBatch.Progress)

			if flowCtx.Cfg.TestingKnobs.BulkAdderFlushesEveryBatch {
				_ = pkIndexAdder.Flush(ctx)
				_ = indexAdder.Flush(ctx)
				pushProgress()
			}
		}
		return nil
	})

	if err := g.Wait(); err != nil {
		return nil, err
	}

	if err := pkIndexAdder.Flush(ctx); err != nil {
		if errors.HasType(err, (*kvserverbase.DuplicateKeyError)(nil)) {
			return nil, errors.Wrap(err, "duplicate key in primary index")
		}
		return nil, err
	}

	if err := indexAdder.Flush(ctx); err != nil {
		if errors.HasType(err, (*kvserverbase.DuplicateKeyError)(nil)) {
			return nil, errors.Wrap(err, "duplicate key in index")
		}
		return nil, err
	}

	addedSummary := pkIndexAdder.GetSummary()
	addedSummary.Add(indexAdder.GetSummary())

	pkIndexAdder.Reset()
	indexAdder.Reset()

	return &addedSummary, nil
}

func init() {
	rowexec.NewReadImportDataProcessor = newReadImportDataProcessor
}
