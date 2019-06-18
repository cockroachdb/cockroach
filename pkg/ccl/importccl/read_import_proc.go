// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package importccl

import (
	"compress/bzip2"
	"compress/gzip"
	"context"
	"io"
	"io/ioutil"
	"math/rand"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlpb"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlrun"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/transform"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

type readFileFunc func(context.Context, io.Reader, int32, string, progressFn) error

// readInputFile reads each of the passed dataFiles using the passed func. The
// key part of dataFiles is the unique index of the data file among all files in
// the IMPORT. progressFn, if not nil, is periodically invoked with a percentage
// of the total progress of reading through all of the files. This percentage
// attempts to use the Size() method of ExportStorage to determine how many
// bytes must be read of the input files, and reports the percent of bytes read
// among all dataFiles. If any Size() fails for any file, then progress is
// reported only after each file has been read.
func readInputFiles(
	ctx context.Context,
	dataFiles map[int32]string,
	format roachpb.IOFileFormat,
	fileFunc readFileFunc,
	progressFn func(float32) error,
	settings *cluster.Settings,
) error {
	done := ctx.Done()

	var totalBytes, readBytes int64
	// Attempt to fetch total number of bytes for all files.
	for _, dataFile := range dataFiles {
		conf, err := storageccl.ExportStorageConfFromURI(dataFile)
		if err != nil {
			return err
		}
		es, err := storageccl.MakeExportStorage(ctx, conf, settings)
		if err != nil {
			return err
		}
		sz, err := es.Size(ctx, "")
		es.Close()
		if sz <= 0 {
			// Don't log dataFile here because it could leak auth information.
			log.Infof(ctx, "could not fetch file size; falling back to per-file progress: %v", err)
			totalBytes = 0
			break
		}
		totalBytes += sz
	}
	updateFromFiles := progressFn != nil && totalBytes == 0
	updateFromBytes := progressFn != nil && totalBytes > 0

	currentFile := 0
	for dataFileIndex, dataFile := range dataFiles {
		currentFile++
		select {
		case <-done:
			return ctx.Err()
		default:
		}
		if err := func() error {
			conf, err := storageccl.ExportStorageConfFromURI(dataFile)
			if err != nil {
				return err
			}
			es, err := storageccl.MakeExportStorage(ctx, conf, settings)
			if err != nil {
				return err
			}
			defer es.Close()
			f, err := es.ReadFile(ctx, "")
			if err != nil {
				return err
			}
			defer f.Close()
			bc := &byteCounter{r: f}
			src, err := decompressingReader(bc, dataFile, format.Compression)
			if err != nil {
				return err
			}
			defer src.Close()

			wrappedProgressFn := func(finished bool) error { return nil }
			if updateFromBytes {
				const progressBytes = 100 << 20
				wrappedProgressFn = func(finished bool) error {
					// progressBytes is the number of read bytes at which to report job progress. A
					// low value may cause excessive updates in the job table which can lead to
					// very large rows due to MVCC saving each version.
					if finished || bc.n > progressBytes {
						readBytes += bc.n
						bc.n = 0
						if err := progressFn(float32(readBytes) / float32(totalBytes)); err != nil {
							return err
						}
					}
					return nil
				}
			}

			if err := fileFunc(ctx, src, dataFileIndex, dataFile, wrappedProgressFn); err != nil {
				return errors.Wrap(err, dataFile)
			}
			if updateFromFiles {
				if err := progressFn(float32(currentFile) / float32(len(dataFiles))); err != nil {
					return err
				}
			}
			return nil
		}(); err != nil {
			return err
		}
	}
	return nil
}

func decompressingReader(
	in io.Reader, name string, hint roachpb.IOFileFormat_Compression,
) (io.ReadCloser, error) {
	switch guessCompressionFromName(name, hint) {
	case roachpb.IOFileFormat_Gzip:
		return gzip.NewReader(in)
	case roachpb.IOFileFormat_Bzip:
		return ioutil.NopCloser(bzip2.NewReader(in)), nil
	default:
		return ioutil.NopCloser(in), nil
	}
}

func guessCompressionFromName(
	name string, hint roachpb.IOFileFormat_Compression,
) roachpb.IOFileFormat_Compression {
	if hint != roachpb.IOFileFormat_Auto {
		return hint
	}
	switch {
	case strings.HasSuffix(name, ".gz"):
		return roachpb.IOFileFormat_Gzip
	case strings.HasSuffix(name, ".bz2") || strings.HasSuffix(name, ".bz"):
		return roachpb.IOFileFormat_Bzip
	default:
		return roachpb.IOFileFormat_None
	}
}

type byteCounter struct {
	r io.Reader
	n int64
}

func (b *byteCounter) Read(p []byte) (int, error) {
	n, err := b.r.Read(p)
	b.n += int64(n)
	return n, err
}

type rowConverter struct {
	// current row buf
	datums []tree.Datum

	// kv destination and current batch
	kvCh     chan<- []roachpb.KeyValue
	kvBatch  []roachpb.KeyValue
	batchCap int

	tableDesc *sqlbase.ImmutableTableDescriptor

	// The rest of these are derived from tableDesc, just cached here.
	hidden                int
	ri                    row.Inserter
	evalCtx               *tree.EvalContext
	cols                  []sqlbase.ColumnDescriptor
	visibleCols           []sqlbase.ColumnDescriptor
	visibleColTypes       []*types.T
	defaultExprs          []tree.TypedExpr
	computedIVarContainer sqlbase.RowIndexedVarContainer
}

const kvBatchSize = 5000

func newRowConverter(
	tableDesc *sqlbase.TableDescriptor, evalCtx *tree.EvalContext, kvCh chan<- []roachpb.KeyValue,
) (*rowConverter, error) {
	immutDesc := sqlbase.NewImmutableTableDescriptor(*tableDesc)
	c := &rowConverter{
		tableDesc: immutDesc,
		kvCh:      kvCh,
		evalCtx:   evalCtx,
	}

	ri, err := row.MakeInserter(nil /* txn */, immutDesc, nil, /* fkTables */
		immutDesc.Columns, false /* checkFKs */, &sqlbase.DatumAlloc{})
	if err != nil {
		return nil, errors.Wrap(err, "make row inserter")
	}
	c.ri = ri

	var txCtx transform.ExprTransformContext
	// Although we don't yet support DEFAULT expressions on visible columns,
	// we do on hidden columns (which is only the default _rowid one). This
	// allows those expressions to run.
	cols, defaultExprs, err := sqlbase.ProcessDefaultColumns(immutDesc.Columns, immutDesc, &txCtx, c.evalCtx)
	if err != nil {
		return nil, errors.Wrap(err, "process default columns")
	}
	c.cols = cols
	c.defaultExprs = defaultExprs

	c.visibleCols = immutDesc.VisibleColumns()
	c.visibleColTypes = make([]*types.T, len(c.visibleCols))
	for i := range c.visibleCols {
		c.visibleColTypes[i] = c.visibleCols[i].DatumType()
	}
	c.datums = make([]tree.Datum, len(c.visibleCols), len(cols))

	// Check for a hidden column. This should be the unique_rowid PK if present.
	c.hidden = -1
	for i := range cols {
		col := &cols[i]
		if col.Hidden {
			if col.DefaultExpr == nil || *col.DefaultExpr != "unique_rowid()" || c.hidden != -1 {
				return nil, errors.New("unexpected hidden column")
			}
			c.hidden = i
			c.datums = append(c.datums, nil)
		}
	}
	if len(c.datums) != len(cols) {
		return nil, errors.New("unexpected hidden column")
	}

	padding := 2 * (len(immutDesc.Indexes) + len(immutDesc.Families))
	c.batchCap = kvBatchSize + padding
	c.kvBatch = make([]roachpb.KeyValue, 0, c.batchCap)

	c.computedIVarContainer = sqlbase.RowIndexedVarContainer{
		Mapping: ri.InsertColIDtoRowIndex,
		Cols:    immutDesc.Columns,
	}
	return c, nil
}

func (c *rowConverter) row(ctx context.Context, fileIndex int32, rowIndex int64) error {
	if c.hidden >= 0 {
		// We don't want to call unique_rowid() for the hidden PK column because
		// it is not idempotent. The sampling from the first stage will be useless
		// during the read phase, producing a single range split with all of the
		// data. Instead, we will call our own function that mimics that function,
		// but more-or-less guarantees that it will not interfere with the numbers
		// that will be produced by it. The lower 15 bits mimic the node id, but as
		// the CSV file number. The upper 48 bits are the line number and mimic the
		// timestamp. It would take a file with many more than 2**32 lines to even
		// begin approaching what unique_rowid would return today, so we assume it
		// to be safe. Since the timestamp is won't overlap, it is safe to use any
		// number in the node id portion. The 15 bits in that portion should account
		// for up to 32k CSV files in a single IMPORT. In the case of > 32k files,
		// the data is xor'd so the final bits are flipped instead of set.
		c.datums[c.hidden] = tree.NewDInt(builtins.GenerateUniqueID(fileIndex, uint64(rowIndex)))
	}

	// TODO(justin): we currently disallow computed columns in import statements.
	var computeExprs []tree.TypedExpr
	var computedCols []sqlbase.ColumnDescriptor

	insertRow, err := sql.GenerateInsertRow(
		c.defaultExprs, computeExprs, c.cols, computedCols, c.evalCtx, c.tableDesc, c.datums, &c.computedIVarContainer)
	if err != nil {
		return errors.Wrap(err, "generate insert row")
	}
	if err := c.ri.InsertRow(
		ctx,
		inserter(func(kv roachpb.KeyValue) {
			kv.Value.InitChecksum(kv.Key)
			c.kvBatch = append(c.kvBatch, kv)
		}),
		insertRow,
		true, /* ignoreConflicts */
		row.SkipFKs,
		false, /* traceKV */
	); err != nil {
		return errors.Wrap(err, "insert row")
	}
	// If our batch is full, flush it and start a new one.
	if len(c.kvBatch) >= kvBatchSize {
		if err := c.sendBatch(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (c *rowConverter) sendBatch(ctx context.Context) error {
	if len(c.kvBatch) == 0 {
		return nil
	}
	select {
	case c.kvCh <- c.kvBatch:
	case <-ctx.Done():
		return ctx.Err()
	}
	c.kvBatch = make([]roachpb.KeyValue, 0, c.batchCap)
	return nil
}

var csvOutputTypes = []types.T{
	*types.Bytes,
	*types.Bytes,
}

func newReadImportDataProcessor(
	flowCtx *distsqlrun.FlowCtx,
	processorID int32,
	spec distsqlpb.ReadImportDataSpec,
	output distsqlrun.RowReceiver,
) (distsqlrun.Processor, error) {
	cp := &readImportDataProcessor{
		flowCtx:     flowCtx,
		processorID: processorID,
		spec:        spec,
		output:      output,
	}

	if err := cp.out.Init(&distsqlpb.PostProcessSpec{}, csvOutputTypes, flowCtx.NewEvalCtx(), output); err != nil {
		return nil, err
	}
	return cp, nil
}

type progressFn func(finished bool) error

type inputConverter interface {
	start(group ctxgroup.Group)
	readFiles(ctx context.Context, dataFiles map[int32]string, format roachpb.IOFileFormat, progressFn func(float32) error, settings *cluster.Settings) error
	inputFinished(ctx context.Context)
}

type readImportDataProcessor struct {
	flowCtx     *distsqlrun.FlowCtx
	processorID int32
	spec        distsqlpb.ReadImportDataSpec
	out         distsqlrun.ProcOutputHelper
	output      distsqlrun.RowReceiver
}

var _ distsqlrun.Processor = &readImportDataProcessor{}

func (cp *readImportDataProcessor) OutputTypes() []types.T {
	return csvOutputTypes
}

func isMultiTableFormat(format roachpb.IOFileFormat_FileFormat) bool {
	switch format {
	case roachpb.IOFileFormat_Mysqldump,
		roachpb.IOFileFormat_PgDump:
		return true
	}
	return false
}

func (cp *readImportDataProcessor) Run(ctx context.Context) {
	ctx, span := tracing.ChildSpan(ctx, "readImportDataProcessor")
	defer tracing.FinishSpan(span)

	if err := cp.doRun(ctx); err != nil {
		distsqlrun.DrainAndClose(ctx, cp.output, err, func(context.Context) {} /* pushTrailingMeta */)
	} else {
		cp.out.Close()
	}
}

// doRun uses a more familiar error return API, allowing concise early returns
// on errors in setup that all then are handled by the actual DistSQL Run method
// wrapper, doing the correct DrainAndClose error handling logic.
func (cp *readImportDataProcessor) doRun(ctx context.Context) error {
	group := ctxgroup.WithContext(ctx)
	kvCh := make(chan []roachpb.KeyValue, 10)
	evalCtx := cp.flowCtx.NewEvalCtx()

	var singleTable *sqlbase.TableDescriptor
	if len(cp.spec.Tables) == 1 {
		for _, table := range cp.spec.Tables {
			singleTable = table
		}
	}

	if format := cp.spec.Format.Format; singleTable == nil && !isMultiTableFormat(format) {
		return errors.Errorf("%s only supports reading a single, pre-specified table", format.String())
	}

	var conv inputConverter
	var err error
	switch cp.spec.Format.Format {
	case roachpb.IOFileFormat_CSV:
		isWorkload := true
		for _, file := range cp.spec.Uri {
			if conf, err := storageccl.ExportStorageConfFromURI(file); err != nil || conf.Provider != roachpb.ExportStorageProvider_Workload {
				isWorkload = false
				break
			}
		}
		if isWorkload {
			conv = newWorkloadReader(kvCh, singleTable, evalCtx)
		} else {
			conv = newCSVInputReader(kvCh, cp.spec.Format.Csv, singleTable, evalCtx)
		}
	case roachpb.IOFileFormat_MysqlOutfile:
		conv, err = newMysqloutfileReader(kvCh, cp.spec.Format.MysqlOut, singleTable, evalCtx)
	case roachpb.IOFileFormat_Mysqldump:
		conv, err = newMysqldumpReader(kvCh, cp.spec.Tables, evalCtx)
	case roachpb.IOFileFormat_PgCopy:
		conv, err = newPgCopyReader(kvCh, cp.spec.Format.PgCopy, singleTable, evalCtx)
	case roachpb.IOFileFormat_PgDump:
		conv, err = newPgDumpReader(kvCh, cp.spec.Format.PgDump, cp.spec.Tables, evalCtx)
	default:
		err = errors.Errorf("Requested IMPORT format (%d) not supported by this node", cp.spec.Format.Format)
	}
	if err != nil {
		return err
	}
	conv.start(group)

	// Read input files into kvs
	group.GoCtx(func(ctx context.Context) error {
		ctx, span := tracing.ChildSpan(ctx, "readImportFiles")
		defer tracing.FinishSpan(span)
		defer conv.inputFinished(ctx)

		job, err := cp.flowCtx.JobRegistry.LoadJob(ctx, cp.spec.Progress.JobID)
		if err != nil {
			return err
		}

		progFn := func(pct float32) error {
			return job.FractionProgressed(ctx, func(ctx context.Context, details jobspb.ProgressDetails) float32 {
				d := details.(*jobspb.Progress_Import).Import
				slotpct := pct * cp.spec.Progress.Contribution
				if len(d.SamplingProgress) > 0 {
					d.SamplingProgress[cp.spec.Progress.Slot] = slotpct
				} else {
					d.ReadProgress[cp.spec.Progress.Slot] = slotpct
				}
				return d.Completed()
			})
		}

		return conv.readFiles(ctx, cp.spec.Uri, cp.spec.Format, progFn, cp.flowCtx.Settings)
	})

	if cp.spec.IngestDirectly {
		for _, tbl := range cp.spec.Tables {
			for _, span := range tbl.AllIndexSpans() {
				if err := cp.flowCtx.ClientDB.AdminSplit(ctx, span.Key, span.Key, hlc.Timestamp{} /* expirationTime */); err != nil {
					return err
				}

				log.VEventf(ctx, 1, "scattering index range %s", span.Key)
				scatterReq := &roachpb.AdminScatterRequest{
					RequestHeader: roachpb.RequestHeaderFromSpan(span),
				}
				if _, pErr := client.SendWrapped(ctx, cp.flowCtx.ClientDB.NonTransactionalSender(), scatterReq); pErr != nil {
					log.Errorf(ctx, "failed to scatter span %s: %s", span.Key, pErr)
				}
			}
		}
		// IngestDirectly means this reader will just ingest the KVs that the
		// producer emitted to the chan, and the only result we push into distsql at
		// the end is one row containing an encoded BulkOpSummary.
		group.GoCtx(func(ctx context.Context) error {
			ctx, span := tracing.ChildSpan(ctx, "ingestKVs")
			defer tracing.FinishSpan(span)

			writeTS := hlc.Timestamp{WallTime: cp.spec.WalltimeNanos}
			const bufferSize, flushSize = 64 << 20, 16 << 20
			adder, err := cp.flowCtx.BulkAdder(ctx, cp.flowCtx.ClientDB, bufferSize, flushSize, writeTS)
			if err != nil {
				return err
			}
			defer adder.Close(ctx)

			// Drain the kvCh using the BulkAdder until it closes.
			if err := ingestKvs(ctx, adder, kvCh); err != nil {
				return err
			}

			added := adder.GetSummary()
			countsBytes, err := protoutil.Marshal(&added)
			if err != nil {
				return err
			}
			cs, err := cp.out.EmitRow(ctx, sqlbase.EncDatumRow{
				sqlbase.DatumToEncDatum(types.Bytes, tree.NewDBytes(tree.DBytes(countsBytes))),
				sqlbase.DatumToEncDatum(types.Bytes, tree.NewDBytes(tree.DBytes([]byte{}))),
			})
			if err != nil {
				return err
			}
			if cs != distsqlrun.NeedMoreRows {
				return errors.New("unexpected closure of consumer")
			}
			return nil
		})
	} else {
		// Sample KVs
		group.GoCtx(func(ctx context.Context) error {

			ctx, span := tracing.ChildSpan(ctx, "sendImportKVs")
			defer tracing.FinishSpan(span)

			var fn sampleFunc
			var sampleAll bool
			if cp.spec.SampleSize == 0 {
				sampleAll = true
			} else {
				sr := sampleRate{
					rnd:        rand.New(rand.NewSource(rand.Int63())),
					sampleSize: float64(cp.spec.SampleSize),
				}
				fn = sr.sample
			}

			// Populate the split-point spans which have already been imported.
			var completedSpans roachpb.SpanGroup
			job, err := cp.flowCtx.JobRegistry.LoadJob(ctx, cp.spec.Progress.JobID)
			if err != nil {
				return err
			}
			progress := job.Progress()
			if details, ok := progress.Details.(*jobspb.Progress_Import); ok {
				completedSpans.Add(details.Import.SpanProgress...)
			} else {
				return errors.Errorf("unexpected progress type %T", progress)
			}

			for kvBatch := range kvCh {
				for _, kv := range kvBatch {
					// Allow KV pairs to be dropped if they belong to a completed span.
					if completedSpans.Contains(kv.Key) {
						continue
					}

					rowRequired := sampleAll || keys.IsDescriptorKey(kv.Key)
					if rowRequired || fn(kv) {
						var row sqlbase.EncDatumRow
						if rowRequired {
							row = sqlbase.EncDatumRow{
								sqlbase.DatumToEncDatum(types.Bytes, tree.NewDBytes(tree.DBytes(kv.Key))),
								sqlbase.DatumToEncDatum(types.Bytes, tree.NewDBytes(tree.DBytes(kv.Value.RawBytes))),
							}
						} else {
							// Don't send the value for rows returned for sampling
							row = sqlbase.EncDatumRow{
								sqlbase.DatumToEncDatum(types.Bytes, tree.NewDBytes(tree.DBytes(kv.Key))),
								sqlbase.DatumToEncDatum(types.Bytes, tree.NewDBytes(tree.DBytes([]byte{}))),
							}
						}

						cs, err := cp.out.EmitRow(ctx, row)
						if err != nil {
							return err
						}
						if cs != distsqlrun.NeedMoreRows {
							return errors.New("unexpected closure of consumer")
						}
					}
				}
			}
			return nil
		})
	}

	return group.Wait()
}

type sampleFunc func(roachpb.KeyValue) bool

// sampleRate is a sampleFunc that samples a row with a probability of the
// row's size / the sample size.
type sampleRate struct {
	rnd        *rand.Rand
	sampleSize float64
}

func (s sampleRate) sample(kv roachpb.KeyValue) bool {
	sz := float64(len(kv.Key) + len(kv.Value.RawBytes))
	prob := sz / s.sampleSize
	return prob > s.rnd.Float64()
}

func makeRowErr(file string, row int64, code, format string, args ...interface{}) error {
	return pgerror.NewWithDepthf(1, code,
		"%q: row %d: "+format, append([]interface{}{file, row}, args...)...)
}

func wrapRowErr(err error, file string, row int64, code, format string, args ...interface{}) error {
	if format != "" || len(args) > 0 {
		err = errors.WrapWithDepthf(1, err, format, args...)
	}
	err = errors.WrapWithDepthf(1, err, "%q: row %d", file, row)
	if code != pgcode.Uncategorized {
		err = pgerror.WithCandidateCode(err, code)
	}
	return err
}

// ingestKvs drains kvs from the channel until it closes, ingesting them using
// the BulkAdder. It handles the required buffering/sorting/etc.
func ingestKvs(
	ctx context.Context, adder storagebase.BulkAdder, kvCh <-chan []roachpb.KeyValue,
) error {
	const sortBatchSize = 48 << 20 // 48MB

	// TODO(dt): buffer to disk instead of all in-mem.

	// Batching all kvs together leads to worst case overlap behavior in the
	// resulting AddSSTable calls, leading to compactions and potentially L0
	// stalls. Instead maintain a separate buffer for each table's primary data.
	// This optimizes for the case when the data arriving to IMPORT is already
	// sorted by primary key, leading to no overlapping AddSSTable requests. Given
	// that many workloads (and actual imported data) will be sorted by primary
	// key, it makes sense to try to exploit this.
	//
	// TODO(dan): This was merged because it stabilized direct ingest IMPORT, but
	// we may be able to do something simpler (such as chunking along index
	// boundaries in flush) or more general (such as chunking based on the common
	// prefix of the last N kvs).
	kvsByTableIDIndexID := make(map[string]roachpb.KeyValueByKey)
	sizeByTableIDIndexID := make(map[string]int64)

	flush := func(ctx context.Context, buf roachpb.KeyValueByKey) error {
		if len(buf) == 0 {
			return nil
		}
		for i := range buf {
			if err := adder.Add(ctx, buf[i].Key, buf[i].Value.RawBytes); err != nil {
				if _, ok := err.(storagebase.DuplicateKeyError); ok {
					return errors.WithStack(err)
				}
				return err
			}
		}
		return nil
	}

	for kvBatch := range kvCh {
		for _, kv := range kvBatch {
			tableLen, err := encoding.PeekLength(kv.Key)
			if err != nil {
				return err
			}
			indexLen, err := encoding.PeekLength(kv.Key[tableLen:])
			if err != nil {
				return err
			}
			bufKey := kv.Key[:tableLen+indexLen]
			kvsByTableIDIndexID[string(bufKey)] = append(kvsByTableIDIndexID[string(bufKey)], kv)
			sizeByTableIDIndexID[string(bufKey)] += int64(len(kv.Key) + len(kv.Value.RawBytes))

			// TODO(dan): Prevent unbounded memory usage by flushing the largest
			// buffer when the total size of all buffers exceeds some threshold.
			if s := sizeByTableIDIndexID[string(bufKey)]; s > sortBatchSize {
				buf := kvsByTableIDIndexID[string(bufKey)]
				if err := flush(ctx, buf); err != nil {
					return err
				}
				kvsByTableIDIndexID[string(bufKey)] = buf[:0]
				sizeByTableIDIndexID[string(bufKey)] = 0
			}
		}
	}
	for _, buf := range kvsByTableIDIndexID {
		if err := flush(ctx, buf); err != nil {
			return err
		}
	}

	if err := adder.Flush(ctx); err != nil {
		if err, ok := err.(storagebase.DuplicateKeyError); ok {
			return errors.WithStack(err)
		}
		return err
	}
	return nil
}

func init() {
	distsqlrun.NewReadImportDataProcessor = newReadImportDataProcessor
}
