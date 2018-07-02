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
	"math/rand"
	"strings"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/transform"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"

	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlrun"
	"github.com/cockroachdb/cockroach/pkg/sql/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/pkg/errors"
)

type readFileFunc func(context.Context, io.Reader, int32, string, progressFn) error

type ctxProvider struct {
	context.Context
}

func (c ctxProvider) Ctx() context.Context {
	return c
}

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

			var src io.Reader
			compression := guessCompressionFromName(dataFile, format.Compression)
			switch compression {
			case roachpb.IOFileFormat_Gzip:
				r, err := gzip.NewReader(bc)
				if err != nil {
					return err
				}
				defer r.Close()
				src = r
			case roachpb.IOFileFormat_Bzip:
				src = bzip2.NewReader(bc)
			default:
				src = bc
			}

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

type kvBatch []roachpb.KeyValue

type rowConverter struct {
	// current row buf
	datums []tree.Datum

	// kv destination and current batch
	kvCh     chan<- kvBatch
	kvBatch  kvBatch
	batchCap int

	tableDesc *sqlbase.TableDescriptor

	// The rest of these are derived from tableDesc, just cached here.
	hidden                int
	ri                    sqlbase.RowInserter
	evalCtx               *tree.EvalContext
	cols                  []sqlbase.ColumnDescriptor
	visibleCols           []sqlbase.ColumnDescriptor
	visibleColTypes       []types.T
	defaultExprs          []tree.TypedExpr
	computedIVarContainer sqlbase.RowIndexedVarContainer
}

const kvBatchSize = 1000

func newRowConverter(
	tableDesc *sqlbase.TableDescriptor, evalCtx *tree.EvalContext, kvCh chan<- kvBatch,
) (*rowConverter, error) {
	c := &rowConverter{
		tableDesc: tableDesc,
		kvCh:      kvCh,
		evalCtx:   evalCtx,
	}

	ri, err := sqlbase.MakeRowInserter(nil /* txn */, tableDesc, nil, /* fkTables */
		tableDesc.Columns, false /* checkFKs */, &sqlbase.DatumAlloc{})
	if err != nil {
		return nil, errors.Wrap(err, "make row inserter")
	}
	c.ri = ri

	var txCtx transform.ExprTransformContext
	// Although we don't yet support DEFAULT expressions on visible columns,
	// we do on hidden columns (which is only the default _rowid one). This
	// allows those expressions to run.
	cols, defaultExprs, err := sqlbase.ProcessDefaultColumns(tableDesc.Columns, tableDesc, &txCtx, c.evalCtx)
	if err != nil {
		return nil, errors.Wrap(err, "process default columns")
	}
	c.cols = cols
	c.defaultExprs = defaultExprs

	c.visibleCols = tableDesc.VisibleColumns()
	c.visibleColTypes = make([]types.T, len(c.visibleCols))
	for i := range c.visibleCols {
		c.visibleColTypes[i] = c.visibleCols[i].DatumType()
	}
	c.datums = make([]tree.Datum, len(c.visibleCols), len(cols))

	// Check for a hidden column. This should be the unique_rowid PK if present.
	c.hidden = -1
	for i, col := range cols {
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

	padding := 2 * (len(tableDesc.Indexes) + len(tableDesc.Families))
	c.batchCap = kvBatchSize + padding
	c.kvBatch = make(kvBatch, 0, c.batchCap)

	c.computedIVarContainer = sqlbase.RowIndexedVarContainer{
		Mapping: ri.InsertColIDtoRowIndex,
		Cols:    tableDesc.Columns,
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

	row, err := sql.GenerateInsertRow(
		c.defaultExprs, computeExprs, c.cols, computedCols, *c.evalCtx, c.tableDesc, c.datums, &c.computedIVarContainer)
	if err != nil {
		return errors.Wrapf(err, "generate insert row")
	}
	if err := c.ri.InsertRow(
		ctx,
		inserter(func(kv roachpb.KeyValue) {
			kv.Value.InitChecksum(kv.Key)
			c.kvBatch = append(c.kvBatch, kv)
		}),
		row,
		true, /* ignoreConflicts */
		sqlbase.SkipFKs,
		false, /* traceKV */
	); err != nil {
		return errors.Wrapf(err, "insert row")
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
	c.kvBatch = make(kvBatch, 0, c.batchCap)
	return nil
}

var csvOutputTypes = []sqlbase.ColumnType{
	{SemanticType: sqlbase.ColumnType_BYTES},
	{SemanticType: sqlbase.ColumnType_BYTES},
}

func newReadImportDataProcessor(
	flowCtx *distsqlrun.FlowCtx,
	processorID int32,
	spec distsqlrun.ReadImportDataSpec,
	output distsqlrun.RowReceiver,
) (distsqlrun.Processor, error) {
	cp := &readImportDataProcessor{
		flowCtx:     flowCtx,
		processorID: processorID,
		spec:        spec,
		output:      output,
	}

	if err := cp.out.Init(&distsqlrun.PostProcessSpec{}, csvOutputTypes, flowCtx.NewEvalCtx(), output); err != nil {
		return nil, err
	}
	return cp, nil
}

type progressFn func(finished bool) error

type inputConverter interface {
	start(group ctxgroup.Group)
	readFile(ctx context.Context, input io.Reader, fileIdx int32, filename string, progress progressFn) error
	inputFinished(ctx context.Context)
}

type readImportDataProcessor struct {
	flowCtx     *distsqlrun.FlowCtx
	processorID int32
	spec        distsqlrun.ReadImportDataSpec
	out         distsqlrun.ProcOutputHelper
	output      distsqlrun.RowReceiver
}

var _ distsqlrun.Processor = &readImportDataProcessor{}

func (cp *readImportDataProcessor) OutputTypes() []sqlbase.ColumnType {
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

func (cp *readImportDataProcessor) Run(ctx context.Context, wg *sync.WaitGroup) {
	ctx, span := tracing.ChildSpan(ctx, "readImportDataProcessor")
	defer tracing.FinishSpan(span)

	if wg != nil {
		defer wg.Done()
	}

	if err := cp.doRun(ctx, wg); err != nil {
		distsqlrun.DrainAndClose(ctx, cp.output, err, func(context.Context) {} /* pushTrailingMeta */)
	} else {
		cp.out.Close()
	}
}

// doRun uses a more familiar error return API, allowing concise early returns
// on errors in setup that all then are handled by the actual DistSQL Run method
// wrapper, doing the correct DrainAndClose error handling logic.
func (cp *readImportDataProcessor) doRun(ctx context.Context, wg *sync.WaitGroup) error {
	group := ctxgroup.WithContext(ctx)
	kvCh := make(chan kvBatch)
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
		conv = newCSVInputReader(kvCh, cp.spec.Format.Csv, singleTable, evalCtx)
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
			return job.Progressed(ctx, func(ctx context.Context, details jobspb.ProgressDetails) float32 {
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

		return readInputFiles(ctx, cp.spec.Uri, cp.spec.Format, conv.readFile, progFn, cp.flowCtx.Settings)
	})

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

		typeBytes := sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_BYTES}
		for kvBatch := range kvCh {
			for _, kv := range kvBatch {
				// Allow KV pairs to be dropped if they belong to a completed span.
				if completedSpans.Contains(kv.Key) {
					continue
				}
				if sampleAll || keys.IsDescriptorKey(kv.Key) || fn(kv) {
					row := sqlbase.EncDatumRow{
						sqlbase.DatumToEncDatum(typeBytes, tree.NewDBytes(tree.DBytes(kv.Key))),
						sqlbase.DatumToEncDatum(typeBytes, tree.NewDBytes(tree.DBytes(kv.Value.RawBytes))),
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

func makeRowErr(file string, row int64, format string, args ...interface{}) error {
	return errors.Errorf("%q: row %d: "+format, append([]interface{}{file, row}, args...)...)
}

func init() {
	distsqlrun.NewReadImportDataProcessor = newReadImportDataProcessor
}
