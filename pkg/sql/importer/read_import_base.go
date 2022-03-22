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
	"bytes"
	"compress/bzip2"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/ioctx"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

func runImport(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	spec *execinfrapb.ReadImportDataSpec,
	progCh chan execinfrapb.RemoteProducerMetadata_BulkProcessorProgress,
	seqChunkProvider *row.SeqChunkProvider,
) (*roachpb.BulkOpSummary, error) {
	// Used to send ingested import rows to the KV layer.
	kvCh := make(chan row.KVBatch, 10)

	// Install type metadata in all of the import tables.
	importResolver := newImportTypeResolver(spec.Types)
	for _, table := range spec.Tables {
		if err := typedesc.HydrateTypesInTableDescriptor(ctx, table.Desc, importResolver); err != nil {
			return nil, err
		}
	}

	evalCtx := flowCtx.NewEvalCtx()
	// TODO(adityamaru): Should we just plumb the flowCtx instead of this
	// assignment.
	evalCtx.DB = flowCtx.Cfg.DB
	evalCtx.Regions = makeImportRegionOperator(spec.DatabasePrimaryRegion)
	semaCtx := tree.MakeSemaContext()
	semaCtx.TypeResolver = importResolver
	conv, err := makeInputConverter(ctx, &semaCtx, spec, evalCtx, kvCh, seqChunkProvider)
	if err != nil {
		return nil, err
	}

	// This group holds the go routines that are responsible for producing KV batches.
	// and ingesting produced KVs.
	// Depending on the import implementation both conv.start and conv.readFiles can
	// produce KVs so we should close the channel only after *both* are finished.
	group := ctxgroup.WithContext(ctx)
	conv.start(group)

	// Read input files into kvs
	group.GoCtx(func(ctx context.Context) error {
		defer close(kvCh)
		ctx, span := tracing.ChildSpan(ctx, "import-files-to-kvs")
		defer span.Finish()
		var inputs map[int32]string
		if spec.ResumePos != nil {
			// Filter out files that were completely processed.
			inputs = make(map[int32]string)
			for id, name := range spec.Uri {
				if seek, ok := spec.ResumePos[id]; !ok || seek < math.MaxInt64 {
					inputs[id] = name
				}
			}
		} else {
			inputs = spec.Uri
		}

		return conv.readFiles(ctx, inputs, spec.ResumePos, spec.Format, flowCtx.Cfg.ExternalStorage,
			spec.User())
	})

	// Ingest the KVs that the producer group emitted to the chan and the row result
	// at the end is one row containing an encoded BulkOpSummary.
	var summary *roachpb.BulkOpSummary
	group.GoCtx(func(ctx context.Context) error {
		summary, err = ingestKvs(ctx, flowCtx, spec, progCh, kvCh)
		if err != nil {
			return err
		}
		var prog execinfrapb.RemoteProducerMetadata_BulkProcessorProgress
		prog.ResumePos = make(map[int32]int64)
		prog.CompletedFraction = make(map[int32]float32)
		for i := range spec.Uri {
			prog.CompletedFraction[i] = 1.0
			prog.ResumePos[i] = math.MaxInt64
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case progCh <- prog:
			return nil
		}
	})

	if err = group.Wait(); err != nil {
		return nil, err
	}

	return summary, nil
}

type readFileFunc func(context.Context, *fileReader, int32, int64, chan string) error

// readInputFile reads each of the passed dataFiles using the passed func. The
// key part of dataFiles is the unique index of the data file among all files in
// the IMPORT. progressFn, if not nil, is periodically invoked with a percentage
// of the total progress of reading through all of the files. This percentage
// attempts to use the Size() method of ExternalStorage to determine how many
// bytes must be read of the input files, and reports the percent of bytes read
// among all dataFiles. If any Size() fails for any file, then progress is
// reported only after each file has been read.
func readInputFiles(
	ctx context.Context,
	dataFiles map[int32]string,
	resumePos map[int32]int64,
	format roachpb.IOFileFormat,
	fileFunc readFileFunc,
	makeExternalStorage cloud.ExternalStorageFactory,
	user security.SQLUsername,
) error {
	done := ctx.Done()

	fileSizes := make(map[int32]int64, len(dataFiles))

	// "Pre-import" work.
	// Validate readability early, and attempt to fetch total number of bytes for
	// all files to track progress.
	for id, dataFile := range dataFiles {
		if err := func() error {
			// Run within an anonymous function to release each connection after each
			// iteration, rather than all at once after the `for` loop.
			conf, err := cloud.ExternalStorageConfFromURI(dataFile, user)
			if err != nil {
				return err
			}
			es, err := makeExternalStorage(ctx, conf)
			if err != nil {
				return err
			}
			defer es.Close()

			sz, err := es.Size(ctx, "")

			if sz <= 0 {
				// Don't log dataFile here because it could leak auth information.
				log.Infof(ctx, "could not fetch file size; falling back to per-file progress: %v", err)
			} else {
				fileSizes[id] = sz
			}

			if len(dataFiles) > 1 {
				// If there's more than one file, try to read a byte from each to verify
				// readability (e.g. permissions).
				// If there's only one file, skip that check because it provides no
				// advantage.
				raw, err := es.ReadFile(ctx, "")
				if err != nil {
					return err
				}
				defer raw.Close(ctx)

				p := make([]byte, 1)
				if _, err := raw.Read(ctx, p); err != nil && err != io.EOF {
					// Check that we can read the file. We don't care about content yet,
					// so we read a single byte and we don't process it in any way.
					// If the file is empty -- and we can tell that -- that also counts
					// as readable, so don't error.
					return err
				}

			}

			return nil
		}(); err != nil {
			return err
		}
	}

	for dataFileIndex, dataFile := range dataFiles {
		select {
		case <-done:
			return ctx.Err()
		default:
		}
		if err := func() error {
			conf, err := cloud.ExternalStorageConfFromURI(dataFile, user)
			if err != nil {
				return err
			}
			es, err := makeExternalStorage(ctx, conf)
			if err != nil {
				return err
			}
			defer es.Close()
			raw, err := es.ReadFile(ctx, "")
			if err != nil {
				return err
			}
			defer raw.Close(ctx)

			src := &fileReader{total: fileSizes[dataFileIndex], counter: byteCounter{r: ioctx.ReaderCtxAdapter(ctx, raw)}}
			decompressed, err := decompressingReader(&src.counter, dataFile, format.Compression)
			if err != nil {
				return err
			}
			defer decompressed.Close()
			src.Reader = decompressed

			var rejected chan string
			if (format.Format == roachpb.IOFileFormat_CSV && format.SaveRejected) ||
				(format.Format == roachpb.IOFileFormat_MysqlOutfile && format.SaveRejected) {
				rejected = make(chan string)
			}
			if rejected != nil {
				grp := ctxgroup.WithContext(ctx)
				grp.GoCtx(func(ctx context.Context) error {
					var buf []byte
					var countRejected int64
					for s := range rejected {
						countRejected++
						if countRejected > 1000 { // TODO(spaskob): turn the magic constant into an option
							return pgerror.Newf(
								pgcode.DataCorrupted,
								"too many parsing errors (%d) encountered for file %s",
								countRejected,
								dataFile,
							)
						}
						buf = append(buf, s...)
					}
					if countRejected == 0 {
						// no rejected rows
						return nil
					}
					rejFn, err := rejectedFilename(dataFile)
					if err != nil {
						return err
					}
					conf, err := cloud.ExternalStorageConfFromURI(rejFn, user)
					if err != nil {
						return err
					}
					rejectedStorage, err := makeExternalStorage(ctx, conf)
					if err != nil {
						return err
					}
					defer rejectedStorage.Close()
					if err := cloud.WriteFile(ctx, rejectedStorage, "", bytes.NewReader(buf)); err != nil {
						return err
					}
					return nil
				})

				grp.GoCtx(func(ctx context.Context) error {
					defer close(rejected)
					if err := fileFunc(ctx, src, dataFileIndex, resumePos[dataFileIndex], rejected); err != nil {
						return err
					}
					return nil
				})

				if err := grp.Wait(); err != nil {
					return errors.Wrapf(err, "%s", dataFile)
				}
			} else {
				if err := fileFunc(ctx, src, dataFileIndex, resumePos[dataFileIndex], nil /* rejected */); err != nil {
					return errors.Wrapf(err, "%s", dataFile)
				}
			}
			return nil
		}(); err != nil {
			return err
		}
	}
	return nil
}

func rejectedFilename(datafile string) (string, error) {
	parsedURI, err := url.Parse(datafile)
	if err != nil {
		return "", err
	}
	parsedURI.Path = parsedURI.Path + ".rejected"
	return parsedURI.String(), nil
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
		if parsed, err := url.Parse(name); err == nil && parsed.Path != name {
			return guessCompressionFromName(parsed.Path, hint)
		}
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

type fileReader struct {
	io.Reader
	total   int64
	counter byteCounter
}

func (f fileReader) ReadFraction() float32 {
	if f.total == 0 {
		return 0.0
	}
	return float32(f.counter.n) / float32(f.total)
}

type inputConverter interface {
	start(group ctxgroup.Group)
	readFiles(ctx context.Context, dataFiles map[int32]string, resumePos map[int32]int64,
		format roachpb.IOFileFormat, makeExternalStorage cloud.ExternalStorageFactory, user security.SQLUsername) error
}

// formatHasNamedColumns returns true if the data in the input files can be
// mapped specifically to a particular data column.
func formatHasNamedColumns(format roachpb.IOFileFormat_FileFormat) bool {
	switch format {
	case roachpb.IOFileFormat_Avro,
		roachpb.IOFileFormat_Mysqldump,
		roachpb.IOFileFormat_PgDump:
		return true
	}
	return false
}

func isMultiTableFormat(format roachpb.IOFileFormat_FileFormat) bool {
	switch format {
	case roachpb.IOFileFormat_Mysqldump,
		roachpb.IOFileFormat_PgDump:
		return true
	}
	return false
}

func makeRowErr(row int64, code pgcode.Code, format string, args ...interface{}) error {
	err := pgerror.NewWithDepthf(1, code, format, args...)
	err = errors.WrapWithDepthf(1, err, "row %d", row)
	return err
}

func wrapRowErr(err error, row int64, code pgcode.Code, format string, args ...interface{}) error {
	if format != "" || len(args) > 0 {
		err = errors.WrapWithDepthf(1, err, format, args...)
	}
	err = errors.WrapWithDepthf(1, err, "row %d", row)
	if code != pgcode.Uncategorized {
		err = pgerror.WithCandidateCode(err, code)
	}
	return err
}

// importRowError is an error type describing malformed import data.
type importRowError struct {
	err    error
	row    string
	rowNum int64
}

const (
	importRowErrMaxRuneCount    = 1024
	importRowErrTruncatedMarker = " -- TRUNCATED"
)

func (e *importRowError) Error() string {
	// The job system will truncate this error before saving it,
	// but we will additionally truncate it here since it is
	// separately written to the log and could easily result in
	// very large log files.
	rowForLog := e.row
	if len(rowForLog) > importRowErrMaxRuneCount {
		rowForLog = util.TruncateString(rowForLog, importRowErrMaxRuneCount) + importRowErrTruncatedMarker
	}
	return fmt.Sprintf("error parsing row %d: %v (row: %s)", e.rowNum, e.err, rowForLog)
}

func newImportRowError(err error, row string, num int64) error {
	return &importRowError{
		err:    err,
		row:    row,
		rowNum: num,
	}
}

// parallelImportContext describes state associated with the import.
type parallelImportContext struct {
	walltime         int64                   // Import time stamp.
	numWorkers       int                     // Parallelism.
	batchSize        int                     // Number of records to batch.
	semaCtx          *tree.SemaContext       // Semantic analysis context.
	evalCtx          *tree.EvalContext       // Evaluation context.
	tableDesc        catalog.TableDescriptor // Table descriptor we're importing into.
	targetCols       tree.NameList           // List of columns to import.  nil if importing all columns.
	kvCh             chan row.KVBatch        // Channel for sending KV batches.
	seqChunkProvider *row.SeqChunkProvider   // Used to reserve chunks of sequence values.
}

// importFileContext describes state specific to a file being imported.
type importFileContext struct {
	source   int32       // Source is where the row data in the batch came from.
	skip     int64       // Number of records to skip
	rejected chan string // Channel for reporting corrupt "rows"
	rowLimit int64       // Number of records to process before we stop importing from a file.
}

// handleCorruptRow reports an error encountered while processing a row
// in an input file.
func handleCorruptRow(ctx context.Context, fileCtx *importFileContext, err error) error {
	log.Errorf(ctx, "%+v", err)

	if rowErr := (*importRowError)(nil); errors.As(err, &rowErr) && fileCtx.rejected != nil {
		fileCtx.rejected <- rowErr.row + "\n"
		return nil
	}

	return err
}

func makeDatumConverter(
	ctx context.Context, importCtx *parallelImportContext, fileCtx *importFileContext,
) (*row.DatumRowConverter, error) {
	conv, err := row.NewDatumRowConverter(
		ctx, importCtx.semaCtx, importCtx.tableDesc, importCtx.targetCols, importCtx.evalCtx,
		importCtx.kvCh, importCtx.seqChunkProvider, nil /* metrics */)
	if err == nil {
		conv.KvBatch.Source = fileCtx.source
	}
	return conv, err
}

// importRowProducer is producer of "rows" that must be imported.
// Row is an opaque interface{} object which will be passed onto
// the consumer implementation.
// The implementations of this interface need not need to be thread safe.
// However, since the data returned by the Row() method may be
// handled by a different go-routine, the data returned must not access,
// or reference internal state in a thread-unsafe way.
type importRowProducer interface {
	// Scan returns true if there is more data available.
	// After Scan() returns false, the caller should verify
	// that the scanner has not encountered an error (Err() == nil).
	Scan() bool

	// Err returns an error (if any) encountered while processing rows.
	Err() error

	// Skip, as the name implies, skips the current record in this stream.
	Skip() error

	// Row returns current row (record).
	Row() (interface{}, error)

	// Progress returns a fraction of the input that has been consumed so far.
	Progress() float32
}

// importRowConsumer consumes the data produced by the importRowProducer.
// Implementations of this interface do not need to be thread safe.
type importRowConsumer interface {
	// FillDatums sends row data to the provide datum converter.
	FillDatums(row interface{}, rowNum int64, conv *row.DatumRowConverter) error
}

// batch represents batch of data to convert.
type batch struct {
	data     []interface{}
	startPos int64
	progress float32
}

// parallelImporter is a helper to facilitate running input
// conversion using parallel workers.
type parallelImporter struct {
	b         batch
	batchSize int
	recordCh  chan batch
}

var parallelImporterReaderBatchSize = 500

// TestingSetParallelImporterReaderBatchSize is a testing knob to modify
// csv input reader batch size.
// Returns a function that resets the value back to the default.
func TestingSetParallelImporterReaderBatchSize(s int) func() {
	parallelImporterReaderBatchSize = s
	return func() {
		parallelImporterReaderBatchSize = 500
	}
}

// runParallelImport reads the data produced by 'producer' and sends
// the data to a set of workers responsible for converting this data to the
// appropriate key/values.
func runParallelImport(
	ctx context.Context,
	importCtx *parallelImportContext,
	fileCtx *importFileContext,
	producer importRowProducer,
	consumer importRowConsumer,
) error {
	batchSize := importCtx.batchSize
	if batchSize <= 0 {
		batchSize = parallelImporterReaderBatchSize
	}
	importer := &parallelImporter{
		b: batch{
			data: make([]interface{}, 0, batchSize),
		},
		batchSize: batchSize,
		recordCh:  make(chan batch),
	}

	group := ctxgroup.WithContext(ctx)

	// Start consumers.

	minEmited := make([]int64, importCtx.numWorkers)
	group.GoCtx(func(ctx context.Context) error {
		var span *tracing.Span
		ctx, span = tracing.ChildSpan(ctx, "import-rows-to-datums")
		defer span.Finish()
		if importCtx.numWorkers <= 0 {
			return errors.AssertionFailedf("invalid parallelism: %d", importCtx.numWorkers)
		}
		return ctxgroup.GroupWorkers(ctx, importCtx.numWorkers, func(ctx context.Context, id int) error {
			return importer.importWorker(ctx, id, consumer, importCtx, fileCtx, minEmited)
		})
	})

	// Read data from producer and send it to consumers.
	group.GoCtx(func(ctx context.Context) error {
		defer close(importer.recordCh)
		var span *tracing.Span
		ctx, span = tracing.ChildSpan(ctx, "import-file-to-rows")
		defer span.Finish()
		var numSkipped int64
		var count int64
		for producer.Scan() {
			// Skip rows if needed.
			count++
			if count <= fileCtx.skip {
				if err := producer.Skip(); err != nil {
					return err
				}
				numSkipped++
				continue
			}

			// Stop when we have processed row limit number of rows.
			rowBeingProcessedIdx := count - numSkipped
			if fileCtx.rowLimit != 0 && rowBeingProcessedIdx > fileCtx.rowLimit {
				break
			}

			// Batch parsed data.
			data, err := producer.Row()
			if err != nil {
				if err = handleCorruptRow(ctx, fileCtx, err); err != nil {
					return err
				}
				continue
			}

			if err := importer.add(ctx, data, count, producer.Progress); err != nil {
				return err
			}
		}

		if producer.Err() == nil {
			return importer.close(ctx)
		}
		return producer.Err()
	})

	return group.Wait()
}

// Adds data to the current batch, flushing batches as needed.
func (p *parallelImporter) add(
	ctx context.Context, data interface{}, pos int64, progress func() float32,
) error {
	if len(p.b.data) == 0 {
		p.b.startPos = pos
	}
	p.b.data = append(p.b.data, data)

	if len(p.b.data) == p.batchSize {
		p.b.progress = progress()
		return p.flush(ctx)
	}
	return nil
}

// close closes this importer, flushing remaining accumulated data if needed.
func (p *parallelImporter) close(ctx context.Context) error {
	if len(p.b.data) > 0 {
		return p.flush(ctx)
	}
	return nil
}

// flush flushes currently accumulated data.
func (p *parallelImporter) flush(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case p.recordCh <- p.b:
		p.b = batch{
			data: make([]interface{}, 0, cap(p.b.data)),
		}
		return nil
	}
}

func timestampAfterEpoch(walltime int64) uint64 {
	epoch := time.Date(2015, time.January, 1, 0, 0, 0, 0, time.UTC).UnixNano()
	const precision = uint64(10 * time.Microsecond)
	return uint64(walltime-epoch) / precision
}

func (p *parallelImporter) importWorker(
	ctx context.Context,
	workerID int,
	consumer importRowConsumer,
	importCtx *parallelImportContext,
	fileCtx *importFileContext,
	minEmitted []int64,
) error {
	conv, err := makeDatumConverter(ctx, importCtx, fileCtx)
	if err != nil {
		return err
	}
	if conv.EvalCtx.SessionData() == nil {
		panic("uninitialized session data")
	}

	var rowNum int64
	timestamp := timestampAfterEpoch(importCtx.walltime)

	conv.CompletedRowFn = func() int64 {
		m := emittedRowLowWatermark(workerID, rowNum, minEmitted)
		return m
	}

	for batch := range p.recordCh {
		conv.KvBatch.Progress = batch.progress
		for batchIdx, record := range batch.data {
			rowNum = batch.startPos + int64(batchIdx)
			if err := consumer.FillDatums(record, rowNum, conv); err != nil {
				if err = handleCorruptRow(ctx, fileCtx, err); err != nil {
					return err
				}
				continue
			}

			rowIndex := int64(timestamp) + rowNum
			if err := conv.Row(ctx, conv.KvBatch.Source, rowIndex); err != nil {
				return newImportRowError(err, fmt.Sprintf("%v", record), rowNum)
			}
		}
	}
	return conv.SendBatch(ctx)
}

// Updates emitted row for the specified worker and returns
// low watermark for the emitted rows across all workers.
func emittedRowLowWatermark(workerID int, emittedRow int64, minEmitted []int64) int64 {
	atomic.StoreInt64(&minEmitted[workerID], emittedRow)

	for i := 0; i < len(minEmitted); i++ {
		if i != workerID {
			w := atomic.LoadInt64(&minEmitted[i])
			if w < emittedRow {
				emittedRow = w
			}
		}
	}

	return emittedRow
}
