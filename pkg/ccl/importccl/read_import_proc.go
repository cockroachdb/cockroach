// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package importccl

import (
	"context"
	"io"
	"math/rand"
	"sync"

	"golang.org/x/sync/errgroup"

	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlrun"
	"github.com/cockroachdb/cockroach/pkg/sql/jobs"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/pkg/errors"
)

type readFileFunc func(context.Context, io.Reader, int32, string, func(finished bool) error) error

type ctxProvider struct {
	context.Context
}

func (c ctxProvider) Ctx() context.Context {
	return c
}

// groupWorkers creates num worker go routines in an error group.
func groupWorkers(ctx context.Context, num int, f func(context.Context) error) error {
	group, ctx := errgroup.WithContext(ctx)
	for i := 0; i < num; i++ {
		group.Go(func() error {
			return f(ctx)
		})
	}
	return group.Wait()
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
		bc := byteCounter{r: f}

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

		if err := fileFunc(ctx, &bc, dataFileIndex, dataFile, wrappedProgressFn); err != nil {
			return errors.Wrap(err, dataFile)
		}
		if updateFromFiles {
			if err := progressFn(float32(currentFile) / float32(len(dataFiles))); err != nil {
				return err
			}
		}
	}
	return nil
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

var csvOutputTypes = []sqlbase.ColumnType{
	{SemanticType: sqlbase.ColumnType_BYTES},
	{SemanticType: sqlbase.ColumnType_BYTES},
}

func newReadImportDataProcessor(
	flowCtx *distsqlrun.FlowCtx, spec distsqlrun.ReadImportDataSpec, output distsqlrun.RowReceiver,
) (distsqlrun.Processor, error) {
	cp := &readImportDataProcessor{
		flowCtx:     flowCtx,
		inputFromat: spec.Format,
		sampleSize:  spec.SampleSize,
		tableDesc:   spec.TableDesc,
		uri:         spec.Uri,
		output:      output,
		settings:    flowCtx.Settings,
		registry:    flowCtx.JobRegistry,
		progress:    spec.Progress,
	}

	// Check if this was was sent by an older node.
	if spec.Format.Format == roachpb.IOFileFormat_Unknown {
		spec.Format.Format = roachpb.IOFileFormat_CSV
		spec.Format.Csv = spec.LegacyCsvOptions
	}
	if err := cp.out.Init(&distsqlrun.PostProcessSpec{}, csvOutputTypes, flowCtx.NewEvalCtx(), output); err != nil {
		return nil, err
	}
	return cp, nil
}

type readImportDataProcessor struct {
	flowCtx     *distsqlrun.FlowCtx
	sampleSize  int32
	tableDesc   sqlbase.TableDescriptor
	uri         map[int32]string
	inputFromat roachpb.IOFileFormat
	out         distsqlrun.ProcOutputHelper
	output      distsqlrun.RowReceiver
	settings    *cluster.Settings
	registry    *jobs.Registry
	progress    distsqlrun.JobProgress
}

var _ distsqlrun.Processor = &readImportDataProcessor{}

func (cp *readImportDataProcessor) OutputTypes() []sqlbase.ColumnType {
	return csvOutputTypes
}

func (cp *readImportDataProcessor) Run(ctx context.Context, wg *sync.WaitGroup) {
	ctx, span := tracing.ChildSpan(ctx, "readImportDataProcessor")
	defer tracing.FinishSpan(span)

	if wg != nil {
		defer wg.Done()
	}

	group, gCtx := errgroup.WithContext(ctx)
	done := gCtx.Done()
	kvCh := make(chan kvBatch)
	sampleCh := make(chan sqlbase.EncDatumRow)

	c := newCSVInputReader(ctx, cp.inputFromat.Csv, &cp.tableDesc, len(cp.tableDesc.VisibleColumns()))
	c.start(ctx, group, kvCh)

	// Read input files into kvs
	group.Go(func() error {
		sCtx, span := tracing.ChildSpan(gCtx, "readImportFiles")
		defer tracing.FinishSpan(span)
		defer c.inputFinished()

		job, err := cp.registry.LoadJob(sCtx, cp.progress.JobID)
		if err != nil {
			return err
		}

		progFn := func(pct float32) error {
			return job.Progressed(sCtx, func(ctx context.Context, details jobs.Details) float32 {
				d := details.(*jobs.Payload_Import).Import
				slotpct := pct * cp.progress.Contribution
				if len(d.Tables[0].SamplingProgress) > 0 {
					d.Tables[0].SamplingProgress[cp.progress.Slot] = slotpct
				} else {
					d.Tables[0].ReadProgress[cp.progress.Slot] = slotpct
				}
				return d.Tables[0].Completed()
			})
		}

		return readInputFiles(sCtx, cp.uri, c.readFile, progFn, cp.settings)
	})

	// Sample KVs
	group.Go(func() error {
		sCtx, span := tracing.ChildSpan(gCtx, "sampleImportKVs")
		defer tracing.FinishSpan(span)

		defer close(sampleCh)
		var fn sampleFunc
		if cp.sampleSize == 0 {
			fn = sampleAll
		} else {
			sr := sampleRate{
				rnd:        rand.New(rand.NewSource(rand.Int63())),
				sampleSize: float64(cp.sampleSize),
			}
			fn = sr.sample
		}

		// Populate the split-point spans which have already been imported.
		var completedSpans roachpb.SpanGroup
		job, err := cp.registry.LoadJob(gCtx, cp.progress.JobID)
		if err != nil {
			return err
		}
		if details, ok := job.Payload().Details.(*jobs.Payload_Import); ok {
			completedSpans.Add(details.Import.Tables[0].SpanProgress...)
		}

		typeBytes := sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_BYTES}
		for kvBatch := range kvCh {
			for _, kv := range kvBatch {
				// Allow KV pairs to be dropped if they belong to a completed span.
				if completedSpans.Contains(kv.Key) {
					continue
				}
				if fn(kv) {
					row := sqlbase.EncDatumRow{
						sqlbase.DatumToEncDatum(typeBytes, tree.NewDBytes(tree.DBytes(kv.Key))),
						sqlbase.DatumToEncDatum(typeBytes, tree.NewDBytes(tree.DBytes(kv.Value.RawBytes))),
					}
					select {
					case <-done:
						return sCtx.Err()
					case sampleCh <- row:
					}
				}
			}
		}
		return nil
	})
	// Send sampled KVs to dist sql
	group.Go(func() error {
		sCtx, span := tracing.ChildSpan(gCtx, "sendImportKVs")
		defer tracing.FinishSpan(span)

		for row := range sampleCh {
			cs, err := cp.out.EmitRow(sCtx, row)
			if err != nil {
				return err
			}
			if cs != distsqlrun.NeedMoreRows {
				return errors.New("unexpected closure of consumer")
			}
		}
		return nil
	})
	if err := group.Wait(); err != nil {
		distsqlrun.DrainAndClose(ctx, cp.output, err, func(context.Context) {} /* pushTrailingMeta */)
		return
	}

	cp.out.Close()
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

func sampleAll(kv roachpb.KeyValue) bool {
	return true
}

func init() {
	distsqlrun.NewReadImportDataProcessor = newReadImportDataProcessor
}
