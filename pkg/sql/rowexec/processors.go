// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rowexec

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/backfill"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// emitHelper is a utility wrapper on top of ProcOutputHelper.EmitRow(). It
// takes a row to emit and, if anything happens other than the normal situation
// where the emitting succeeds and the consumer still needs rows, both the input
// and the output are properly closed after potentially draining the input.
//
// As opposed to EmitRow(), this also supports metadata rows which bypass the
// ProcOutputHelper and are routed directly to its output.
//
// If the consumer signals the producer to drain, the message is relayed and all
// the draining metadata is consumed and forwarded.
//
// Returns true if more rows are needed, false otherwise. If false is returned
// both the inputs and the output have been properly closed, or there is an
// error encountered.
func emitHelper(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	input execinfra.RowSource,
	output execinfra.RowReceiver,
	procOutputHelper *execinfra.ProcOutputHelper,
	row rowenc.EncDatumRow,
	meta *execinfrapb.ProducerMetadata,
) bool {
	if output == nil {
		panic("output RowReceiver is not set for emitting")
	}
	var consumerStatus execinfra.ConsumerStatus
	if meta != nil {
		if row != nil {
			panic("both row data and metadata in the same emitHelper call")
		}
		// Bypass EmitRow() and send directly to output.output.
		foundErr := meta.Err != nil
		consumerStatus = output.Push(nil /* row */, meta)
		if foundErr {
			consumerStatus = execinfra.ConsumerClosed
		}
	} else {
		var err error
		consumerStatus, err = procOutputHelper.EmitRow(ctx, row, output)
		if err != nil {
			output.Push(nil /* row */, &execinfrapb.ProducerMetadata{Err: err})
			consumerStatus = execinfra.ConsumerClosed
		}
	}
	switch consumerStatus {
	case execinfra.NeedMoreRows:
		return true
	case execinfra.SwitchToAnotherPortal:
		output.Push(nil /* row */, &execinfrapb.ProducerMetadata{
			Err: errors.AssertionFailedf("not allowed to pause and switch to another portal"),
		})
		log.Fatalf(ctx, "not allowed to pause and switch to another portal")
		return false
	case execinfra.DrainRequested:
		log.VEventf(ctx, 1, "no more rows required. drain requested.")
		execinfra.DrainAndClose(ctx, flowCtx, input, output, nil /* cause */)
		return false
	case execinfra.ConsumerClosed:
		log.VEventf(ctx, 1, "no more rows required. Consumer shut down.")
		input.ConsumerClosed()
		output.ProducerDone()
		return false
	default:
		log.Fatalf(ctx, "unexpected consumerStatus: %d", consumerStatus)
		return false
	}
}

func checkNumIn(inputs []execinfra.RowSource, numIn int) error {
	if len(inputs) != numIn {
		return errors.AssertionFailedf("expected %d input(s), got %d", numIn, len(inputs))
	}
	return nil
}

// NewProcessor creates a new execinfra.Processor according to the provided
// core.
func NewProcessor(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	core *execinfrapb.ProcessorCoreUnion,
	post *execinfrapb.PostProcessSpec,
	inputs []execinfra.RowSource,
	localProcessors []execinfra.LocalProcessor,
) (execinfra.Processor, error) {
	if core.Noop != nil {
		if err := checkNumIn(inputs, 1); err != nil {
			return nil, err
		}
		return newNoopProcessor(ctx, flowCtx, processorID, inputs[0], post)
	}
	if core.Values != nil {
		if err := checkNumIn(inputs, 0); err != nil {
			return nil, err
		}
		return newValuesProcessor(ctx, flowCtx, processorID, core.Values, post)
	}
	if core.TableReader != nil {
		if err := checkNumIn(inputs, 0); err != nil {
			return nil, err
		}
		return newTableReader(ctx, flowCtx, processorID, core.TableReader, post)
	}
	if core.Filterer != nil {
		if err := checkNumIn(inputs, 1); err != nil {
			return nil, err
		}
		return newFiltererProcessor(ctx, flowCtx, processorID, core.Filterer, inputs[0], post)
	}
	if core.JoinReader != nil {
		if err := checkNumIn(inputs, 1); err != nil {
			return nil, err
		}
		if core.JoinReader.IsIndexJoin() {
			return newJoinReader(
				ctx, flowCtx, processorID, core.JoinReader, inputs[0], post, indexJoinReaderType,
			)
		}
		return newJoinReader(ctx, flowCtx, processorID, core.JoinReader, inputs[0], post, lookupJoinReaderType)
	}
	if core.Sorter != nil {
		if err := checkNumIn(inputs, 1); err != nil {
			return nil, err
		}
		return newSorter(ctx, flowCtx, processorID, core.Sorter, inputs[0], post)
	}
	if core.Distinct != nil {
		if err := checkNumIn(inputs, 1); err != nil {
			return nil, err
		}
		return newDistinct(ctx, flowCtx, processorID, core.Distinct, inputs[0], post)
	}
	if core.Ordinality != nil {
		if err := checkNumIn(inputs, 1); err != nil {
			return nil, err
		}
		return newOrdinalityProcessor(ctx, flowCtx, processorID, core.Ordinality, inputs[0], post)
	}
	if core.Aggregator != nil {
		if err := checkNumIn(inputs, 1); err != nil {
			return nil, err
		}
		return newAggregator(ctx, flowCtx, processorID, core.Aggregator, inputs[0], post)
	}
	if core.MergeJoiner != nil {
		if err := checkNumIn(inputs, 2); err != nil {
			return nil, err
		}
		return newMergeJoiner(
			ctx, flowCtx, processorID, core.MergeJoiner, inputs[0], inputs[1], post,
		)
	}
	if core.ZigzagJoiner != nil {
		if err := checkNumIn(inputs, 0); err != nil {
			return nil, err
		}
		return newZigzagJoiner(
			ctx, flowCtx, processorID, core.ZigzagJoiner, nil, post,
		)
	}
	if core.HashJoiner != nil {
		if err := checkNumIn(inputs, 2); err != nil {
			return nil, err
		}
		return newHashJoiner(
			ctx, flowCtx, processorID, core.HashJoiner, inputs[0], inputs[1], post,
		)
	}
	if core.InvertedJoiner != nil {
		if err := checkNumIn(inputs, 1); err != nil {
			return nil, err
		}
		return newInvertedJoiner(
			ctx, flowCtx, processorID, core.InvertedJoiner, nil, inputs[0], post,
		)
	}
	if core.Backfiller != nil {
		if err := checkNumIn(inputs, 0); err != nil {
			return nil, err
		}
		switch core.Backfiller.Type {
		case execinfrapb.BackfillerSpec_Index:
			return newIndexBackfiller(ctx, flowCtx, processorID, *core.Backfiller)
		case execinfrapb.BackfillerSpec_Column:
			return newColumnBackfiller(ctx, flowCtx, processorID, *core.Backfiller)
		}
	}
	if core.Sampler != nil {
		if err := checkNumIn(inputs, 1); err != nil {
			return nil, err
		}
		return newSamplerProcessor(ctx, flowCtx, processorID, core.Sampler, inputs[0], post)
	}
	if core.SampleAggregator != nil {
		if err := checkNumIn(inputs, 1); err != nil {
			return nil, err
		}
		return newSampleAggregator(ctx, flowCtx, processorID, core.SampleAggregator, inputs[0], post)
	}
	if core.ReadImport != nil {
		if err := checkNumIn(inputs, 0); err != nil {
			return nil, err
		}
		if NewReadImportDataProcessor == nil {
			return nil, errors.New("ReadImportData processor unimplemented")
		}
		return NewReadImportDataProcessor(ctx, flowCtx, processorID, *core.ReadImport, post)
	}
	if core.CloudStorageTest != nil {
		if err := checkNumIn(inputs, 0); err != nil {
			return nil, err
		}
		if NewCloudStorageTestProcessor == nil {
			return nil, errors.New("CloudStorageTestProcessor processor unimplemented")
		}
		return NewCloudStorageTestProcessor(ctx, flowCtx, processorID, *core.CloudStorageTest, post)
	}
	if core.IngestStopped != nil {
		if err := checkNumIn(inputs, 0); err != nil {
			return nil, err
		}
		if NewIngestStoppedProcessor == nil {
			return nil, errors.New("IsIngestingImport processor unimplemented")
		}
		return NewIngestStoppedProcessor(ctx, flowCtx, processorID, *core.IngestStopped, post)
	}
	if core.BackupData != nil {
		if err := checkNumIn(inputs, 0); err != nil {
			return nil, err
		}
		if NewBackupDataProcessor == nil {
			return nil, errors.New("BackupData processor unimplemented")
		}
		return NewBackupDataProcessor(ctx, flowCtx, processorID, *core.BackupData, post)
	}
	if core.RestoreData != nil {
		if err := checkNumIn(inputs, 1); err != nil {
			return nil, err
		}
		if NewRestoreDataProcessor == nil {
			return nil, errors.New("RestoreData processor unimplemented")
		}
		return NewRestoreDataProcessor(ctx, flowCtx, processorID, *core.RestoreData, post, inputs[0])
	}
	if core.StreamIngestionData != nil {
		if err := checkNumIn(inputs, 0); err != nil {
			return nil, err
		}
		if NewStreamIngestionDataProcessor == nil {
			return nil, errors.New("StreamIngestionData processor unimplemented")
		}
		return NewStreamIngestionDataProcessor(ctx, flowCtx, processorID, *core.StreamIngestionData, post)
	}
	if core.Exporter != nil {
		if err := checkNumIn(inputs, 1); err != nil {
			return nil, err
		}

		if core.Exporter.Format.Format == roachpb.IOFileFormat_Parquet {
			return NewParquetWriterProcessor(ctx, flowCtx, processorID, *core.Exporter, post, inputs[0])
		}
		return NewCSVWriterProcessor(ctx, flowCtx, processorID, *core.Exporter, post, inputs[0])
	}

	if core.BulkRowWriter != nil {
		if err := checkNumIn(inputs, 1); err != nil {
			return nil, err
		}
		return newBulkRowWriterProcessor(ctx, flowCtx, processorID, *core.BulkRowWriter, inputs[0])
	}
	if core.ProjectSet != nil {
		if err := checkNumIn(inputs, 1); err != nil {
			return nil, err
		}
		return newProjectSetProcessor(ctx, flowCtx, processorID, core.ProjectSet, inputs[0], post)
	}
	if core.Windower != nil {
		if err := checkNumIn(inputs, 1); err != nil {
			return nil, err
		}
		return newWindower(ctx, flowCtx, processorID, core.Windower, inputs[0], post)
	}
	if core.VectorSearch != nil {
		if err := checkNumIn(inputs, 0); err != nil {
			return nil, err
		}
		return newVectorSearchProcessor(ctx, flowCtx, processorID, core.VectorSearch, post)
	}
	if core.VectorMutationSearch != nil {
		if err := checkNumIn(inputs, 1); err != nil {
			return nil, err
		}
		return newVectorMutationSearchProcessor(
			ctx, flowCtx, processorID, core.VectorMutationSearch, inputs[0], post,
		)
	}
	if core.LocalPlanNode != nil {
		numInputs := int(core.LocalPlanNode.NumInputs)
		if err := checkNumIn(inputs, numInputs); err != nil {
			return nil, err
		}
		processor := localProcessors[core.LocalPlanNode.RowSourceIdx]
		if err := processor.Init(ctx, flowCtx, processorID, post); err != nil {
			return nil, err
		}
		if numInputs == 1 {
			if err := processor.SetInput(ctx, inputs[0]); err != nil {
				return nil, err
			}
		} else if numInputs > 1 {
			return nil, errors.AssertionFailedf("invalid localPlanNode core with multiple inputs %+v", core.LocalPlanNode)
		}
		return processor, nil
	}
	if core.ChangeAggregator != nil {
		if err := checkNumIn(inputs, 0); err != nil {
			return nil, err
		}
		if NewChangeAggregatorProcessor == nil {
			return nil, errors.New("ChangeAggregator processor unimplemented")
		}
		return NewChangeAggregatorProcessor(ctx, flowCtx, processorID, *core.ChangeAggregator, post)
	}
	if core.ChangeFrontier != nil {
		if err := checkNumIn(inputs, 1); err != nil {
			return nil, err
		}
		if NewChangeFrontierProcessor == nil {
			return nil, errors.New("ChangeFrontier processor unimplemented")
		}
		return NewChangeFrontierProcessor(ctx, flowCtx, processorID, *core.ChangeFrontier, inputs[0], post)
	}
	if core.InvertedFilterer != nil {
		if err := checkNumIn(inputs, 1); err != nil {
			return nil, err
		}
		return newInvertedFilterer(ctx, flowCtx, processorID, core.InvertedFilterer, inputs[0], post)
	}
	if core.StreamIngestionFrontier != nil {
		if err := checkNumIn(inputs, 1); err != nil {
			return nil, err
		}
		if NewStreamIngestionFrontierProcessor == nil {
			return nil, errors.New("StreamIngestionFrontier processor unimplemented")
		}
		return NewStreamIngestionFrontierProcessor(ctx, flowCtx, processorID, *core.StreamIngestionFrontier, inputs[0], post)
	}
	if core.IndexBackfillMerger != nil {
		if err := checkNumIn(inputs, 0); err != nil {
			return nil, err
		}
		return backfill.NewIndexBackfillMerger(flowCtx, processorID, *core.IndexBackfillMerger), nil
	}
	if core.Ttl != nil {
		if err := checkNumIn(inputs, 0); err != nil {
			return nil, err
		}
		return NewTTLProcessor(ctx, flowCtx, processorID, *core.Ttl)
	}
	if core.LogicalReplicationWriter != nil {
		if err := checkNumIn(inputs, 0); err != nil {
			return nil, err
		}
		return NewLogicalReplicationWriterProcessor(ctx, flowCtx, processorID, *core.LogicalReplicationWriter, post)
	}
	if core.LogicalReplicationOfflineScan != nil {
		if err := checkNumIn(inputs, 0); err != nil {
			return nil, err
		}
		return NewLogicalReplicationOfflineScanProcessor(ctx, flowCtx, processorID, *core.LogicalReplicationOfflineScan, post)
	}
	if core.HashGroupJoiner != nil {
		if err := checkNumIn(inputs, 2); err != nil {
			return nil, err
		}
		return newHashGroupJoiner(ctx, flowCtx, processorID, core.HashGroupJoiner, inputs[0], inputs[1], post)
	}
	if core.GenerativeSplitAndScatter != nil {
		if err := checkNumIn(inputs, 0); err != nil {
			return nil, err
		}
		if NewGenerativeSplitAndScatterProcessor == nil {
			return nil, errors.New("GenerativeSplitAndScatter processor unimplemented")
		}
		return NewGenerativeSplitAndScatterProcessor(ctx, flowCtx, processorID, *core.GenerativeSplitAndScatter, post)
	}
	return nil, errors.Errorf("unsupported processor core %q", core)
}

// NewReadImportDataProcessor is implemented in the non-free (CCL) codebase and then injected here via runtime initialization.
var NewReadImportDataProcessor func(context.Context, *execinfra.FlowCtx, int32, execinfrapb.ReadImportDataSpec, *execinfrapb.PostProcessSpec) (execinfra.Processor, error)

// NewCloudStorageTestProcessor is implemented in the non-free (CCL) codebase and then injected here via runtime initialization.
var NewCloudStorageTestProcessor func(context.Context, *execinfra.FlowCtx, int32, execinfrapb.CloudStorageTestSpec, *execinfrapb.PostProcessSpec) (execinfra.Processor, error)

// NewIngestStoppedProcessor is implemented in the non-free (CCL) codebase and then injected here via runtime initialization.
var NewIngestStoppedProcessor func(context.Context, *execinfra.FlowCtx, int32, execinfrapb.IngestStoppedSpec, *execinfrapb.PostProcessSpec) (execinfra.Processor, error)

// NewBackupDataProcessor is implemented in the non-free (CCL) codebase and then injected here via runtime initialization.
var NewBackupDataProcessor func(context.Context, *execinfra.FlowCtx, int32, execinfrapb.BackupDataSpec, *execinfrapb.PostProcessSpec) (execinfra.Processor, error)

// NewRestoreDataProcessor is implemented in the non-free (CCL) codebase and then injected here via runtime initialization.
var NewRestoreDataProcessor func(context.Context, *execinfra.FlowCtx, int32, execinfrapb.RestoreDataSpec, *execinfrapb.PostProcessSpec, execinfra.RowSource) (execinfra.Processor, error)

// NewStreamIngestionDataProcessor is implemented in the non-free (CCL) codebase and then injected here via runtime initialization.
var NewStreamIngestionDataProcessor func(context.Context, *execinfra.FlowCtx, int32, execinfrapb.StreamIngestionDataSpec, *execinfrapb.PostProcessSpec) (execinfra.Processor, error)

// NewCSVWriterProcessor is implemented in the non-free (CCL) codebase and then injected here via runtime initialization.
var NewCSVWriterProcessor func(context.Context, *execinfra.FlowCtx, int32, execinfrapb.ExportSpec, *execinfrapb.PostProcessSpec, execinfra.RowSource) (execinfra.Processor, error)

// NewParquetWriterProcessor is implemented in the non-free (CCL) codebase and then injected here via runtime initialization.
var NewParquetWriterProcessor func(context.Context, *execinfra.FlowCtx, int32, execinfrapb.ExportSpec, *execinfrapb.PostProcessSpec, execinfra.RowSource) (execinfra.Processor, error)

// NewChangeAggregatorProcessor is implemented in the non-free (CCL) codebase and then injected here via runtime initialization.
var NewChangeAggregatorProcessor func(context.Context, *execinfra.FlowCtx, int32, execinfrapb.ChangeAggregatorSpec, *execinfrapb.PostProcessSpec) (execinfra.Processor, error)

// NewChangeFrontierProcessor is implemented in the non-free (CCL) codebase and then injected here via runtime initialization.
var NewChangeFrontierProcessor func(context.Context, *execinfra.FlowCtx, int32, execinfrapb.ChangeFrontierSpec, execinfra.RowSource, *execinfrapb.PostProcessSpec) (execinfra.Processor, error)

// NewStreamIngestionFrontierProcessor is implemented in the non-free (CCL) codebase and then injected here via runtime initialization.
var NewStreamIngestionFrontierProcessor func(context.Context, *execinfra.FlowCtx, int32, execinfrapb.StreamIngestionFrontierSpec, execinfra.RowSource, *execinfrapb.PostProcessSpec) (execinfra.Processor, error)

// NewTTLProcessor is implemented in the non-free (CCL) codebase and then injected here via runtime initialization.
var NewTTLProcessor func(context.Context, *execinfra.FlowCtx, int32, execinfrapb.TTLSpec) (execinfra.Processor, error)

// NewGenerativeSplitAndScatterProcessor is implemented in the non-free (CCL) codebase and then injected here via runtime initialization.
var NewGenerativeSplitAndScatterProcessor func(context.Context, *execinfra.FlowCtx, int32, execinfrapb.GenerativeSplitAndScatterSpec, *execinfrapb.PostProcessSpec) (execinfra.Processor, error)

var NewLogicalReplicationWriterProcessor func(context.Context, *execinfra.FlowCtx, int32, execinfrapb.LogicalReplicationWriterSpec, *execinfrapb.PostProcessSpec) (execinfra.Processor, error)

var NewLogicalReplicationOfflineScanProcessor func(context.Context, *execinfra.FlowCtx, int32, execinfrapb.LogicalReplicationOfflineScanSpec, *execinfrapb.PostProcessSpec) (execinfra.Processor, error)
