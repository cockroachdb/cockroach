// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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

// emitHelper is a utility wrapper on top of ProcOutputHelper.EmitRow().
// It takes a row to emit and, if anything happens other than the normal
// situation where the emitting succeeds and the consumer still needs rows, both
// the (potentially many) inputs and the output are properly closed after
// potentially draining the inputs. It's allowed to not pass any inputs, in
// which case nothing will be drained (this can happen when the caller has
// already fully consumed the inputs).
//
// As opposed to EmitRow(), this also supports metadata rows which bypass the
// ProcOutputHelper and are routed directly to its output.
//
// If the consumer signals the producer to drain, the message is relayed and all
// the draining metadata is consumed and forwarded.
//
// inputs are optional.
//
// pushTrailingMeta is called after draining the sources and before calling
// dst.ProducerDone(). It gives the caller the opportunity to push some trailing
// metadata (e.g. tracing information and txn updates, if applicable).
//
// Returns true if more rows are needed, false otherwise. If false is returned
// both the inputs and the output have been properly closed.
func emitHelper(
	ctx context.Context,
	output execinfra.RowReceiver,
	procOutputHelper *execinfra.ProcOutputHelper,
	row rowenc.EncDatumRow,
	meta *execinfrapb.ProducerMetadata,
	pushTrailingMeta func(context.Context),
	inputs ...execinfra.RowSource,
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
	case execinfra.DrainRequested:
		log.VEventf(ctx, 1, "no more rows required. drain requested.")
		execinfra.DrainAndClose(ctx, output, nil /* cause */, pushTrailingMeta, inputs...)
		return false
	case execinfra.ConsumerClosed:
		log.VEventf(ctx, 1, "no more rows required. Consumer shut down.")
		for _, input := range inputs {
			input.ConsumerClosed()
		}
		output.ProducerDone()
		return false
	default:
		log.Fatalf(ctx, "unexpected consumerStatus: %d", consumerStatus)
		return false
	}
}

func checkNumInOut(
	inputs []execinfra.RowSource, outputs []execinfra.RowReceiver, numIn, numOut int,
) error {
	if len(inputs) != numIn {
		return errors.Errorf("expected %d input(s), got %d", numIn, len(inputs))
	}
	if len(outputs) != numOut {
		return errors.Errorf("expected %d output(s), got %d", numOut, len(outputs))
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
	outputs []execinfra.RowReceiver,
	localProcessors []execinfra.LocalProcessor,
) (execinfra.Processor, error) {
	if core.Noop != nil {
		if err := checkNumInOut(inputs, outputs, 1, 1); err != nil {
			return nil, err
		}
		return newNoopProcessor(flowCtx, processorID, inputs[0], post, outputs[0])
	}
	if core.Values != nil {
		if err := checkNumInOut(inputs, outputs, 0, 1); err != nil {
			return nil, err
		}
		return newValuesProcessor(flowCtx, processorID, core.Values, post, outputs[0])
	}
	if core.TableReader != nil {
		if err := checkNumInOut(inputs, outputs, 0, 1); err != nil {
			return nil, err
		}
		return newTableReader(flowCtx, processorID, core.TableReader, post, outputs[0])
	}
	if core.Filterer != nil {
		if err := checkNumInOut(inputs, outputs, 1, 1); err != nil {
			return nil, err
		}
		return newFiltererProcessor(flowCtx, processorID, core.Filterer, inputs[0], post, outputs[0])
	}
	if core.JoinReader != nil {
		if err := checkNumInOut(inputs, outputs, 1, 1); err != nil {
			return nil, err
		}
		if core.JoinReader.IsIndexJoin() {
			return newJoinReader(
				flowCtx, processorID, core.JoinReader, inputs[0], post, outputs[0], indexJoinReaderType)
		}
		return newJoinReader(flowCtx, processorID, core.JoinReader, inputs[0], post, outputs[0], lookupJoinReaderType)
	}
	if core.Sorter != nil {
		if err := checkNumInOut(inputs, outputs, 1, 1); err != nil {
			return nil, err
		}
		return newSorter(ctx, flowCtx, processorID, core.Sorter, inputs[0], post, outputs[0])
	}
	if core.Distinct != nil {
		if err := checkNumInOut(inputs, outputs, 1, 1); err != nil {
			return nil, err
		}
		return newDistinct(flowCtx, processorID, core.Distinct, inputs[0], post, outputs[0])
	}
	if core.Ordinality != nil {
		if err := checkNumInOut(inputs, outputs, 1, 1); err != nil {
			return nil, err
		}
		return newOrdinalityProcessor(flowCtx, processorID, core.Ordinality, inputs[0], post, outputs[0])
	}
	if core.Aggregator != nil {
		if err := checkNumInOut(inputs, outputs, 1, 1); err != nil {
			return nil, err
		}
		return newAggregator(flowCtx, processorID, core.Aggregator, inputs[0], post, outputs[0])
	}
	if core.MergeJoiner != nil {
		if err := checkNumInOut(inputs, outputs, 2, 1); err != nil {
			return nil, err
		}
		return newMergeJoiner(
			flowCtx, processorID, core.MergeJoiner, inputs[0], inputs[1], post, outputs[0],
		)
	}
	if core.ZigzagJoiner != nil {
		if err := checkNumInOut(inputs, outputs, 0, 1); err != nil {
			return nil, err
		}
		return newZigzagJoiner(
			flowCtx, processorID, core.ZigzagJoiner, nil, post, outputs[0],
		)
	}
	if core.HashJoiner != nil {
		if err := checkNumInOut(inputs, outputs, 2, 1); err != nil {
			return nil, err
		}
		return newHashJoiner(
			flowCtx, processorID, core.HashJoiner, inputs[0], inputs[1], post, outputs[0],
		)
	}
	if core.InvertedJoiner != nil {
		if err := checkNumInOut(inputs, outputs, 1, 1); err != nil {
			return nil, err
		}
		return newInvertedJoiner(
			flowCtx, processorID, core.InvertedJoiner, nil, inputs[0], post, outputs[0])
	}
	if core.Backfiller != nil {
		if err := checkNumInOut(inputs, outputs, 0, 1); err != nil {
			return nil, err
		}
		switch core.Backfiller.Type {
		case execinfrapb.BackfillerSpec_Index:
			return newIndexBackfiller(ctx, flowCtx, processorID, *core.Backfiller, post, outputs[0])
		case execinfrapb.BackfillerSpec_Column:
			return newColumnBackfiller(ctx, flowCtx, processorID, *core.Backfiller, post, outputs[0])
		}
	}
	if core.Sampler != nil {
		if err := checkNumInOut(inputs, outputs, 1, 1); err != nil {
			return nil, err
		}
		return newSamplerProcessor(flowCtx, processorID, core.Sampler, inputs[0], post, outputs[0])
	}
	if core.SampleAggregator != nil {
		if err := checkNumInOut(inputs, outputs, 1, 1); err != nil {
			return nil, err
		}
		return newSampleAggregator(flowCtx, processorID, core.SampleAggregator, inputs[0], post, outputs[0])
	}
	if core.ReadImport != nil {
		if err := checkNumInOut(inputs, outputs, 0, 1); err != nil {
			return nil, err
		}
		if NewReadImportDataProcessor == nil {
			return nil, errors.New("ReadImportData processor unimplemented")
		}
		return NewReadImportDataProcessor(flowCtx, processorID, *core.ReadImport, post, outputs[0])
	}
	if core.BackupData != nil {
		if err := checkNumInOut(inputs, outputs, 0, 1); err != nil {
			return nil, err
		}
		if NewBackupDataProcessor == nil {
			return nil, errors.New("BackupData processor unimplemented")
		}
		return NewBackupDataProcessor(flowCtx, processorID, *core.BackupData, post, outputs[0])
	}
	if core.SplitAndScatter != nil {
		if err := checkNumInOut(inputs, outputs, 0, 1); err != nil {
			return nil, err
		}
		if NewSplitAndScatterProcessor == nil {
			return nil, errors.New("SplitAndScatter processor unimplemented")
		}
		return NewSplitAndScatterProcessor(flowCtx, processorID, *core.SplitAndScatter, post, outputs[0])
	}
	if core.RestoreData != nil {
		if err := checkNumInOut(inputs, outputs, 1, 1); err != nil {
			return nil, err
		}
		if NewRestoreDataProcessor == nil {
			return nil, errors.New("RestoreData processor unimplemented")
		}
		return NewRestoreDataProcessor(flowCtx, processorID, *core.RestoreData, post, inputs[0], outputs[0])
	}
	if core.StreamIngestionData != nil {
		if err := checkNumInOut(inputs, outputs, 0, 1); err != nil {
			return nil, err
		}
		if NewStreamIngestionDataProcessor == nil {
			return nil, errors.New("StreamIngestionData processor unimplemented")
		}
		return NewStreamIngestionDataProcessor(flowCtx, processorID, *core.StreamIngestionData, post, outputs[0])
	}
	if core.Exporter != nil {
		if err := checkNumInOut(inputs, outputs, 1, 1); err != nil {
			return nil, err
		}

		if core.Exporter.Format.Format == roachpb.IOFileFormat_Parquet {
			return NewParquetWriterProcessor(flowCtx, processorID, *core.Exporter, inputs[0], outputs[0])
		}
		return NewCSVWriterProcessor(flowCtx, processorID, *core.Exporter, inputs[0], outputs[0])
	}

	if core.BulkRowWriter != nil {
		if err := checkNumInOut(inputs, outputs, 1, 1); err != nil {
			return nil, err
		}
		return newBulkRowWriterProcessor(flowCtx, processorID, *core.BulkRowWriter, inputs[0], outputs[0])
	}
	if core.MetadataTestSender != nil {
		if err := checkNumInOut(inputs, outputs, 1, 1); err != nil {
			return nil, err
		}
		return execinfra.NewMetadataTestSender(flowCtx, processorID, inputs[0], post, outputs[0], core.MetadataTestSender.ID)
	}
	if core.MetadataTestReceiver != nil {
		if err := checkNumInOut(inputs, outputs, 1, 1); err != nil {
			return nil, err
		}
		return execinfra.NewMetadataTestReceiver(
			flowCtx, processorID, inputs[0], post, outputs[0], core.MetadataTestReceiver.SenderIDs,
		)
	}
	if core.ProjectSet != nil {
		if err := checkNumInOut(inputs, outputs, 1, 1); err != nil {
			return nil, err
		}
		return newProjectSetProcessor(flowCtx, processorID, core.ProjectSet, inputs[0], post, outputs[0])
	}
	if core.Windower != nil {
		if err := checkNumInOut(inputs, outputs, 1, 1); err != nil {
			return nil, err
		}
		return newWindower(flowCtx, processorID, core.Windower, inputs[0], post, outputs[0])
	}
	if core.LocalPlanNode != nil {
		numInputs := int(core.LocalPlanNode.NumInputs)
		if err := checkNumInOut(inputs, outputs, numInputs, 1); err != nil {
			return nil, err
		}
		processor := localProcessors[core.LocalPlanNode.RowSourceIdx]
		if err := processor.InitWithOutput(flowCtx, post, outputs[0]); err != nil {
			return nil, err
		}
		if numInputs == 1 {
			if err := processor.SetInput(ctx, inputs[0]); err != nil {
				return nil, err
			}
		} else if numInputs > 1 {
			return nil, errors.Errorf("invalid localPlanNode core with multiple inputs %+v", core.LocalPlanNode)
		}
		return processor, nil
	}
	if core.ChangeAggregator != nil {
		if err := checkNumInOut(inputs, outputs, 0, 1); err != nil {
			return nil, err
		}
		if NewChangeAggregatorProcessor == nil {
			return nil, errors.New("ChangeAggregator processor unimplemented")
		}
		return NewChangeAggregatorProcessor(flowCtx, processorID, *core.ChangeAggregator, post, outputs[0])
	}
	if core.ChangeFrontier != nil {
		if err := checkNumInOut(inputs, outputs, 1, 1); err != nil {
			return nil, err
		}
		if NewChangeFrontierProcessor == nil {
			return nil, errors.New("ChangeFrontier processor unimplemented")
		}
		return NewChangeFrontierProcessor(flowCtx, processorID, *core.ChangeFrontier, inputs[0], post, outputs[0])
	}
	if core.InvertedFilterer != nil {
		if err := checkNumInOut(inputs, outputs, 1, 1); err != nil {
			return nil, err
		}
		return newInvertedFilterer(flowCtx, processorID, core.InvertedFilterer, inputs[0], post, outputs[0])
	}
	if core.StreamIngestionFrontier != nil {
		if err := checkNumInOut(inputs, outputs, 1, 1); err != nil {
			return nil, err
		}
		return NewStreamIngestionFrontierProcessor(flowCtx, processorID, *core.StreamIngestionFrontier, inputs[0], post, outputs[0])
	}
	if core.IndexBackfillMerger != nil {
		if err := checkNumInOut(inputs, outputs, 0, 1); err != nil {
			return nil, err
		}
		return backfill.NewIndexBackfillMerger(ctx, flowCtx, *core.IndexBackfillMerger, outputs[0])
	}
	return nil, errors.Errorf("unsupported processor core %q", core)
}

// NewReadImportDataProcessor is implemented in the non-free (CCL) codebase and then injected here via runtime initialization.
var NewReadImportDataProcessor func(*execinfra.FlowCtx, int32, execinfrapb.ReadImportDataSpec, *execinfrapb.PostProcessSpec, execinfra.RowReceiver) (execinfra.Processor, error)

// NewBackupDataProcessor is implemented in the non-free (CCL) codebase and then injected here via runtime initialization.
var NewBackupDataProcessor func(*execinfra.FlowCtx, int32, execinfrapb.BackupDataSpec, *execinfrapb.PostProcessSpec, execinfra.RowReceiver) (execinfra.Processor, error)

// NewSplitAndScatterProcessor is implemented in the non-free (CCL) codebase and then injected here via runtime initialization.
var NewSplitAndScatterProcessor func(*execinfra.FlowCtx, int32, execinfrapb.SplitAndScatterSpec, *execinfrapb.PostProcessSpec, execinfra.RowReceiver) (execinfra.Processor, error)

// NewRestoreDataProcessor is implemented in the non-free (CCL) codebase and then injected here via runtime initialization.
var NewRestoreDataProcessor func(*execinfra.FlowCtx, int32, execinfrapb.RestoreDataSpec, *execinfrapb.PostProcessSpec, execinfra.RowSource, execinfra.RowReceiver) (execinfra.Processor, error)

// NewStreamIngestionDataProcessor is implemented in the non-free (CCL) codebase and then injected here via runtime initialization.
var NewStreamIngestionDataProcessor func(*execinfra.FlowCtx, int32, execinfrapb.StreamIngestionDataSpec, *execinfrapb.PostProcessSpec, execinfra.RowReceiver) (execinfra.Processor, error)

// NewCSVWriterProcessor is implemented in the non-free (CCL) codebase and then injected here via runtime initialization.
var NewCSVWriterProcessor func(*execinfra.FlowCtx, int32, execinfrapb.ExportSpec, execinfra.RowSource, execinfra.RowReceiver) (execinfra.Processor, error)

// NewParquetWriterProcessor is implemented in the non-free (CCL) codebase and then injected here via runtime initialization.
var NewParquetWriterProcessor func(*execinfra.FlowCtx, int32, execinfrapb.ExportSpec, execinfra.RowSource, execinfra.RowReceiver) (execinfra.Processor, error)

// NewChangeAggregatorProcessor is implemented in the non-free (CCL) codebase and then injected here via runtime initialization.
var NewChangeAggregatorProcessor func(*execinfra.FlowCtx, int32, execinfrapb.ChangeAggregatorSpec, *execinfrapb.PostProcessSpec, execinfra.RowReceiver) (execinfra.Processor, error)

// NewChangeFrontierProcessor is implemented in the non-free (CCL) codebase and then injected here via runtime initialization.
var NewChangeFrontierProcessor func(*execinfra.FlowCtx, int32, execinfrapb.ChangeFrontierSpec, execinfra.RowSource, *execinfrapb.PostProcessSpec, execinfra.RowReceiver) (execinfra.Processor, error)

// NewStreamIngestionFrontierProcessor is implemented in the non-free (CCL) codebase and then injected here via runtime initialization.
var NewStreamIngestionFrontierProcessor func(*execinfra.FlowCtx, int32, execinfrapb.StreamIngestionFrontierSpec, execinfra.RowSource, *execinfrapb.PostProcessSpec, execinfra.RowReceiver) (execinfra.Processor, error)
