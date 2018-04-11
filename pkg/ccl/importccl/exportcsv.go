// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package importccl

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlrun"
	"github.com/cockroachdb/cockroach/pkg/sql/jobs"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/pkg/errors"
)

// exportHeader is the header for EXPORT stmt results.
var exportHeader = sqlbase.ResultColumns{
	{Name: "filename", Typ: types.String},
	{Name: "rows", Typ: types.Int},
	{Name: "bytes", Typ: types.Int},
}

const (
	exportOptionDelimiter = "delimiter"
	exportOptionChunkSize = "chunk"
	exportOptionFileName  = "filename"
)

var exportOptionExpectValues = map[string]bool{
	exportOptionDelimiter: true,
	exportOptionChunkSize: true,
	exportOptionFileName:  true,
}

const exportFilePatternPart = "%part%"
const exportFilePatternDefault = exportFilePatternPart + ".csv"

// exportPlanHook implements sql.PlanHook.
func exportPlanHook(
	ctx context.Context, stmt tree.Statement, p sql.PlanHookState,
) (sql.PlanHookRowFn, sqlbase.ResultColumns, []sql.PlanNode, error) {
	exportStmt, ok := stmt.(*tree.Export)
	if !ok {
		return nil, nil, nil, nil
	}

	fileFn, err := p.TypeAsString(exportStmt.File, "EXPORT")
	if err != nil {
		return nil, nil, nil, err
	}

	if exportStmt.FileFormat != "CSV" {
		// not possible with current parser rules.
		return nil, nil, nil, errors.Errorf("unsupported import format: %q", exportStmt.FileFormat)
	}

	optsFn, err := p.TypeAsStringOpts(exportStmt.Options, exportOptionExpectValues)
	if err != nil {
		return nil, nil, nil, err
	}

	sel, err := p.Select(ctx, exportStmt.Query, nil)
	if err != nil {
		return nil, nil, nil, err
	}

	if !p.DistSQLPlanner().CheckPossible(sel) {
		return nil, nil, nil, errors.Errorf("unsupported EXPORT query -- as an alternative try `cockroach sql --format=csv`")
	}

	fn := func(ctx context.Context, plans []sql.PlanNode, resultsCh chan<- tree.Datums) error {
		ctx, span := tracing.ChildSpan(ctx, exportStmt.StatementTag())
		defer tracing.FinishSpan(span)

		if err := p.RequireSuperUser(ctx, "EXPORT"); err != nil {
			return err
		}

		if !p.ExtendedEvalContext().TxnImplicit {
			return errors.Errorf("EXPORT cannot be used inside a transaction")
		}

		file, err := fileFn()
		if err != nil {
			return err
		}

		opts, err := optsFn()
		if err != nil {
			return err
		}

		delim := ','
		if override, ok := opts[exportOptionDelimiter]; ok {
			if len(override) > 1 {
				return errors.Errorf("TODO")
			}
			delim = []rune(override)[0]
		}

		chunk := 0
		if override, ok := opts[exportOptionChunkSize]; ok {
			chunk, err = strconv.Atoi(override)
			if err != nil {
				return err
			}
		}

		filename := exportFilePatternDefault
		if override, ok := opts[exportOptionChunkSize]; ok {
			filename = override
		}
		if strings.Count(filename, exportFilePatternPart) != 1 {
			return errors.Errorf("EXPORT filename pattern does must contain one instance of %q", exportFilePatternPart)
		}

		out := distsqlrun.ProcessorCoreUnion{CSVWriter: &distsqlrun.CSVWriterSpec{
			Destination: file,
			NamePattern: filename,
			Delimiter:   delim,
			ChunkRows:   int64(chunk),
		}}

		rows := sqlbase.NewRowContainer(
			p.ExtendedEvalContext().Mon.MakeBoundAccount(), sqlbase.ColTypeInfoFromColTypes(sql.ExportPlanResultTypes), 0,
		)
		rw := sql.NewRowResultWriter(rows)

		if err := p.DistSQLPlanner().PlanAndRunExport(
			ctx, p.Txn(), p.ExtendedEvalContext(), plans[0], out, rw,
		); err != nil {
			return err
		}
		for i := 0; i < rows.Len(); i++ {
			resultsCh <- rows.At(i)
		}
		rows.Close(ctx)
		return rw.Err()
	}

	return fn, exportHeader, []sql.PlanNode{sel}, nil
}

func newCSVWriterProcessor(
	flowCtx *distsqlrun.FlowCtx,
	spec distsqlrun.CSVWriterSpec,
	input distsqlrun.RowSource,
	output distsqlrun.RowReceiver,
) (distsqlrun.Processor, error) {
	c := &csvWriter{
		flowCtx:  flowCtx,
		spec:     spec,
		input:    input,
		output:   output,
		settings: flowCtx.Settings,
		registry: flowCtx.JobRegistry,
		progress: spec.Progress,
		db:       flowCtx.EvalCtx.Txn.DB(),
	}
	if err := c.out.Init(&distsqlrun.PostProcessSpec{}, sql.ExportPlanResultTypes, flowCtx.NewEvalCtx(), output); err != nil {
		return nil, err
	}
	return c, nil
}

type csvWriter struct {
	flowCtx  *distsqlrun.FlowCtx
	spec     distsqlrun.CSVWriterSpec
	input    distsqlrun.RowSource
	out      distsqlrun.ProcOutputHelper
	output   distsqlrun.RowReceiver
	settings *cluster.Settings
	registry *jobs.Registry
	progress distsqlrun.JobProgress
	db       *client.DB
}

var _ distsqlrun.Processor = &csvWriter{}

func (sp *csvWriter) OutputTypes() []sqlbase.ColumnType {
	return sql.ExportPlanResultTypes
}

func (sp *csvWriter) Run(ctx context.Context, wg *sync.WaitGroup) {
	ctx, span := tracing.ChildSpan(ctx, "csvWriter")
	defer tracing.FinishSpan(span)

	if wg != nil {
		defer wg.Done()
	}

	err := func() error {
		pattern := exportFilePatternDefault
		if sp.spec.NamePattern != "" {
			pattern = sp.spec.NamePattern
		}

		types := sp.input.OutputTypes()
		sp.input.Start(ctx)
		input := distsqlrun.MakeNoMetadataRowSource(sp.input, sp.output)

		alloc := &sqlbase.DatumAlloc{}

		var buf bytes.Buffer
		writer := csv.NewWriter(&buf)
		if sp.spec.Delimiter != 0 {
			writer.Comma = sp.spec.Delimiter
		}

		f := tree.NewFmtCtxWithBuf(tree.FmtParsable)
		defer f.Close()

		csvRow := make([]string, len(types))

		chunk := 0
		done := false
		var rows int64
		for {
			buf.Reset()
			for {
				if sp.spec.ChunkRows > 0 && rows >= sp.spec.ChunkRows {
					break
				}
				row, err := input.NextRow()
				if err != nil {
					return err
				}
				if row == nil {
					done = true
					break
				}
				rows++

				for i, ed := range row {
					if err := ed.EnsureDecoded(&types[i], alloc); err != nil {
						return err
					}
					ed.Datum.Format(&f.FmtCtx)
					csvRow[i] = f.String()
					f.Reset()
				}
				if err := writer.Write(csvRow); err != nil {
					return err
				}
			}
			writer.Flush()

			conf, err := storageccl.ExportStorageConfFromURI(sp.spec.Destination)
			if err != nil {
				return err
			}
			es, err := storageccl.MakeExportStorage(ctx, conf, sp.settings)
			if err != nil {
				return err
			}
			defer es.Close()

			size := buf.Len()

			part := fmt.Sprintf("n%d.%d", sp.flowCtx.EvalCtx.NodeID, chunk)
			filename := strings.Replace(pattern, exportFilePatternPart, part, -1)
			if err := es.WriteFile(ctx, filename, bytes.NewReader(buf.Bytes())); err != nil {
				return err
			}

			res := sqlbase.EncDatumRow{
				sqlbase.DatumToEncDatum(
					sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_STRING},
					tree.NewDString(filename),
				),
				sqlbase.DatumToEncDatum(
					sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_INT},
					tree.NewDInt(tree.DInt(rows)),
				),
				sqlbase.DatumToEncDatum(
					sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_INT},
					tree.NewDInt(tree.DInt(size)),
				),
			}

			cs, err := sp.out.EmitRow(ctx, res)
			if err != nil {
				return err
			}
			if cs != distsqlrun.NeedMoreRows {
				return errors.New("unexpected closure of consumer")
			}
			if done {
				break
			}
		}

		return nil
	}()

	distsqlrun.DrainAndClose(
		ctx, sp.output, err, func(context.Context) {} /* pushTrailingMeta */, sp.input)
}

func init() {
	sql.AddPlanHook(exportPlanHook)
	distsqlrun.NewCSVWriterProcessor = newCSVWriterProcessor
}
