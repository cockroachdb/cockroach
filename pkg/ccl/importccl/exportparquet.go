// Copyright 2021 The Cockroach Authors.
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
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	goparquet "github.com/fraugster/parquet-go"
	"github.com/fraugster/parquet-go/parquet"
	"github.com/fraugster/parquet-go/parquetschema"
	"github.com/lib/pq/oid"
)

const exportParquetFilePatternDefault = exportFilePatternPart + ".parquet"

// parquetExporter is used to augment the parquetWriter, encapsulating the internals to make
// exporting oblivious for the consumers.
type parquetExporter struct {
	buf            *bytes.Buffer
	parquetWriter  *goparquet.FileWriter
	schema         *parquetschema.SchemaDefinition
	parquetColumns []parquetColumn
}

// Write appends a record to a parquet file.
func (c *parquetExporter) Write(record map[string]interface{}) error {
	return c.parquetWriter.AddData(record)
}

// Flush is merely a placeholder to mimic the CSV Exporter (we may add their methods
// to an interface in the near future). All flushing is done by parquetExporter.Close().
func (c *parquetExporter) Flush() error {
	return nil
}

// Close flushes all records to parquetExporter.buf and closes the parquet writer.
func (c *parquetExporter) Close() error {
	return c.parquetWriter.Close()
}

// Bytes results in the slice of bytes.
func (c *parquetExporter) Bytes() []byte {
	return c.buf.Bytes()
}

func (c *parquetExporter) ResetBuffer() {
	c.buf.Reset()
	c.parquetWriter = buildFileWriter(c.buf, c.schema)
}

// Len returns length of the buffer with content.
func (c *parquetExporter) Len() int {
	return c.buf.Len()
}

func (c *parquetExporter) FileName(spec execinfrapb.ParquetWriterSpec, part string) string {
	pattern := exportParquetFilePatternDefault
	if spec.NamePattern != "" {
		pattern = spec.NamePattern
	}

	fileName := strings.Replace(pattern, exportFilePatternPart, part, -1)

	return fileName
}

// newParquetExporter creates a new parquet file writer, defines the parquet
// file schema, and initializes a new parquetExporter.
func newParquetExporter(
	sp execinfrapb.ParquetWriterSpec, typs []*types.T,
) (*parquetExporter, error) {

	var exporter *parquetExporter

	buf := bytes.NewBuffer([]byte{})
	parquetColumns, err := newParquetColumns(typs, sp)
	if err != nil {
		return nil, err
	}
	schema := newParquetSchema(parquetColumns)

	exporter = &parquetExporter{
		buf:            buf,
		schema:         schema,
		parquetColumns: parquetColumns,
	}
	return exporter, nil
}

// parquetColumn contains the relevant data to map a crdb table column to a parquet table column.
type parquetColumn struct {
	name     string
	crbdType *types.T

	//definition contains all relevant information around the parquet type for the table column
	definition *parquetschema.ColumnDefinition

	//encodeFn converts crdb table column value to a native go type.
	encodeFn func(datum tree.Datum) (interface{}, error)
}

// newParquetColumns creates a list of parquet columns, given the input relation's column types.
func newParquetColumns(typs []*types.T, sp execinfrapb.ParquetWriterSpec) ([]parquetColumn, error) {
	parquetColumns := make([]parquetColumn, len(typs))
	for i := 0; i < len(typs); i++ {
		parquetCol, err := newParquetColumn(typs[i], sp.ColNames[i], sp.ColNullability[i])
		if err != nil {
			return nil, err
		}
		parquetColumns[i] = parquetCol
	}
	return parquetColumns, nil
}

// newParquetColumn populates a parquetColumn by finding the right parquet type and defining the
// encodeFn.
func newParquetColumn(typ *types.T, name string, nullable bool) (parquetColumn, error) {
	col := parquetColumn{}
	col.definition = new(parquetschema.ColumnDefinition)
	col.definition.SchemaElement = parquet.NewSchemaElement()
	col.name = name
	col.crbdType = typ

	/*
			The type of a parquet column is either a group (i.e.
		  an array in crdb) or a primitive type (e.g., int, float, boolean,
		  string) and the repetition can be one of the three following cases:

		  - required: exactly one occurrence (i.e. the column value is a scalar, and
		  cannot have null values). A column is set to required if the user
		  specified the CRDB column as NOT NULL.
		  - optional: 0 or 1 occurrence (i.e. same as above, but can have values)
		  - repeated: 0 or more occurrences (the column value can be an array of values,
		  so the value within the array will have its own repetition type)

			See this blog post for more on parquet type specification:
			https://blog.twitter.com/engineering/en_us/a/2013/dremel-made-simple-with-parquet
	*/
	col.definition.SchemaElement.RepetitionType = parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_OPTIONAL)
	if !nullable {
		col.definition.SchemaElement.RepetitionType = parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_REQUIRED)
	}
	col.definition.SchemaElement.Name = col.name

	// MB figured out the low level properties of the encoding by running the goland debugger on
	// the following vendor example:
	// https://github.com/fraugster/parquet-go/blob/master/examples/write-low-level/main.go
	switch typ.Family() {
	case types.BoolFamily:
		col.definition.SchemaElement.Type = parquet.TypePtr(parquet.Type_BOOLEAN)
		col.encodeFn = func(d tree.Datum) (interface{}, error) {
			return bool(*d.(*tree.DBool)), nil
		}

	case types.StringFamily:
		col.definition.SchemaElement.Type = parquet.TypePtr(parquet.Type_BYTE_ARRAY)
		col.definition.SchemaElement.LogicalType = parquet.NewLogicalType()
		col.definition.SchemaElement.LogicalType.STRING = parquet.NewStringType()
		col.definition.SchemaElement.ConvertedType = parquet.ConvertedTypePtr(parquet.ConvertedType_UTF8)
		col.encodeFn = func(d tree.Datum) (interface{}, error) {
			return []byte(*d.(*tree.DString)), nil
		}

	case types.IntFamily:
		if typ.Oid() == oid.T_int8 {
			col.definition.SchemaElement.Type = parquet.TypePtr(parquet.Type_INT64)
			col.encodeFn = func(d tree.Datum) (interface{}, error) {
				return int64(*d.(*tree.DInt)), nil
			}
		} else {
			col.definition.SchemaElement.Type = parquet.TypePtr(parquet.Type_INT32)
			col.encodeFn = func(d tree.Datum) (interface{}, error) {
				return int32(*d.(*tree.DInt)), nil
			}
		}

	case types.FloatFamily:
		if typ.Oid() == oid.T_float8 {
			col.definition.SchemaElement.Type = parquet.TypePtr(parquet.Type_DOUBLE)
			col.encodeFn = func(d tree.Datum) (interface{}, error) {
				return float64(*d.(*tree.DFloat)), nil
			}
		} else {
			col.definition.SchemaElement.Type = parquet.TypePtr(parquet.Type_FLOAT)
			col.encodeFn = func(d tree.Datum) (interface{}, error) {
				return float32(*d.(*tree.DFloat)), nil
			}
		}

	case types.ArrayFamily:
		// TODO(mb): Figure out how to modify the encodeFn for arrays.
		// One possibility: recurse on type within array, define encodeFn elsewhere
		// col.definition.Children[0] =  newParquetColumn(typ.ArrayContents(), name)
		return col, errors.Errorf("parquet export does not support array type yet")

	default:
		return col, errors.Errorf("parquet export does not support the %v type yet", typ.Family())
	}

	return col, nil
}

// newParquetSchema creates the schema for the parquet file,
// see example schema:
//     https://github.com/fraugster/parquet-go/issues/18#issuecomment-946013210
// see docs here:
//     https://pkg.go.dev/github.com/fraugster/parquet-go/parquetschema#SchemaDefinition
func newParquetSchema(parquetFields []parquetColumn) *parquetschema.SchemaDefinition {

	schemaDefinition := new(parquetschema.SchemaDefinition)
	schemaDefinition.RootColumn = new(parquetschema.ColumnDefinition)
	schemaDefinition.RootColumn.SchemaElement = parquet.NewSchemaElement()

	for i := 0; i < len(parquetFields); i++ {
		schemaDefinition.RootColumn.Children = append(schemaDefinition.RootColumn.Children,
			parquetFields[i].definition)
		schemaDefinition.RootColumn.SchemaElement.Name = "root"
	}
	return schemaDefinition
}

func buildFileWriter(
	buf *bytes.Buffer, schema *parquetschema.SchemaDefinition,
) *goparquet.FileWriter {
	pw := goparquet.NewFileWriter(buf,
		// TODO(MB): allow for user defined compression
		goparquet.WithCompressionCodec(parquet.CompressionCodec_SNAPPY),
		goparquet.WithSchemaDefinition(schema),
	)
	return pw
}

func newParquetWriterProcessor(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec execinfrapb.ParquetWriterSpec,
	input execinfra.RowSource,
	output execinfra.RowReceiver,
) (execinfra.Processor, error) {
	c := &parquetWriterProcessor{
		flowCtx:     flowCtx,
		processorID: processorID,
		spec:        spec,
		input:       input,
		output:      output,
	}
	semaCtx := tree.MakeSemaContext()
	if err := c.out.Init(&execinfrapb.PostProcessSpec{}, c.OutputTypes(), &semaCtx, flowCtx.NewEvalCtx()); err != nil {
		return nil, err
	}
	return c, nil
}

type parquetWriterProcessor struct {
	flowCtx     *execinfra.FlowCtx
	processorID int32
	spec        execinfrapb.ParquetWriterSpec
	input       execinfra.RowSource
	out         execinfra.ProcOutputHelper
	output      execinfra.RowReceiver
}

var _ execinfra.Processor = &parquetWriterProcessor{}

func (sp *parquetWriterProcessor) OutputTypes() []*types.T {
	res := make([]*types.T, len(colinfo.ExportColumns))
	for i := range res {
		res[i] = colinfo.ExportColumns[i].Typ
	}
	return res
}

// MustBeStreaming currently never gets called by the parquetWriterProcessor as
// the function only applies to implementation.
func (sp *parquetWriterProcessor) MustBeStreaming() bool {
	return false
}

func (sp *parquetWriterProcessor) Run(ctx context.Context) {
	ctx, span := tracing.ChildSpan(ctx, "parquetWriter")
	defer span.Finish()

	instanceID := sp.flowCtx.EvalCtx.NodeID.SQLInstanceID()
	uniqueID := builtins.GenerateUniqueInt(instanceID)

	err := func() error {
		typs := sp.input.OutputTypes()
		sp.input.Start(ctx)
		input := execinfra.MakeNoMetadataRowSource(sp.input, sp.output)
		alloc := &rowenc.DatumAlloc{}

		exporter, err := newParquetExporter(sp.spec, typs)
		if err != nil {
			return err
		}

		parquetRow := make(map[string]interface{}, len(typs))
		chunk := 0
		done := false
		for {
			var rows int64
			exporter.ResetBuffer()
			for {
				// If the bytes.Buffer sink exceeds the target size of a Parquet file, we
				// flush before exporting any additional rows.
				if int64(exporter.buf.Len()) >= sp.spec.ChunkSize {
					break
				}
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
					if ed.IsNull() {
						parquetRow[exporter.parquetColumns[i].name] = nil
					} else {
						if err := ed.EnsureDecoded(typs[i], alloc); err != nil {
							return err
						}
						edNative, err := exporter.parquetColumns[i].encodeFn(ed.Datum)
						if err != nil {
							return err
						}
						parquetRow[exporter.parquetColumns[i].name] = edNative
					}
				}
				if err := exporter.Write(parquetRow); err != nil {
					return err
				}
			}
			if rows < 1 {
				break
			}
			if err := exporter.Flush(); err != nil {
				return errors.Wrap(err, "failed to flush parquet exporter")
			}

			// Close exporter to ensure buffer and any compression footer is flushed.
			err = exporter.Close()
			if err != nil {
				return errors.Wrapf(err, "failed to close exporting exporter")
			}

			conf, err := cloud.ExternalStorageConfFromURI(sp.spec.Destination, sp.spec.User())
			if err != nil {
				return err
			}
			es, err := sp.flowCtx.Cfg.ExternalStorage(ctx, conf)
			if err != nil {
				return err
			}
			defer es.Close()

			part := fmt.Sprintf("n%d.%d", uniqueID, chunk)
			chunk++
			filename := exporter.FileName(sp.spec, part)

			size := exporter.Len()

			if err := cloud.WriteFile(ctx, es, filename, bytes.NewReader(exporter.Bytes())); err != nil {
				return err
			}
			res := rowenc.EncDatumRow{
				rowenc.DatumToEncDatum(
					types.String,
					tree.NewDString(filename),
				),
				rowenc.DatumToEncDatum(
					types.Int,
					tree.NewDInt(tree.DInt(rows)),
				),
				rowenc.DatumToEncDatum(
					types.Int,
					tree.NewDInt(tree.DInt(size)),
				),
			}

			cs, err := sp.out.EmitRow(ctx, res, sp.output)
			if err != nil {
				return err
			}
			if cs != execinfra.NeedMoreRows {
				// TODO(dt): presumably this is because our recv already closed due to
				// another error... so do we really need another one?
				return errors.New("unexpected closure of consumer")
			}
			if done {
				break
			}
		}

		return nil
	}()

	// TODO(dt): pick up tracing info in trailing meta
	execinfra.DrainAndClose(
		ctx, sp.output, err, func(context.Context) {} /* pushTrailingMeta */, sp.input)
}

func init() {
	rowexec.NewParquetWriterProcessor = newParquetWriterProcessor
}
