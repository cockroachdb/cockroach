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
	"compress/gzip"
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
	"github.com/xitongsys/parquet-go-source/writerfile"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/tool/parquet-tools/schematool"
	"github.com/xitongsys/parquet-go/writer"
)

// parquetExporter data structure to augment the compression
// and csv writer, encapsulating the internals to make
// exporting oblivious for the consumers.
type parquetExporter struct {
	compressor    *gzip.Writer
	buf           *bytes.Buffer
	parquetWriter *writer.CSVWriter
}

// Write append record to parquet file.
func (c *parquetExporter) Write(record []*string) error {
	return c.parquetWriter.WriteString(record)
}

// Flush append record to parquet file.
func (c *parquetExporter) Flush() error {
	return c.parquetWriter.WriteStop()
}

// Close closes the parquet writer
func (c *parquetExporter) Close() error {

	c.parquetWriter.PFile.Close()
	//if c.compressor != nil {
	//	return c.compressor.Close()
	//}
	return nil
}

// Bytes results in the slice of bytes with compressed content.
func (c *parquetExporter) Bytes() []byte { // IDENTICAL
	return c.buf.Bytes()
}

func (c *parquetExporter) ResetBuffer() { // IDENTICAL
	c.buf.Reset()
}

// Len returns length of the buffer with content. //IDENTICAL
func (c *parquetExporter) Len() int {
	return c.buf.Len()
}

func (c *parquetExporter) FileName(spec execinfrapb.ParquetWriterSpec, part string) string { // DIFFERENT INPU
	pattern := exportFilePatternDefault
	if spec.NamePattern != "" {
		pattern = spec.NamePattern
	}

	fileName := strings.Replace(pattern, exportFilePatternPart, part, -1)

	return fileName
}

func newParquetExporter(
	sp execinfrapb.ParquetWriterSpec, typs []*types.T,
) (*parquetExporter, error) {

	schema, err := newParquetSchema(typs)
	if err != nil {
		return nil, err
	}

	var exporter *parquetExporter

	buf := bytes.NewBuffer([]byte{})
	pf := writerfile.NewWriterFile(buf)
	pw, err := writer.NewCSVWriter(schema, pf, 4)
	if err != nil {
		return nil, err
	}

	switch sp.CompressionCodec {
	default:
		{
			exporter = &parquetExporter{
				buf:           buf,
				parquetWriter: pw,
			}
		}
	}

	return exporter, nil
}

//MB: KEEPING THIS AROUND FOR NOW. NOT USED. NO NEED TO REVIEW
// parquetToJson defines an element in the json which defines the parquet file schema. Typically,
// an instantiation of the struct defines the properties of a column in the parquet file.
// MB Note: I think the recursive nature of this struct will make it easy to deal with user
// defined types
/*type parquetToJSON struct{
	Tag string `json:"Tag"`
	Fields []parquetToJSON `json:"Fields,omitempty"`
}*/

// MB: KEEPING THIS AROUND FOR NOW. NOT USED. NO NEED TO REVIEW.
// newParquetSchemaJSON creates the JSON schema for the parquet file, see example schema: here
// https://github.com/xitongsys/parquet-go/blob/055d06dd4609f7374dc35ea1c2b521a0a28ec3e6/example/json_write.go
/*func newParquetSchemaJSON() (string, error){

	root := new(parquetToJSON)
	root.Tag = "name=root, repetitiontype=REQUIRED"
	// THIS IS WRONG, can't use colinfo.ExportColumns
	colSchema:= make([]parquetToJSON, len(colinfo.ExportColumns))

	for i := 0; i< len(colinfo.ExportColumns); i++{
		colType := typeToParquetType(colinfo.ExportColumns[i].Typ)
		colSchema[i].Tag = fmt.Sprintf("name=%s,inname=%s,type=%s",
			colinfo.ExportColumns[i].Name, strings.ToTitle(colinfo.ExportColumns[i].Name), colType)
	}
	root.Fields = colSchema
	json_enc, err := json.Marshal(root)
	if err != nil{
		return "", err
	}
	return string(json_enc), nil
}*/

// newParquetSchema defines the column names and types for the parquet file based on the table
// passed to the parquet exporter
func newParquetSchema(typs []*types.T) ([]string, error) {
	colSchema := make([]string, len(typs))
	for i := 0; i < len(typs); i++ {
		colType := typeToParquetType(typs[i])
		colSchema[i] = fmt.Sprintf("name=%d,type=%s",
			i, colType)
	}
	return colSchema, nil
}

// typeToParquetType maps crbd sql data types to parquet file types
func typeToParquetType(typ *types.T) string {
	var parquetType parquet.Type
	switch typ.Family() {
	case types.BoolFamily:
		parquetType = parquet.Type_BOOLEAN
	case types.StringFamily:
		parquetType = parquet.Type_BYTE_ARRAY
	case types.IntFamily:
		parquetType = parquet.Type_INT32

	case types.FloatFamily:
		parquetType = parquet.Type_FLOAT

	// MB: everything else, for now...
	default:
		parquetType = parquet.Type_BYTE_ARRAY
	}
	parquetTypeString, _ := schematool.ParquetTypeToParquetTypeStr(&parquetType, nil)
	fmt.Println(parquetTypeString)
	return parquetTypeString
}

// MB note: basically the same as a csv writer processor
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

// MB: just copying this from csv exporter, not sure if it's correct
func (sp *parquetWriterProcessor) OutputTypes() []*types.T {
	res := make([]*types.T, len(colinfo.ExportColumns))
	for i := range res {
		res[i] = colinfo.ExportColumns[i].Typ
	}
	return res
}

// MB: copying from csv exporter. not sure what this does.
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

		var nullsAs string
		if sp.spec.Options.NullEncoding != nil {
			nullsAs = *sp.spec.Options.NullEncoding
		}

		f := tree.NewFmtCtx(tree.FmtExport)
		defer f.Close()

		parquetRow := make([]*string, len(typs))
		chunk := 0
		done := false
		for {
			var rows int64
			// MB: biggest difference from csv: for parquet, it's easiest to create a
			// new ParquetExporter each time the processor creates parquet file
			writer, err := newParquetExporter(sp.spec, typs)
			if err != nil {
				return err
			}

			for {
				// If the bytes.Buffer sink exceeds the target size of a Parquet file, we
				// flush before exporting any additional rows.
				if int64(writer.buf.Len()) >= sp.spec.ChunkSize {
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
						if sp.spec.Options.NullEncoding != nil {
							parquetRow[i] = &nullsAs // slightly diff from csv
							continue
						} else {
							return errors.New("NULL value encountered during EXPORT, " +
								"use `WITH nullas` to specify the string representation of NULL")
						}
					}
					if err := ed.EnsureDecoded(typs[i], alloc); err != nil {
						return err
					}
					ed.Datum.Format(f)
					val := f.String() //MB: slightly different from csv
					parquetRow[i] = &val
					f.Reset()
				}
				if err := writer.Write(parquetRow); err != nil {
					return err
				}
			}
			if rows < 1 {
				break
			}
			if err := writer.Flush(); err != nil {
				return errors.Wrap(err, "failed to flush parquet writer")
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
			filename := writer.FileName(sp.spec, part)

			// Close writer to ensure buffer and any compression footer is flushed.
			err = writer.Close()
			if err != nil {
				return errors.Wrapf(err, "failed to close exporting writer")
			}

			size := writer.Len()

			if err := cloud.WriteFile(ctx, es, filename, bytes.NewReader(writer.Bytes())); err != nil {
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
