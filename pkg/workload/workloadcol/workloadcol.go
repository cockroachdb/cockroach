// Copyright 2019 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package workloadcol

import (
	"encoding/binary"
	"fmt"

	goarrow "github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/arrow"
	"github.com/cockroachdb/cockroach/pkg/util/arrow/arrowserde"
	"github.com/cockroachdb/cockroach/pkg/workload"
	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/pkg/errors"
)

// TableInitialData creates columnar encodings of workload initial data.
type TableInitialData struct {
	table workload.Table

	schema    *arrowserde.Schema
	schemaBuf []byte

	builders  []array.Builder
	appendFns []func(interface{})
}

// MakeTableInitialData returns a TableInitialData for the given table.
func MakeTableInitialData(table workload.Table) (*TableInitialData, error) {
	parsed, err := parser.ParseOne(fmt.Sprintf(`CREATE TABLE "%s" %s`, table.Name, table.Schema))
	if err != nil {
		return nil, err
	}
	defs := parsed.AST.(*tree.CreateTable).Defs

	d := &TableInitialData{
		table: table,
	}

	builder := flatbuffers.NewBuilder(1024)
	fieldOffsets := make([]flatbuffers.UOffsetT, 0, len(defs))
	for i := len(defs) - 1; i >= 0; i-- {
		colDef, ok := defs[i].(*tree.ColumnTableDef)
		if !ok {
			continue
		}
		col, _, _, err := sqlbase.MakeColumnDefDescs(colDef, &tree.SemaContext{})
		if err != nil {
			return nil, err
		}

		var fieldType arrowserde.Type
		var fieldOffset flatbuffers.UOffsetT
		switch col.Type.SemanticType {
		case sqlbase.ColumnType_INT:
			fieldType = arrowserde.TypeInt
			arrowserde.IntStart(builder)
			arrowserde.IntAddBitWidth(builder, 64)
			arrowserde.IntAddIsSigned(builder, 1)
			fieldOffset = arrowserde.IntEnd(builder)
		case sqlbase.ColumnType_STRING:
			fieldType = arrowserde.TypeBinary
		case sqlbase.ColumnType_DECIMAL:
			fieldType = arrowserde.TypeBinary
		default:
			return nil, errors.Errorf(`unknown type: %s`, col.Type.SemanticType)
		}

		fieldMetaKey := builder.CreateString(`sqltype`)
		fieldMetaValue := builder.CreateString(col.Type.SemanticType.String())
		arrowserde.KeyValueStart(builder)
		arrowserde.KeyValueAddKey(builder, fieldMetaKey)
		arrowserde.KeyValueAddValue(builder, fieldMetaValue)
		fieldMetaOffset := arrowserde.KeyValueEnd(builder)
		arrowserde.FieldStartCustomMetadataVector(builder, 1)
		builder.PrependUOffsetT(fieldMetaOffset)
		fieldMeta := builder.EndVector(1)

		fieldName := builder.CreateString(col.Name)
		arrowserde.FieldStart(builder)
		arrowserde.FieldAddName(builder, fieldName)
		arrowserde.FieldAddTypeType(builder, fieldType)
		arrowserde.FieldAddType(builder, fieldOffset)
		arrowserde.FieldAddCustomMetadata(builder, fieldMeta)
		if col.Nullable {
			arrowserde.FieldAddNullable(builder, 1)
		}
		fieldOffsets = append(fieldOffsets, arrowserde.FieldEnd(builder))
	}

	arrowserde.SchemaStartFieldsVector(builder, len(fieldOffsets))
	for _, fieldOffset := range fieldOffsets {
		builder.PrependUOffsetT(fieldOffset)
	}
	fields := builder.EndVector(len(fieldOffsets))
	arrowserde.SchemaStart(builder)
	arrowserde.SchemaAddFields(builder, fields)
	schema := arrowserde.SchemaEnd(builder)

	builder.Finish(schema)
	d.schemaBuf = builder.FinishedBytes()
	d.schema = arrowserde.GetRootAsSchema(d.schemaBuf, 0)

	d.builders = make([]array.Builder, d.schema.FieldsLength())
	d.appendFns = make([]func(interface{}), d.schema.FieldsLength())
	var field arrowserde.Field
	for i := range d.builders {
		d.schema.Fields(&field, i)
		var err error
		if d.builders[i], d.appendFns[i], err = builderForField(&field); err != nil {
			return nil, err
		}
	}

	return d, nil
}

// RecordBatch returns a columnar representation of the selected rows.
func (d *TableInitialData) RecordBatch(startIdx, endIdx int) *arrow.RecordBatch {
	var rowCount int64
	for batchIdx := startIdx; batchIdx < endIdx; batchIdx++ {
		batch := d.table.InitialRows.Batch(batchIdx)
		for _, row := range batch {
			rowCount++
			for idx, datum := range row {
				d.appendFns[idx](datum)
			}
		}
	}

	data := make([]*array.Data, len(d.builders))
	for i, builder := range d.builders {
		data[i] = builder.NewArray().Data()
	}
	return arrow.MakeBatch(d.schema, rowCount, data)
}

// builderForField returns a closure that creates columnar data for the given
// field. Calling the closure with each piece of data, then finalize the
// array.Builder.
func builderForField(field *arrowserde.Field) (array.Builder, func(interface{}), error) {
	var table flatbuffers.Table
	switch arrowserde.Type(field.TypeType()) {
	case arrowserde.TypeInt:
		field.Type(&table)
		var intField arrowserde.Int
		intField.Init(table.Bytes, table.Pos)
		if intField.IsSigned() > 0 {
			switch intField.BitWidth() {
			case 64:
				b := array.NewInt64Builder(memory.DefaultAllocator)
				fn := func(datum interface{}) { b.Append(int64(datum.(int))) }
				return b, fn, nil
			}
		}
	case arrowserde.TypeBinary:
		var kv arrowserde.KeyValue
		for i := 0; i < field.CustomMetadataLength(); i++ {
			field.CustomMetadata(&kv, i)
			if string(kv.Key()) != `sqltype` {
				continue
			}
			id := sqlbase.ColumnType_SemanticType_value[string(kv.Value())]
			switch sqlbase.ColumnType_SemanticType(id) {
			case sqlbase.ColumnType_STRING:
				b := array.NewBinaryBuilder(memory.DefaultAllocator, goarrow.BinaryTypes.String)
				fn := func(datum interface{}) { b.AppendString(datum.(string)) }
				return b, fn, nil
			case sqlbase.ColumnType_DECIMAL:
				var d apd.Decimal
				scratch := make([]byte, 0, 100)
				b := array.NewBinaryBuilder(memory.DefaultAllocator, goarrow.BinaryTypes.Binary)
				fn := func(datum interface{}) {
					// TODO: This is a super incomplete placeholder encoding.
					_, err := d.SetFloat64(datum.(float64))
					if err != nil {
						panic(err)
					}
					scratch = scratch[:4]
					binary.LittleEndian.PutUint32(scratch[:4], uint32(d.Exponent))
					scratch = append(scratch, d.Coeff.Bytes()...)
					b.Append(scratch)
				}
				return b, fn, nil
			}
		}
	}
	return nil, nil, errors.Errorf(`unsupported type %s`, arrowserde.EnumNamesType[field.TypeType()])
}
