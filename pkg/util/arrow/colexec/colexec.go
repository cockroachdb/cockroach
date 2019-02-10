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

package colexec

import (
	"encoding/binary"
	"math/big"
	"strings"

	goarrow "github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/math"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/arrow"
	"github.com/cockroachdb/cockroach/pkg/util/arrow/arrowserde"
	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/pkg/errors"
)

// Processor is an operator that repeatedly does work and outputs the result in
// columnar batches. Typically, it will be constructed with input Processor(s),
// which it will internally use to get the data needed to do its next piece of
// work. Next returns nil when this processor is finished.
type Processor interface {
	Next() *arrow.RecordBatch
}

type aggregator func(*array.Data, array.Builder)

var int64SumAggregator aggregator = func(in *array.Data, out array.Builder) {
	total := math.Int64.Sum(array.NewInt64Data(in))
	out.(*array.Int64Builder).Append(total)
}

var decimalMaxAggregator aggregator = func(in *array.Data, out array.Builder) {
	arr := array.NewBinaryData(in)
	var max apd.Decimal

	var d apd.Decimal
	var coeff big.Int
	for i := 0; i < arr.Len(); i++ {
		encoded := arr.Value(i)
		d.Exponent = int32(binary.LittleEndian.Uint32(encoded[:4]))
		d.Coeff = *coeff.SetBytes(encoded[4:])
		if d.Cmp(&max) > 0 {
			max = d
		}
	}
	scratch := make([]byte, 4)
	binary.LittleEndian.PutUint32(scratch[:4], uint32(max.Exponent))
	scratch = append(scratch, max.Coeff.Bytes()...)
	out.(*array.BinaryBuilder).Append(scratch)
}

var stringMaxAggregator aggregator = func(in *array.Data, out array.Builder) {
	arr := array.NewBinaryData(in)
	var max string
	for i := 0; i < arr.Len(); i++ {
		if s := arr.ValueString(i); strings.Compare(s, max) > 0 {
			max = s
		}
	}
	out.(*array.BinaryBuilder).AppendString(max)
}

// AggregationProcessor aggregates a number of columns each into a single row
// using a hardcoded operator per column type.
type AggregationProcessor struct {
	input func() *arrow.RecordBatch

	builders    []array.Builder
	aggregators []aggregator
}

// NewAggregationProcessor returns an AggregationProcessor for the columns in
// the given schema. For no particular reason, it aggregates each batch returned
// from input individually instead of aggregating all batches returned, but this
// latter is what it would do in real life.
func NewAggregationProcessor(
	schema *arrowserde.Schema, input func() *arrow.RecordBatch,
) (*AggregationProcessor, error) {
	p := &AggregationProcessor{
		input:       input,
		builders:    make([]array.Builder, schema.FieldsLength()),
		aggregators: make([]aggregator, schema.FieldsLength()),
	}

	var field arrowserde.Field
	for i := range p.aggregators {
		schema.Fields(&field, i)
		// TODO: Select the aggregator based on the SQL operator needed.
		var err error
		if p.builders[i], p.aggregators[i], err = aggregatorForField(&field); err != nil {
			return nil, err
		}
	}

	return p, nil
}

// Next implements the Processor interface.
func (p *AggregationProcessor) Next() *arrow.RecordBatch {
	input := p.input()
	if input == nil {
		return nil
	}
	for idx, aggregator := range p.aggregators {
		aggregator(input.Data[idx], p.builders[idx])
	}

	data := make([]*array.Data, len(p.builders))
	for i, builder := range p.builders {
		data[i] = builder.NewArray().Data()
	}
	return arrow.MakeBatch(input.Schema, 1, data)
}

// aggregatorForField returns a columnar aggregator that either does sum or max,
// depending on the field type.
func aggregatorForField(field *arrowserde.Field) (array.Builder, aggregator, error) {
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
				return b, int64SumAggregator, nil
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
				return b, stringMaxAggregator, nil
			case sqlbase.ColumnType_DECIMAL:
				b := array.NewBinaryBuilder(memory.DefaultAllocator, goarrow.BinaryTypes.Binary)
				return b, decimalMaxAggregator, nil
			}
		}
	}
	return nil, nil, errors.Errorf(`unsupported type %s`, arrowserde.EnumNamesType[field.TypeType()])
}
