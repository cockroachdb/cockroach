// Copyright 2018 The Cockroach Authors.
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

package distsqlrun

import (
	"testing"

	"context"

	"fmt"

	"sort"

	"strings"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

type ProcessorTestConfig struct {
	// An optional FlowCtx. ProcessorTest takes ownership of the evalCtx passed
	// through this FlowCtx.
	FlowCtx *FlowCtx

	BeforeTestCase func(p Processor, inputs []RowSource, output RowReceiver)
	AfterTestCase  func(p Processor, inputs []RowSource, output RowReceiver)
}

func DefaultProcessorTestConfig() ProcessorTestConfig {
	return ProcessorTestConfig{}
}

// TODO(asubiotto): Comment.
type ProcessorTestCaseRows struct {
	Rows  [][]interface{}
	Types []sqlbase.ColumnType
}

// toEncDatum converts a concrete type to an EncDatum.
// TODO(asubiotto): This might make more sense in another package.
func toEncDatum(datumType sqlbase.ColumnType, v interface{}) sqlbase.EncDatum {
	d := func() tree.Datum {
		switch concreteType := v.(type) {
		case int:
			if datumType.SemanticType == sqlbase.ColumnType_DECIMAL {
				dd := &tree.DDecimal{}
				dd.SetInt64(int64(v.(int)))
				return dd
			}
			return tree.NewDInt(tree.DInt(v.(int)))
		case nil:
			return tree.DNull
		default:
			panic(fmt.Sprintf("type %T not supported yet", concreteType))
		}
	}()
	return sqlbase.DatumToEncDatum(datumType, d)
}

func (r ProcessorTestCaseRows) toEncDatumRows() sqlbase.EncDatumRows {
	result := make(sqlbase.EncDatumRows, len(r.Rows))
	for i, row := range r.Rows {
		if len(row) != len(r.Types) {
			panic("mismatched number of columns and number of types")
		}
		result[i] = make(sqlbase.EncDatumRow, len(row))
		for j, col := range row {
			result[i][j] = toEncDatum(r.Types[j], col)
		}
	}
	return result
}

type ProcessorTestCase struct {
	Name   string
	Input  ProcessorTestCaseRows
	Output ProcessorTestCaseRows

	// SecondInput can be optionally set by processors that take in two inputs.
	SecondInput ProcessorTestCaseRows

	// DisableSort disables the sorting of the output produced by the processor
	// before checking for expected output.
	DisableSort bool

	// NewProcessor should be set to a function that performs specific setup and
	// returns the desired processor to test. Common arguments to processor
	// constructors are provided.
	NewProcessor func(
		ctx *FlowCtx, id int32, inputs []RowSource, output RowReceiver,
	) (Processor, error)
}

type ProcessorTest struct {
	config  ProcessorTestConfig
	flowCtx *FlowCtx
}

// Init initializes the ProcessorTest with the given config.
func MakeProcessorTest(config ProcessorTestConfig) ProcessorTest {
	p := ProcessorTest{}
	if config.FlowCtx == nil {
		st := cluster.MakeTestingClusterSettings()
		evalCtx := tree.MakeTestingEvalContext(st)
		p.flowCtx = &FlowCtx{
			Settings: st,
			EvalCtx:  &evalCtx,
		}
	} else {
		p.flowCtx = config.FlowCtx
	}
	p.config = config
	return p
}

func (p *ProcessorTest) RunTestCases(
	ctx context.Context, t *testing.T, testCases []ProcessorTestCase,
) {
	var processorID int32
	for _, tc := range testCases {
		inputs := make([]RowSource, 1, 2)
		inputs[0] = NewRowBuffer(
			tc.Input.Types, tc.Input.toEncDatumRows(), RowBufferArgs{},
		)
		output := NewRowBuffer(
			tc.Output.Types, nil, RowBufferArgs{},
		)

		processor, err := tc.NewProcessor(p.flowCtx, processorID, inputs, output)
		if err != nil {
			t.Fatalf("test case %s processor creation failed %s", tc.Name, err)
		}
		processorID++

		if p.config.BeforeTestCase != nil {
			p.config.BeforeTestCase(processor, inputs, output)
		}

		processor.Run(ctx, nil)

		expectedRows := tc.Output.toEncDatumRows()
		expected := make([]string, len(expectedRows))
		for i, row := range expectedRows {
			expected[i] = row.String(tc.Output.Types)
		}
		if !tc.DisableSort {
			sort.Strings(expected)
		}

		var returned []string
		for {
			row := output.NextNoMeta(t)
			if row == nil {
				break
			}
			returned = append(returned, row.String(tc.Output.Types))
		}
		if !tc.DisableSort {
			sort.Strings(returned)
		}

		expStr := strings.Join(expected, "")
		retStr := strings.Join(returned, "")
		if expStr != retStr {
			t.Errorf(
				"test case %s invalid results; expected\n%s\ngot:\n%s",
				tc.Name,
				expStr,
				retStr,
			)
		}
	}
}

func (p ProcessorTest) Close(ctx context.Context) {
	p.flowCtx.EvalCtx.Stop(ctx)
}
