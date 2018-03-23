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

package memo_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/util/treeprinter"
)

func TestLogicalProps(t *testing.T) {
	md := opt.NewMetadata()
	i := md.AddColumn("i", types.Int)
	d := md.AddColumn("d", types.Decimal)
	s := md.AddColumn("s", types.String)

	outCols := opt.ColSet{}
	outCols.Add(int(i))

	outerCols := opt.ColSet{}
	outerCols.Add(int(d))
	outerCols.Add(int(s))

	colList := opt.ColList{s, i}

	tp := treeprinter.New()
	nd := tp.Child("props")

	relational := &memo.LogicalProps{
		Relational: &memo.RelationalProps{
			OutputCols:  outCols,
			OuterCols:   outerCols,
			NotNullCols: outCols,
		},
	}

	scalar := &memo.LogicalProps{
		Scalar: &memo.ScalarProps{OuterCols: outerCols},
	}

	relational.FormatColSet(nd, md, "output:", relational.Relational.OutputCols)
	relational.FormatColSet(nd, md, "outer relational:", relational.OuterCols())
	relational.FormatColList(nd, md, "list:", colList)
	relational.FormatColSet(nd, md, "outer scalar:", scalar.OuterCols())

	expected := "props\n" +
		" ├── output: i:1(int!null)\n" +
		" ├── outer relational: d:2(decimal) s:3(string)\n" +
		" ├── list: s:3(string) i:1(int!null)\n" +
		" └── outer scalar: d:2(decimal) s:3(string)\n"

	actual := tp.String()
	if actual != expected {
		t.Fatalf("expected:\n%s\nactual:\n%s\n", expected, actual)
	}
}
