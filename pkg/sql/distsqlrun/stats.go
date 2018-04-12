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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// StatSummarizer summarizes stats.
type StatSummarizer interface {
	SummarizeStats() string
}

// InputStatCollector wraps a RowSource and collects stats from it.
type InputStatCollector struct {
	RowSource
	InputStats
}

var _ RowSource = &InputStatCollector{}
var _ StatSummarizer = &InputStatCollector{}

// NewInputStatCollector creates a new InputStatCollector that wraps the given
// input described by name.
func NewInputStatCollector(input RowSource, name string) *InputStatCollector {
	return &InputStatCollector{RowSource: input, InputStats: InputStats{Name: name}}
}

// Next implements the RowSource interface. It calls Next on the embedded
// RowSource and collects stats.
func (isc *InputStatCollector) Next() (sqlbase.EncDatumRow, *ProducerMetadata) {
	row, meta := isc.RowSource.Next()
	if row != nil {
		isc.NumRows++
	}
	return row, meta
}

// SummarizeStats implements the StatSummarizer interface.
func (is InputStats) SummarizeStats() string {
	return fmt.Sprintf(
		"stat summary for %s: %d rows read",
		is.Name,
		is.NumRows,
	)
}
