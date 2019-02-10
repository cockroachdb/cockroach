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

package workloadcol_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/arrow"
	"github.com/cockroachdb/cockroach/pkg/workload/tpcc"
	"github.com/cockroachdb/cockroach/pkg/workload/workloadcol"
	"github.com/stretchr/testify/require"
)

type byteCountWriter int64

func (w *byteCountWriter) Write(p []byte) (n int, err error) {
	*w += byteCountWriter(len(p))
	return len(p), nil
}

func BenchmarkColumnarEncodeTPCC(b *testing.B) {
	// TODO: Support the rest of the column types used in TPCC.
	tables := tpcc.FromWarehouses(1).Tables()[5:7]
	tableInitialDatas := make([]*workloadcol.TableInitialData, len(tables))
	for idx, table := range tables {
		var err error
		tableInitialDatas[idx], err = workloadcol.MakeTableInitialData(table)
		require.NoError(b, err)
	}
	var e arrow.RecordBatchEncoder

	var bytes byteCountWriter
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for idx, tableInitialData := range tableInitialDatas {
			table := tables[idx]
			rb := tableInitialData.RecordBatch(0, table.InitialRows.NumBatches)
			require.NoError(b, e.Encode(&bytes, rb))
		}
	}
	b.StopTimer()
	b.SetBytes(int64(bytes) / int64(b.N))
}
