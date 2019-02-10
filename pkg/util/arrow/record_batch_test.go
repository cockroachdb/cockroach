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

package arrow_test

import (
	"bytes"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/arrow"
	"github.com/cockroachdb/cockroach/pkg/workload/tpcc"
	"github.com/cockroachdb/cockroach/pkg/workload/workloadcol"
	"github.com/stretchr/testify/require"
)

func BenchmarkRecordBatchEncode(b *testing.B) {
	newOrder := tpcc.FromWarehouses(1).Tables()[5]
	newOrderData, err := workloadcol.MakeTableInitialData(newOrder)
	require.NoError(b, err)
	newOrderBatch := newOrderData.RecordBatch(0, newOrder.InitialRows.NumBatches)
	var e arrow.RecordBatchEncoder

	var buf bytes.Buffer
	b.SetBytes(newOrderBatch.ApproxSize())
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		require.NoError(b, e.Encode(&buf, newOrderBatch))
	}
	b.StopTimer()
}

func BenchmarkRecordBatchDecode(b *testing.B) {
	newOrder := tpcc.FromWarehouses(1).Tables()[5]
	newOrderData, err := workloadcol.MakeTableInitialData(newOrder)
	require.NoError(b, err)
	newOrderBatch := newOrderData.RecordBatch(0, newOrder.InitialRows.NumBatches)
	var buf bytes.Buffer
	require.NoError(b, (&arrow.RecordBatchEncoder{}).Encode(&buf, newOrderBatch))
	encoded := buf.Bytes()

	var scratch arrow.RecordBatch
	b.SetBytes(newOrderBatch.ApproxSize())
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		require.NoError(b, scratch.Decode(newOrderBatch.Schema, encoded))
	}
	b.StopTimer()
}

// TODO: Tests, heh.
