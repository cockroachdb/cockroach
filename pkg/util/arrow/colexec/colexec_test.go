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

package colexec_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/arrow"
	"github.com/cockroachdb/cockroach/pkg/util/arrow/colexec"
	"github.com/cockroachdb/cockroach/pkg/workload/tpcc"
	"github.com/cockroachdb/cockroach/pkg/workload/workloadcol"
	"github.com/stretchr/testify/require"
)

func BenchmarkAggregatorProcessor(b *testing.B) {
	tpccTables := tpcc.FromWarehouses(1).Tables()
	newOrder, item := tpccTables[5], tpccTables[6]

	newOrderData, err := workloadcol.MakeTableInitialData(newOrder)
	require.NoError(b, err)
	newOrderBatch := newOrderData.RecordBatch(0, newOrder.InitialRows.NumBatches)

	itemData, err := workloadcol.MakeTableInitialData(item)
	require.NoError(b, err)
	itemBatch := itemData.RecordBatch(0, item.InitialRows.NumBatches)

	b.Run(`ints`, func(b *testing.B) {
		b.SetBytes(newOrderBatch.ApproxSize())
		for i := 0; i < b.N; i++ {
			p0 := makePrecomputedProcessor(newOrderBatch)
			p1, err := colexec.NewAggregationProcessor(newOrderBatch.Schema, p0.Next)
			require.NoError(b, err)
			for p1.Next() != nil {
			}
		}
	})
	b.Run(`mixed`, func(b *testing.B) {
		b.SetBytes(itemBatch.ApproxSize())
		for i := 0; i < b.N; i++ {
			p0 := makePrecomputedProcessor(itemBatch)
			p1, err := colexec.NewAggregationProcessor(itemBatch.Schema, p0.Next)
			require.NoError(b, err)
			for p1.Next() != nil {
			}
		}
	})

	// TODO: Switch all these to item. Something is broken with RecordBatch
	// encode/decode for binary columns.
	b.Run(`local`, func(b *testing.B) {
		b.SetBytes(newOrderBatch.ApproxSize())
		for i := 0; i < b.N; i++ {
			p0 := makePrecomputedProcessor(newOrderBatch)
			p1, err := colexec.NewAggregationProcessor(newOrderBatch.Schema, p0.Next)
			require.NoError(b, err)
			for p1.Next() != nil {
			}
		}
	})
	b.Run(`local-reuse`, func(b *testing.B) {
		b.SetBytes(newOrderBatch.ApproxSize())
		p0 := makePrecomputedProcessor(newOrderBatch)
		p1, err := colexec.NewAggregationProcessor(newOrderBatch.Schema, p0.Next)
		for i := 0; i < b.N; i++ {
			p0.Reset()
			require.NoError(b, err)
			for p1.Next() != nil {
			}
		}
	})
	b.Run(`remote`, func(b *testing.B) {
		b.SetBytes(newOrderBatch.ApproxSize())
		for i := 0; i < b.N; i++ {
			p0 := makePrecomputedProcessor(newOrderBatch)
			p1 := makeRemoteProcessor(p0)
			p2, err := colexec.NewAggregationProcessor(newOrderBatch.Schema, p1.Next)
			require.NoError(b, err)
			for p2.Next() != nil {
			}
		}
	})
	b.Run(`remote-reuse`, func(b *testing.B) {
		b.SetBytes(newOrderBatch.ApproxSize())
		p0 := makePrecomputedProcessor(newOrderBatch)
		p1 := makeRemoteProcessor(p0)
		p2, err := colexec.NewAggregationProcessor(newOrderBatch.Schema, p1.Next)
		for i := 0; i < b.N; i++ {
			p0.Reset()
			require.NoError(b, err)
			for p2.Next() != nil {
			}
		}
	})
}

type precomputedProcessor struct {
	batches []*arrow.RecordBatch
	idx     int
}

func makePrecomputedProcessor(batches ...*arrow.RecordBatch) *precomputedProcessor {
	return &precomputedProcessor{batches: batches}
}

func (p *precomputedProcessor) Reset() {
	p.idx = 0
}

func (p *precomputedProcessor) Next() *arrow.RecordBatch {
	if p.idx == len(p.batches) {
		return nil
	}
	batch := p.batches[p.idx]
	p.idx++
	return batch
}

type remoteProcessor struct {
	remote colexec.Processor
	buf    bytes.Buffer

	e   arrow.RecordBatchEncoder
	out arrow.RecordBatch
}

func makeRemoteProcessor(remote colexec.Processor) colexec.Processor {
	return &remoteProcessor{remote: remote}
}

func (p *remoteProcessor) Next() *arrow.RecordBatch {
	p.buf.Reset()
	remoteBatch := p.remote.Next()
	if remoteBatch == nil {
		return nil
	}
	p.e.Encode(&p.buf, remoteBatch)
	// TODO: Serialize/deserialize the schema, too.
	if err := p.out.Decode(remoteBatch.Schema, p.buf.Bytes()); err != nil {
		panic(fmt.Sprintf("%+v", err))
	}
	return &p.out
}
