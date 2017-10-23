// Copyright 2016 The Cockroach Authors.
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
	"math/rand"
	"testing"
	"time"

	"fmt"

	"strconv"

	"sync"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// Benchmark how long it takes to pass rows from one processor to another.

type DevRandRowSource struct {
	r             *rand.Rand
	rowsToGen     int
	cursor        int
	backingDatums []sqlbase.EncDatumRow
	types         []sqlbase.ColumnType
}

func (rs *DevRandRowSource) Init(types []sqlbase.ColumnType, rowsToGen int) {
	rs.r = rand.New(rand.NewSource(int64(time.Now().UnixNano())))
	rs.types = types
	rs.rowsToGen = rowsToGen
	rs.backingDatums = make([]sqlbase.EncDatumRow, rowsToGen)

	for i := range rs.backingDatums {
		rs.backingDatums[i] = genRandomEncDatumRow(types, rs.r)
	}
}

func genRandomEncDatum(t sqlbase.ColumnType, r *rand.Rand) sqlbase.EncDatum {
	var ed sqlbase.EncDatum
	var d parser.Datum
	switch t.SemanticType {
	case sqlbase.ColumnType_BOOL:
		b := r.Int31n(1)&0x01 == 0
		d = parser.MakeDBool(parser.DBool(b))
	case sqlbase.ColumnType_INT:
		i := r.Int31()
		d = parser.NewDInt(parser.DInt(i))
	case sqlbase.ColumnType_FLOAT:
		f := r.Float32()
		d = parser.NewDFloat(parser.DFloat(f))
	case sqlbase.ColumnType_STRING:
		i := r.Int63()
		s := strconv.Itoa(int(i))
		d = parser.NewDString(s)
	case sqlbase.ColumnType_BYTES:
		i := r.Int63()
		s := strconv.Itoa(int(i))
		d = parser.NewDBytes(parser.DBytes(s))
	default:
		panic(fmt.Sprintf("unsuported type: %v"))
	}

	ed = sqlbase.DatumToEncDatum(t, d)
	return ed
}

func genRandomEncDatumRow(ts []sqlbase.ColumnType, r *rand.Rand) sqlbase.EncDatumRow {
	row := make(sqlbase.EncDatumRow, len(ts))
	for i, t := range ts {
		row[i] = genRandomEncDatum(t, r)
	}
	return row
}

func (rs *DevRandRowSource) Types() []sqlbase.ColumnType {
	return rs.types
}

// DevRandRowSource.Next() returns the next row from the backing datums until
// there are no more rows. It never returns a non-nil
func (rs *DevRandRowSource) Next() (sqlbase.EncDatumRow, ProducerMetadata) {
	var row sqlbase.EncDatumRow

	if (rs.cursor + 1) >= rs.rowsToGen {
		return nil, ProducerMetadata{}
	}
	row = rs.backingDatums[rs.cursor]
	rs.cursor++
	return row, ProducerMetadata{}
}

func (rs *DevRandRowSource) ConsumerDone()   {}
func (rs *DevRandRowSource) ConsumerClosed() {}

var _ RowSource = &DevRandRowSource{}

type DevNullRowReceiver struct{}

func (rr *DevNullRowReceiver) Push(row sqlbase.EncDatumRow, meta ProducerMetadata) ConsumerStatus {
	if !meta.Empty() {
		panic(meta)
	}

	if row == nil {
		return ConsumerClosed
	}
	return NeedMoreRows
}

// DevNullRowReceiver can always eat more rows
func (rr *DevNullRowReceiver) ProducerDone() {}

type AddingRowReceiver struct {
	accum int
}

func (rr *AddingRowReceiver) Push(row sqlbase.EncDatumRow, meta ProducerMetadata) ConsumerStatus {
	if !meta.Empty() {
		panic(meta)
	}

	if row == nil {
		return ConsumerClosed
	}

	// ed := row[0]
	//rr.accum +=
	panic("not implemented yet")
	return NeedMoreRows
}

func (rr *AddingRowReceiver) ProducerDone() {}

var _ RowReceiver = &DevNullRowReceiver{}
var _ RowReceiver = &AddingRowReceiver{}

var allTheTypes []sqlbase.ColumnType = []sqlbase.ColumnType{
	{SemanticType: sqlbase.ColumnType_BOOL},
	{SemanticType: sqlbase.ColumnType_INT},
	{SemanticType: sqlbase.ColumnType_FLOAT},
	{SemanticType: sqlbase.ColumnType_STRING},
	{SemanticType: sqlbase.ColumnType_BYTES},
}

func TestGenerateRows(t *testing.T) {
	var d DevRandRowSource
	d.Init(allTheTypes, 1000)
}

func BenchmarkGeneratingRows(b *testing.B) {
	for _, rows := range []int{1000, 10000, 100000} {
		b.Run(strconv.Itoa(rows), func(b *testing.B) {
			var d DevRandRowSource
			for i := 0; i < b.N; i++ {
				d.Init(allTheTypes, rows)
			}
		})
	}
}

func eaterOfRows(sink RowReceiver, channel RowChannel) {
	for {
		d, ok := <-channel.C
		if !ok {
			panic("received non-ok message over channel")
		}
		if !d.Meta.Empty() {
			panic(d.Meta)
		}

		status := sink.Push(d.Row, d.Meta)
		if status == ConsumerClosed {
			channel.ConsumerDone()
			break
		}
	}
}

func pusherOfRows(source RowSource, channel RowChannel) {
	for {
		row, meta := source.Next()
		if !meta.Empty() {
			panic(meta)
		}

		status := channel.Push(row, meta)
		if status == ConsumerClosed || row == nil {
			break
		}
	}
}

func TestSerializingRows(t *testing.T) {
	var source DevRandRowSource
	var sink DevNullRowReceiver

	anInt := []sqlbase.ColumnType{{SemanticType: sqlbase.ColumnType_INT}}
	source.Init(anInt, 1000)

	var channel RowChannel
	channel.Init(anInt)

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		eaterOfRows(&sink, channel)
	}()

	go func() {
		defer wg.Done()
		pusherOfRows(&source, channel)
	}()

	wg.Wait()
}

func BenchmarkSerializingRows(b *testing.B) {
	for _, rows := range []int{100, 1000, 10000, 100000, 1000000} {
		b.Run(fmt.Sprintf("BenchmarkSerializingRows%d", rows), func(b *testing.B) {
			var source DevRandRowSource
			anInt := []sqlbase.ColumnType{{SemanticType: sqlbase.ColumnType_INT}}
			source.Init(anInt, rows)
			var channel RowChannel
			channel.Init(anInt)

			var sink DevNullRowReceiver

			for i := 0; i < b.N; i++ {
				source.cursor = 0
				var wg sync.WaitGroup
				wg.Add(2)
				go func() {
					defer wg.Done()
					eaterOfRows(&sink, channel)
				}()

				go func() {
					defer wg.Done()
					pusherOfRows(&source, channel)
				}()
				wg.Wait()
			}
			b.SetBytes(int64(8 * rows))
		})
	}
}
