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
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/distsqlpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

// The encoder/decoder don't maintain the ordering between rows and metadata
// records.
func testGetDecodedRows(
	tb testing.TB, sd *StreamDecoder, decodedRows sqlbase.EncDatumRows, metas []ProducerMetadata,
) (sqlbase.EncDatumRows, []ProducerMetadata) {
	for {
		row, meta, err := sd.GetRow(nil /* rowBuf */)
		if err != nil {
			tb.Fatal(err)
		}
		if row == nil && meta == nil {
			break
		}
		if row != nil {
			decodedRows = append(decodedRows, row)
		} else {
			metas = append(metas, *meta)
		}
	}
	return decodedRows, metas
}

func testRowStream(tb testing.TB, rng *rand.Rand, types []types.T, records []rowOrMeta) {
	var se StreamEncoder
	var sd StreamDecoder

	var decodedRows sqlbase.EncDatumRows
	var metas []ProducerMetadata
	numRows := 0
	numMeta := 0

	se.init(types)

	for rowIdx := 0; rowIdx <= len(records); rowIdx++ {
		if rowIdx < len(records) {
			if records[rowIdx].row != nil {
				if err := se.AddRow(records[rowIdx].row); err != nil {
					tb.Fatal(err)
				}
				numRows++
			} else {
				se.AddMetadata(records[rowIdx].meta)
				numMeta++
			}
		}
		// "Send" a message every now and then and once at the end.
		final := (rowIdx == len(records))
		if final || (rowIdx > 0 && rng.Intn(10) == 0) {
			msg := se.FormMessage(context.TODO())
			// Make a copy of the data buffer.
			msg.Data.RawBytes = append([]byte(nil), msg.Data.RawBytes...)
			err := sd.AddMessage(msg)
			if err != nil {
				tb.Fatal(err)
			}
			decodedRows, metas = testGetDecodedRows(tb, &sd, decodedRows, metas)
		}
	}
	if len(metas) != numMeta {
		tb.Errorf("expected %d metadata records, got: %d", numMeta, len(metas))
	}
	if len(decodedRows) != numRows {
		tb.Errorf("expected %d rows, got: %d", numRows, len(decodedRows))
	}
}

type rowOrMeta struct {
	row  sqlbase.EncDatumRow
	meta ProducerMetadata
}

// TestStreamEncodeDecode generates random streams of EncDatums and passes them
// through a StreamEncoder and a StreamDecoder
func TestStreamEncodeDecode(t *testing.T) {
	defer leaktest.AfterTest(t)()
	rng, _ := randutil.NewPseudoRand()
	for test := 0; test < 100; test++ {
		rowLen := rng.Intn(20)
		types := sqlbase.RandColumnTypes(rng, rowLen)
		info := make([]distsqlpb.DatumInfo, rowLen)
		for i := range info {
			info[i].Type = types[i]
			info[i].Encoding = sqlbase.RandDatumEncoding(rng)
		}
		numRows := rng.Intn(100)
		rows := make([]rowOrMeta, numRows)
		for i := range rows {
			if rng.Intn(10) != 0 {
				rows[i].row = make(sqlbase.EncDatumRow, rowLen)
				for j := range rows[i].row {
					rows[i].row[j] = sqlbase.DatumToEncDatum(&info[j].Type,
						sqlbase.RandDatum(rng, &info[j].Type, true))
				}
			} else {
				rows[i].meta.Err = fmt.Errorf("test error %d", i)
			}
		}
		testRowStream(t, rng, types, rows)
	}
}

func TestEmptyStreamEncodeDecode(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var se StreamEncoder
	var sd StreamDecoder
	msg := se.FormMessage(context.TODO())
	if err := sd.AddMessage(msg); err != nil {
		t.Fatal(err)
	}
	if msg.Header == nil {
		t.Errorf("no header in first message")
	}
	if row, meta, err := sd.GetRow(nil /* rowBuf */); err != nil {
		t.Fatal(err)
	} else if meta != nil || row != nil {
		t.Errorf("received bogus row %v %v", row, meta)
	}
}

func BenchmarkStreamEncoder(b *testing.B) {
	numRows := 1 << 16

	for _, numCols := range []int{1, 4, 16, 64} {
		b.Run(fmt.Sprintf("rows=%d,cols=%d", numRows, numCols), func(b *testing.B) {
			b.SetBytes(int64(numRows * numCols * 8))
			cols := sqlbase.MakeIntCols(numCols)
			input := NewRepeatableRowSource(cols, sqlbase.MakeIntRows(numRows, numCols))

			b.ResetTimer()
			ctx := context.Background()

			for i := 0; i < b.N; i++ {
				b.StopTimer()
				input.Reset()
				// Reset the EncDatums' encoded bytes cache.
				for _, row := range input.rows {
					for j := range row {
						row[j] = sqlbase.EncDatum{
							Datum: row[j].Datum,
						}
					}
				}
				var se StreamEncoder
				se.init(cols)
				b.StartTimer()

				// Add rows to the StreamEncoder until the input source is exhausted.
				// "Flush" every outboxBufRows.
				for j := 0; ; j++ {
					row, _ := input.Next()
					if row == nil {
						break
					}
					if err := se.AddRow(row); err != nil {
						b.Fatal(err)
					}
					if j%outboxBufRows == 0 {
						// ignore output
						se.FormMessage(ctx)
					}
				}
			}
		})
	}
}

func BenchmarkStreamDecoder(b *testing.B) {
	ctx := context.Background()

	for _, numCols := range []int{1, 4, 16, 64} {
		b.Run(fmt.Sprintf("cols=%d", numCols), func(b *testing.B) {
			b.SetBytes(int64(outboxBufRows * numCols * 8))
			var se StreamEncoder
			colTypes := sqlbase.MakeIntCols(numCols)
			se.init(colTypes)
			inRow := sqlbase.MakeIntRows(1, numCols)[0]
			for i := 0; i < outboxBufRows; i++ {
				if err := se.AddRow(inRow); err != nil {
					b.Fatal(err)
				}
			}
			msg := se.FormMessage(ctx)

			for i := 0; i < b.N; i++ {
				var sd StreamDecoder
				if err := sd.AddMessage(msg); err != nil {
					b.Fatal(err)
				}
				for j := 0; j < outboxBufRows; j++ {
					row, meta, err := sd.GetRow(nil)
					if err != nil {
						b.Fatal(err)
					}
					if row == nil && meta == nil {
						break
					}
				}
			}
		})
	}
}
