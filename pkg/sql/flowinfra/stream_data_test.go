// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package flowinfra_test

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/flowinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

// The encoder/decoder don't maintain the ordering between rows and metadata
// records.
func testGetDecodedRows(
	tb testing.TB,
	sd *flowinfra.StreamDecoder,
	decodedRows rowenc.EncDatumRows,
	metas []execinfrapb.ProducerMetadata,
) (rowenc.EncDatumRows, []execinfrapb.ProducerMetadata) {
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

func testRowStream(tb testing.TB, rng *rand.Rand, types []*types.T, records []rowOrMeta) {
	var se flowinfra.StreamEncoder
	var sd flowinfra.StreamDecoder

	var decodedRows rowenc.EncDatumRows
	var metas []execinfrapb.ProducerMetadata
	numRows := 0
	numMeta := 0

	se.Init(types)

	for rowIdx := 0; rowIdx <= len(records); rowIdx++ {
		if rowIdx < len(records) {
			if records[rowIdx].row != nil {
				if err := se.AddRow(records[rowIdx].row); err != nil {
					tb.Fatal(err)
				}
				numRows++
			} else {
				se.AddMetadata(context.Background(), records[rowIdx].meta)
				numMeta++
			}
		}
		// "Send" a message every now and then and once at the end.
		final := (rowIdx == len(records))
		if final || (rowIdx > 0 && rng.Intn(10) == 0) {
			msg := se.FormMessage(context.Background())
			// Make a copy of the data buffer.
			msg.Data.RawBytes = append([]byte(nil), msg.Data.RawBytes...)
			err := sd.AddMessage(context.Background(), msg)
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
	row  rowenc.EncDatumRow
	meta execinfrapb.ProducerMetadata
}

// TestStreamEncodeDecode generates random streams of EncDatums and passes them
// through a StreamEncoder and a StreamDecoder
func TestStreamEncodeDecode(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	rng, _ := randutil.NewPseudoRand()
	for test := 0; test < 100; test++ {
		rowLen := rng.Intn(20)
		types := randgen.RandEncodableColumnTypes(rng, rowLen)
		info := make([]execinfrapb.DatumInfo, rowLen)
		for i := range info {
			info[i].Type = types[i]
			info[i].Encoding = randgen.RandDatumEncoding(rng)
		}
		numRows := rng.Intn(100)
		rows := make([]rowOrMeta, numRows)
		for i := range rows {
			if rng.Intn(10) != 0 {
				rows[i].row = make(rowenc.EncDatumRow, rowLen)
				for j := range rows[i].row {
					rows[i].row[j] = rowenc.DatumToEncDatum(info[j].Type,
						randgen.RandDatum(rng, info[j].Type, true))
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
	defer log.Scope(t).Close(t)
	var se flowinfra.StreamEncoder
	var sd flowinfra.StreamDecoder
	msg := se.FormMessage(context.Background())
	if err := sd.AddMessage(context.Background(), msg); err != nil {
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
	defer log.Scope(b).Close(b)
	numRows := 1 << 16

	for _, numCols := range []int{1, 4, 16, 64} {
		b.Run(fmt.Sprintf("rows=%d,cols=%d", numRows, numCols), func(b *testing.B) {
			b.SetBytes(int64(numRows * numCols * 8))
			cols := types.MakeIntCols(numCols)
			rows := randgen.MakeIntRows(numRows, numCols)
			input := execinfra.NewRepeatableRowSource(cols, rows)

			b.ResetTimer()
			ctx := context.Background()

			for i := 0; i < b.N; i++ {
				b.StopTimer()
				input.Reset()
				// Reset the EncDatums' encoded bytes cache.
				for _, row := range rows {
					for j := range row {
						row[j] = rowenc.EncDatum{
							Datum: row[j].Datum,
						}
					}
				}
				var se flowinfra.StreamEncoder
				se.Init(cols)
				b.StartTimer()

				// Add rows to the StreamEncoder until the input source is exhausted.
				// "Flush" every OutboxBufRows.
				for j := 0; ; j++ {
					row, _ := input.Next()
					if row == nil {
						break
					}
					if err := se.AddRow(row); err != nil {
						b.Fatal(err)
					}
					if j%flowinfra.OutboxBufRows == 0 {
						// ignore output
						se.FormMessage(ctx)
					}
				}
			}
		})
	}
}

func BenchmarkStreamDecoder(b *testing.B) {
	defer log.Scope(b).Close(b)
	ctx := context.Background()

	for _, numCols := range []int{1, 4, 16, 64} {
		b.Run(fmt.Sprintf("cols=%d", numCols), func(b *testing.B) {
			b.SetBytes(int64(flowinfra.OutboxBufRows * numCols * 8))
			var se flowinfra.StreamEncoder
			colTypes := types.MakeIntCols(numCols)
			se.Init(colTypes)
			inRow := randgen.MakeIntRows(1, numCols)[0]
			for i := 0; i < flowinfra.OutboxBufRows; i++ {
				if err := se.AddRow(inRow); err != nil {
					b.Fatal(err)
				}
			}
			msg := se.FormMessage(ctx)

			for i := 0; i < b.N; i++ {
				var sd flowinfra.StreamDecoder
				if err := sd.AddMessage(ctx, msg); err != nil {
					b.Fatal(err)
				}
				for j := 0; j < flowinfra.OutboxBufRows; j++ {
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
