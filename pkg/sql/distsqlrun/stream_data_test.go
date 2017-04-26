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
//
// Author: Radu Berinde (radu@cockroachlabs.com)

package distsqlrun

import (
	"fmt"
	"math/rand"
	"testing"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

// The encoder/decoder don't maintain the ordering between rows and metadata
// records.
func testGetDecodedRows(
	t *testing.T, sd *StreamDecoder, decodedRows sqlbase.EncDatumRows, metas []ProducerMetadata,
) (sqlbase.EncDatumRows, []ProducerMetadata) {
	for {
		row, meta, err := sd.GetRow(nil /* rowBuf */)
		if err != nil {
			t.Fatal(err)
		}
		if row == nil && meta.Empty() {
			break
		}
		if row != nil {
			decodedRows = append(decodedRows, row)
		} else {
			metas = append(metas, meta)
		}
	}
	return decodedRows, metas
}

func testRowStream(t *testing.T, rng *rand.Rand, records []rowOrMeta) {
	var se StreamEncoder
	var sd StreamDecoder

	var decodedRows sqlbase.EncDatumRows
	var metas []ProducerMetadata
	numRows := 0
	numMeta := 0

	for rowIdx := 0; rowIdx <= len(records); rowIdx++ {
		if rowIdx < len(records) {
			if records[rowIdx].row != nil {
				if err := se.AddRow(records[rowIdx].row); err != nil {
					t.Fatal(err)
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
				t.Fatal(err)
			}
			decodedRows, metas = testGetDecodedRows(t, &sd, decodedRows, metas)
		}
	}
	if len(metas) != numMeta {
		t.Errorf("expected %d metadata records, got: %d", numMeta, len(metas))
	}
	if len(decodedRows) != numRows {
		t.Errorf("expected %d rows, got: %d", numRows, len(decodedRows))
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
		info := make([]DatumInfo, rowLen)
		for i := range info {
			info[i].Type = sqlbase.RandColumnType(rng)
			info[i].Encoding = sqlbase.RandDatumEncoding(rng)
		}
		numRows := rng.Intn(100)
		rows := make([]rowOrMeta, numRows)
		for i := range rows {
			if rng.Intn(10) != 0 {
				rows[i].row = make(sqlbase.EncDatumRow, rowLen)
				for j := range rows[i].row {
					rows[i].row[j] = sqlbase.DatumToEncDatum(info[j].Type,
						sqlbase.RandDatum(rng, info[j].Type, true))
				}
			} else {
				rows[i].meta.Err = fmt.Errorf("test error %d", i)
			}
		}
		testRowStream(t, rng, rows)
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
	} else if !meta.Empty() || row != nil {
		t.Errorf("received bogus row %v %v", row, meta)
	}
}
