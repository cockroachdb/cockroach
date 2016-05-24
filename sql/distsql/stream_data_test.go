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

package distsql

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/sql/sqlbase"
	"github.com/cockroachdb/cockroach/util/randutil"
)

func testGetDecodedRows(
	t *testing.T, sd *StreamDecoder, decodedRows sqlbase.EncDatumRows,
) sqlbase.EncDatumRows {
	for {
		decoded, err := sd.GetRow(nil)
		if err != nil {
			t.Fatal(err)
		}
		if decoded == nil {
			break
		}
		decodedRows = append(decodedRows, decoded)
	}
	return decodedRows
}

func testRowStream(t *testing.T, rng *rand.Rand, rows sqlbase.EncDatumRows, trailerErr error) {
	var se StreamEncoder
	var sd StreamDecoder

	var decodedRows sqlbase.EncDatumRows

	for rowIdx := 0; rowIdx <= len(rows); rowIdx++ {
		if done, _ := sd.IsDone(); done {
			t.Fatal("StreamDecoder is done early")
		}
		if rowIdx < len(rows) {
			if err := se.AddRow(rows[rowIdx]); err != nil {
				t.Fatal(err)
			}
		}
		// "Send" a message every now and then and once at the end.
		final := (rowIdx == len(rows))
		if final || (rowIdx > 0 && rng.Intn(10) == 0) {
			msg := se.FormMessage(final, trailerErr)
			// Make a copy of the data buffer.
			msg.Data.RawBytes = append([]byte(nil), msg.Data.RawBytes...)
			err := sd.AddMessage(msg)
			if err != nil {
				t.Fatal(err)
			}
			decodedRows = testGetDecodedRows(t, &sd, decodedRows)
		}
	}
	done, endErr := sd.IsDone()
	if !done {
		t.Fatalf("StramDecoder not done")
	}
	if (endErr == nil) != (trailerErr == nil) ||
		(endErr != nil && trailerErr != nil && endErr.Error() != trailerErr.Error()) {
		t.Fatalf("invalid trailer error: '%s', expected '%s'", endErr, trailerErr)
	}
}

// TestStreamEncodeDecode generates random streams of EncDatums and passes them
// through a StreamEncoder and a StreamDecoder
func TestStreamEncodeDecode(t *testing.T) {
	rng, _ := randutil.NewPseudoRand()
	for test := 0; test < 100; test++ {
		rowLen := 1 + rng.Intn(20)
		info := make([]DatumInfo, rowLen)
		for i := range info {
			info[i].Type = sqlbase.RandColumnType(rng)
			info[i].Encoding = sqlbase.RandDatumEncoding(rng)
		}
		numRows := rng.Intn(100)
		rows := make(sqlbase.EncDatumRows, numRows)
		for i := range rows {
			rows[i] = make(sqlbase.EncDatumRow, rowLen)
			for j := range rows[i] {
				rows[i][j].SetDatum(info[j].Type, sqlbase.RandDatum(rng, info[j].Type, true))
			}
		}
		var trailerErr error
		if rng.Intn(10) == 0 {
			trailerErr = fmt.Errorf("test error %d", rng.Intn(100))
		}
		testRowStream(t, rng, rows, trailerErr)
	}
}
