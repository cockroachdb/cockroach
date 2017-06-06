// Copyright 2017 The Cockroach Authors.
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
// Author: Arjun Narayan (arjun@cockroachlabs.com)
// Author: Alfonso Subiotto Marques (alfonso@cockroachlabs.com)

package distsqlrun

import (
	"bufio"
	"fmt"
	"io"
	"math/rand"
	"os"
	"os/exec"
	"testing"
	"time"

	"encoding/binary"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
)

type RowGenerator struct {
	rowsGenerated, maxRows int
	t                      []sqlbase.ColumnType
	rng                    *rand.Rand
}

func (rg *RowGenerator) Types() []sqlbase.ColumnType {
	return rg.t
}

func (rg *RowGenerator) Next() (sqlbase.EncDatumRow, ProducerMetadata) {
	if rg.rowsGenerated >= rg.maxRows {
		return nil, ProducerMetadata{}
	}

	row := make(sqlbase.EncDatumRow, len(rg.t))

	for i := range row {
		r := rg.rng.Int()
		datum := parser.NewDInt(parser.DInt(r))
		row[i] = sqlbase.DatumToEncDatum(rg.t[i], datum)
	}
	rg.rowsGenerated = rg.rowsGenerated + 1
	return row, ProducerMetadata{}
}

func (rg *RowGenerator) ConsumerDone() {
	rg.rowsGenerated = rg.maxRows
}

func (rg *RowGenerator) ConsumerClosed() {
	rg.rowsGenerated = rg.maxRows
}

var _ RowSource = &RowGenerator{}

type SortedRowSink struct {
	lastRow              sqlbase.EncDatumRow
	rowsSunk, rowsNeeded int
	sorted               bool
	ordering             sqlbase.ColumnOrdering
}

func (rs *SortedRowSink) Push(row sqlbase.EncDatumRow, meta ProducerMetadata) ConsumerStatus {
	rs.rowsSunk++
	if meta.Err != nil || meta.Ranges != nil {
		return ConsumerClosed
	}

	if row == nil {
		if rs.rowsSunk != rs.rowsNeeded {
			panic(fmt.Sprintf("need %d rows before closing, but only received %d rows", rs.rowsNeeded, rs.rowsSunk))
		}
		return ConsumerClosed
	}

	if rs.lastRow != nil {
		v, err := CompareEncDatumRowForMerge(rs.lastRow, row, rs.ordering, rs.ordering, &sqlbase.DatumAlloc{})
		if v == 1 && rs.sorted {
			panic(fmt.Sprintf("sort order violation: cmp(%v, %v) = %d > 0", rs.lastRow, row, v))
		}
		if err != nil {
			panic(err)
		}
	}

	rs.lastRow = row
	return NeedMoreRows
}

func (rs *SortedRowSink) ProducerDone() {

}

var _ RowReceiver = &SortedRowSink{}

func sortRocksDB(b *testing.B, input RowSource, out procOutputHelper) error {
	path := "./rocksBench"
	resetRocksBenchDir := func() {
		os.RemoveAll(path)
		os.Mkdir(path, 0700)
	}
	resetRocksBenchDir()

	cache := engine.NewRocksDBCache(0 /* size */)
	r, err := engine.NewRocksDB(
		roachpb.Attributes{},
		path,
		cache,
		0, /* maxSize */
		engine.DefaultMaxOpenFiles,
	)
	if err != nil {
		return err
	}

	defer func() {
		r.Close()
		resetRocksBenchDir()
	}()

	var writeBatch engine.Batch
	var batchWritten int
	batchSize := 10000

	writeBatch = r.NewWriteOnlyBatch()

	// Count only meaningful operations.
	b.StopTimer()
	b.ResetTimer()

	var count int
	var key roachpb.Key
	emptyVal := make([]byte, 0)
	for {
		row, meta := input.Next()
		count++
		if !meta.Empty() {
			return err
		}
		if row == nil {
			break
		}
		for _, val := range row {
			// Assume ascending. Note that the comparator isn't set.
			key, err = val.Encode(nil, sqlbase.DatumEncoding_ASCENDING_KEY, key)
			if err != nil {
				return err
			}
		}
		b.StartTimer()
		if batchWritten == batchSize {
			if err := writeBatch.Commit(false); err != nil {
				return err
			}
			batchWritten = 0
			writeBatch = r.NewWriteOnlyBatch()
		}
		if err := writeBatch.Put(engine.MVCCKey{Key: key}, emptyVal); err != nil {
			return err
		}
		batchWritten++
		b.StopTimer()
	}

	b.StartTimer()
	if err := writeBatch.Commit(false); err != nil {
		return err
	}
	b.StopTimer()

	i := r.NewIterator(false)
	defer i.Close()

	i.Seek(engine.NilKey)

	types := input.Types()
	for {
		ok, err := i.Valid()
		if err != nil {
			return err
		} else if !ok {
			return nil
		}
		key := i.Key().Key
		row := make([]sqlbase.EncDatum, len(types))
		for i, typ := range types {
			row[i], key, err = sqlbase.EncDatumFromBuffer(typ, sqlbase.DatumEncoding_ASCENDING_KEY, key)
			if err != nil {
				return err
			}
		}
		consumerStatus, err := out.emitRow(context.TODO(), row)
		if err != nil || consumerStatus != NeedMoreRows {
			return err
		}
		b.StartTimer()
		i.Next()
		b.StopTimer()
	}

	if err != nil {
		return err
	}

	return nil
}

func sortExternal(b *testing.B, input RowSource, out procOutputHelper) error {
	return external(b, input, out, true)
}

func readAndWriteFile(b *testing.B, input RowSource, out procOutputHelper) error {
	return external(b, input, out, false)
}

func external(b *testing.B, input RowSource, out procOutputHelper, sort bool) error {
	path := "external_sort"
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return errors.Wrapf(err, "could not open file: ")
	}
	defer func() {
		f.Close()
		os.RemoveAll(path)
	}()

	writer := bufio.NewWriter(f)
	// Count only meaningful operations.
	b.StopTimer()
	b.ResetTimer()
	for {
		row, meta := input.Next()
		if !meta.Empty() {
			return errors.Wrapf(err, "unexpected nonempty meta data: ")
		}
		if row == nil {
			break
		}
		var key roachpb.Key
		for _, val := range row {
			// Assume ascending. Note that the comparator isn't set.
			key, err = val.Encode(&sqlbase.DatumAlloc{}, sqlbase.DatumEncoding_ASCENDING_KEY, key)
			if err != nil {
				return errors.Wrapf(err, "failed encoding key: ")
			}
		}
		b.StartTimer()
		if err := binary.Write(writer, binary.BigEndian, key); err != nil {
			return errors.Wrapf(err, "failed writing key: ")
		}
		if _, err := writer.WriteString("\n"); err != nil {
			return errors.Wrapf(err, "failed writing newline: ")
		}
		b.StopTimer()
	}

	b.StartTimer()
	if err := writer.Flush(); err != nil {
		return errors.Wrapf(err, "failed flushing file: ")
	}
	b.StopTimer()

	// NOTE: Run `export LC_ALL='C'` to set the locale otherwise this command
	// will fail with illegal byte sequences.
	if sort {
		b.StartTimer()
		if err := exec.Command("sort", path, "-o", path).Run(); err != nil {
			return errors.Wrapf(err, "failed running sort: ")
		}
		b.StopTimer()
	}

	if _, err := f.Seek(0, 0); err != nil {
		return errors.Wrapf(err, "failed seeking to start of file: ")
	}

	buf := bufio.NewReader(f)
	types := input.Types()
	for {
		b.StartTimer()
		var i int64
		err := binary.Read(buf, binary.BigEndian, &i)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return errors.Wrapf(err, "could not read key")
		}
		_, err = buf.ReadBytes('\n')
		if err != nil {
			return errors.Wrapf(err, "could not read newline")
		}
		b.StopTimer()
		row := make([]sqlbase.EncDatum, len(types))
		for i, t := range types {
			datum := parser.NewDInt(parser.DInt(i))
			row[i] = sqlbase.DatumToEncDatum(t, datum)
		}
		consumerStatus, err := out.emitRow(context.TODO(), row)
		if err != nil || consumerStatus != NeedMoreRows {
			return errors.Wrapf(err, "failed emitting row to consumer: ")
		}
	}
	return nil
}

func BenchmarkDiskSorts(b *testing.B) {
	defer leaktest.AfterTest(b)()

	sortFuncs := []func(b *testing.B, input RowSource, out procOutputHelper) error{
		sortRocksDB,
		readAndWriteFile,
		sortExternal,
	}
	sortFuncsDebug := []string{
		"RocksDB with Batching",
		"Write and Read from file (no sorting)",
		"Write, Sort, and Read from file",
	}

	for _, maxRows := range []int{1000000, 3333333, 10000000, 33333333, 100000000} {
		for i, sortFunc := range sortFuncs {
			input := &RowGenerator{
				maxRows: maxRows,
				t: []sqlbase.ColumnType{
					sqlbase.ColumnType{Kind: sqlbase.ColumnType_INT},
				},
				rng: rand.New(rand.NewSource(int64(time.Now().UnixNano()))),
			}

			out := &SortedRowSink{
				ordering: []sqlbase.ColumnOrderInfo{
					sqlbase.ColumnOrderInfo{0, encoding.Ascending},
				},
			}
			// TODO(arjun): shamecube.gif
			if i != 1 {
				out.sorted = true
			}

			poh := procOutputHelper{}
			evalCtx := parser.MakeTestingEvalContext()
			poh.init(&PostProcessSpec{}, input.t, &evalCtx, out)

			b.Run(fmt.Sprintf("%40s: rows:%10d", sortFuncsDebug[i], maxRows), func(b *testing.B) {
				if err := sortFunc(b, input, poh); err != nil {
					b.Fatalf("error encountered: %v\n", err)
				}
			})
		}
	}
}
