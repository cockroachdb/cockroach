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
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/dgraph-io/badger/badger"
	"github.com/elastic/gosigar"
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
		if v != -1 && rs.sorted {
			panic(fmt.Sprintf("sort order violation: cmp(%v, %v) = %d != -1", rs.lastRow, row, v))
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

func sortRocksDB(t *testing.T, input RowSource, out procOutputHelper) error {
	return sortRocksDBHelper(t, input, out, false)
}

func sortRocksDBWithBatching(t *testing.T, input RowSource, out procOutputHelper) error {
	return sortRocksDBHelper(t, input, out, true)
}

func sortRocksDBHelper(t *testing.T, input RowSource, out procOutputHelper, batching bool) error {
	path := "./rocksBench"
	os.RemoveAll(path)
	os.Mkdir(path, 0700)

	// Use as much disk space as possible.
	fsu := gosigar.FileSystemUsage{}
	if err := fsu.Get(path); err != nil {
		return errors.New("could not get filesystem usage")
	}
	cache := engine.NewRocksDBCache(0 /* size */)
	r, err := engine.NewRocksDB(
		roachpb.Attributes{},
		path,
		cache,
		int64(fsu.Total),
		10000, /* open file limit */
	)
	if err != nil {
		return err
	}

	defer func() {
		r.Close()
		os.RemoveAll(path)
		os.Mkdir(path, 0700)
	}()

	var writeBatch engine.Batch
	var batchWritten int
	batchSize := 100

	if batching {
		writeBatch = r.NewWriteOnlyBatch()
	}

	var count int
	for {
		row, meta := input.Next()
		count++
		if !meta.Empty() {
			return err
		}
		if row == nil {
			break
		}
		var key roachpb.Key
		for _, val := range row {
			// Assume ascending. Note that the comparator isn't set.
			key, err = val.Encode(&sqlbase.DatumAlloc{}, sqlbase.DatumEncoding_ASCENDING_KEY, key)
			if err != nil {
				return err
			}
		}
		if batching {
			if batchWritten == batchSize {
				if err := writeBatch.Commit(true); err != nil {
					return err
				}
			}
			if err := writeBatch.Put(engine.MVCCKey{Key: key}, make([]byte, 0)); err != nil {
				return err
			}
			batchWritten++
		} else {
			if err := r.Put(engine.MVCCKey{Key: key}, make([]byte, 0)); err != nil {
				return err
			}
		}
	}

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
		i.Next()
	}

	if err != nil {
		return err
	}

	return nil
}

func sortExternal(t *testing.T, input RowSource, out procOutputHelper) error {
	return external(t, input, out, true)
}

func readAndWriteFile(t *testing.T, input RowSource, out procOutputHelper) error {
	return external(t, input, out, false)
}

func external(t *testing.T, input RowSource, out procOutputHelper, sort bool) error {
	path := "external_sort"
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	defer func() {
		f.Close()
		os.RemoveAll(path)
	}()

	for {
		row, meta := input.Next()
		if !meta.Empty() {
			return err
		}
		if row == nil {
			break
		}
		var key roachpb.Key
		for _, val := range row {
			// Assume ascending. Note that the comparator isn't set.
			key, err = val.Encode(&sqlbase.DatumAlloc{}, sqlbase.DatumEncoding_ASCENDING_KEY, key)
			if err != nil {
				return err
			}
		}
		f.Write([]byte(hex.EncodeToString(key)))
		// Delimiter.
		f.Write([]byte{0})
	}

	// NOTE: Run `export LC_ALL='C'` to set the locale otherwise this command
	// will fail with illegal byte sequences.
	if sort {
		if err := exec.Command("sort", "-z", path, "-o", path).Run(); err != nil {
			return err
		}
	}

	if _, err := f.Seek(0, 0); err != nil {
		return err
	}

	buf := bufio.NewReader(f)
	types := input.Types()
	for {
		hexBytes, err := buf.ReadBytes(byte(0))
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		// Remove delimiter.
		hexBytes = hexBytes[:len(hexBytes)-1]
		key, err := hex.DecodeString(string(hexBytes))
		if err != nil {
			return err
		}
		row := make([]sqlbase.EncDatum, len(types))
		for i, t := range types {
			row[i], key, err = sqlbase.EncDatumFromBuffer(t, sqlbase.DatumEncoding_ASCENDING_KEY, key)
			if err != nil {
				return err
			}
		}
		consumerStatus, err := out.emitRow(context.TODO(), row)
		if err != nil || consumerStatus != NeedMoreRows {
			return err
		}
	}
	return nil
}

func sortBadger(t *testing.T, input RowSource, out procOutputHelper) error {
	path := "badger_bench"
	opts := badger.DefaultOptions
	opts.Dir = path
	b, err := badger.NewKV(&badger.DefaultOptions)
	if err != nil {
		return err
	}
	defer func() {
		b.Close()
		os.RemoveAll(path)
		os.Mkdir(path, 0700)
	}()

	for {
		row, meta := input.Next()
		if !meta.Empty() {
			return err
		}
		if row == nil {
			break
		}

		if row == nil {
			break
		}
		var key roachpb.Key
		for _, val := range row {
			// Assume ascending. Note that the comparator isn't set.
			key, err = val.Encode(&sqlbase.DatumAlloc{}, sqlbase.DatumEncoding_ASCENDING_KEY, key)
			if err != nil {
				return err
			}
		}
		entries := []*badger.Entry{
			&badger.Entry{
				Key:   key,
				Value: nil,
			},
		}
		b.BatchSet(entries)
	}

	// Explicitly unset tuning for reading. In a real scenario we would
	// probably make use of the prefetch size (set dynamically?) and set
	// FetchValues to false when we aren't using the storage solution as an
	// on-disk map.
	i := b.NewIterator(badger.IteratorOptions{
		PrefetchSize: 0,
		FetchValues:  true,
		Reverse:      false,
	})
	defer i.Close()

	types := input.Types()
	for i.Rewind(); i.Valid(); i.Next() {
		key := i.Item().Key()
		row := make([]sqlbase.EncDatum, len(types))
		for i, t := range types {
			row[i], key, err = sqlbase.EncDatumFromBuffer(t, sqlbase.DatumEncoding_ASCENDING_KEY, key)
			if err != nil {
				return err
			}
		}
		consumerStatus, err := out.emitRow(context.TODO(), row)
		if err != nil || consumerStatus != NeedMoreRows {
			return err
		}
	}

	return nil
}

func TestDiskSorts(t *testing.T) {
	defer leaktest.AfterTest(t)()

	sortFuncs := []func(t *testing.T, input RowSource, out procOutputHelper) error{
		sortRocksDB,
		sortRocksDBWithBatching,
		readAndWriteFile,
		sortExternal,
		//		sortBadger,
	}
	sortFuncsDebug := []string{
		"RocksDB (no batching)",
		"RocksDB with Batching",
		"Write and Read from file (no sorting)",
		"Write, Sort, and Read from file",
	}

	for _, maxRows := range []int{100000, 1000000, 3000000, 10000000, 30000000, 100000000} {
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
			if i != 2 {
				out.sorted = true
			}

			poh := procOutputHelper{}
			evalCtx := parser.MakeTestingEvalContext()
			poh.init(&PostProcessSpec{}, input.t, &evalCtx, out)

			start := time.Now()
			if err := sortFunc(t, input, poh); err != nil {
				t.Fatalf("error encountered: %v\n", err)
			}
			log.Infof(context.TODO(), "%40s: rows:%10d elapsed: %20d", sortFuncsDebug[i], maxRows, time.Since(start))
		}
	}
}
