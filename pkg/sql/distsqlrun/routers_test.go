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
// Author: Irfan Sharif (irfansharif@cockroachlabs.com)
// Author: Andrei Matei (andreimatei1@gmail.com)

package distsqlrun

import (
	"bytes"
	"testing"

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

func TestHashRouter(t *testing.T) {
	defer leaktest.AfterTest(t)()
	const numCols = 6
	const numRows = 200

	rng, _ := randutil.NewPseudoRand()
	alloc := &sqlbase.DatumAlloc{}

	// Generate tables of possible values for each column; we have fewer possible
	// values than rows to guarantee many occurrences of each value.
	vals := sqlbase.RandEncDatumSlices(rng, numCols, numRows/10)

	testCases := []struct {
		hashColumns []uint32
		numBuckets  int
	}{
		{[]uint32{0}, 2},
		{[]uint32{3}, 4},
		{[]uint32{1, 3}, 4},
		{[]uint32{5, 2}, 3},
		{[]uint32{0, 1, 2, 3, 4}, 5},
	}

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			bufs := make([]*RowBuffer, tc.numBuckets)
			recvs := make([]RowReceiver, tc.numBuckets)
			for i := 0; i < tc.numBuckets; i++ {
				bufs[i] = &RowBuffer{}
				recvs[i] = bufs[i]
			}
			hr, err := makeHashRouter(tc.hashColumns, recvs)
			if err != nil {
				t.Fatal(err)
			}

			for i := 0; i < numRows; i++ {
				row := make(sqlbase.EncDatumRow, numCols)
				for j := 0; j < numCols; j++ {
					row[j] = vals[j][rng.Intn(len(vals[j]))]
				}
				if status := hr.Push(row, ProducerMetadata{}); status != NeedMoreRows {
					t.Fatalf("unexpected status: %d", status)
				}
			}
			hr.ProducerDone()

			rows := make([]sqlbase.EncDatumRows, len(bufs))
			for i, b := range bufs {
				if !b.ProducerClosed {
					t.Fatalf("bucket not closed: %d", i)
				}
				rows[i] = getRowsFromBuffer(t, b)
			}

			for bIdx := range rows {
				for _, row := range rows[bIdx] {
					// Verify there are no rows that
					//  - have the same values with this row on all the hashColumns, and
					//  - ended up in a different bucket
					for b2Idx, r2 := range rows {
						if b2Idx == bIdx {
							continue
						}
						for _, row2 := range r2 {
							equal := true
							for _, c := range tc.hashColumns {
								cmp, err := row[c].Compare(alloc, &row2[c])
								if err != nil {
									t.Fatal(err)
								}
								if cmp != 0 {
									equal = false
									break
								}
							}
							if equal {
								t.Errorf("rows %s and %s in different buckets", row, row2)
							}
						}
					}
				}
			}
		})
	}
}

func getRowsFromBuffer(t *testing.T, buf *RowBuffer) sqlbase.EncDatumRows {
	var res sqlbase.EncDatumRows
	for {
		row, meta := buf.Next()
		if !meta.Empty() {
			t.Fatalf("unexpected metadata: %v", meta)
		}
		if row == nil {
			break
		}
		res = append(res, row)
	}
	return res
}

func TestMirrorRouter(t *testing.T) {
	defer leaktest.AfterTest(t)()
	const numCols = 6
	const numRows = 20

	rng, _ := randutil.NewPseudoRand()
	alloc := &sqlbase.DatumAlloc{}

	vals := sqlbase.RandEncDatumSlices(rng, numCols, numRows)

	for numBuckets := 2; numBuckets <= 4; numBuckets++ {
		bufs := make([]*RowBuffer, numBuckets)
		recvs := make([]RowReceiver, numBuckets)
		for i := 0; i < numBuckets; i++ {
			bufs[i] = &RowBuffer{}
			recvs[i] = bufs[i]
		}
		mr, err := makeMirrorRouter(recvs)
		if err != nil {
			t.Fatal(err)
		}

		for i := 0; i < numRows; i++ {
			row := make(sqlbase.EncDatumRow, numCols)
			for j := 0; j < numCols; j++ {
				row[j] = vals[j][rng.Intn(len(vals[j]))]
			}
			if status := mr.Push(row, ProducerMetadata{}); status != NeedMoreRows {
				t.Fatalf("unexpected status: %d", status)
			}
		}
		mr.ProducerDone()

		rows := make([]sqlbase.EncDatumRows, len(bufs))
		for i, b := range bufs {
			if !b.ProducerClosed {
				t.Fatalf("bucket not closed: %d", i)
			}
			rows[i] = getRowsFromBuffer(t, b)
		}

		// Verify each row is sent to each of the output streams.
		for bIdx, r := range rows {
			if bIdx == 0 {
				continue
			}
			if len(rows[bIdx]) != len(rows[0]) {
				t.Errorf("buckets %d and %d have different number of rows", 0, bIdx)
			}

			// Verify that the i-th row is the same across all buffers.
			for i, row := range r {
				row2 := rows[0][i]

				equal := true
				for j, c := range row {
					cmp, err := c.Compare(alloc, &row2[j])
					if err != nil {
						t.Fatal(err)
					}
					if cmp != 0 {
						equal = false
						break
					}
				}
				if !equal {
					t.Errorf("rows %s and %s found in one bucket and not the other", row, row2)
				}
			}
		}
	}
}

// Test that the correct status is returned to producers: NeedMoreRows should be
// returned while there's at least one consumer that's not draining, then
// DrainRequested should be returned while there's at least one consumer that's
// not closed, and ConsumerClosed should be returned afterwards.
func TestConsumerStatus(t *testing.T) {
	defer leaktest.AfterTest(t)()
	mirrorRouterFactory := func() (RowReceiver, []RowSource, error) {
		bufs := make([]*RowBuffer, 2)
		recvs := make([]RowReceiver, 2)
		srcs := make([]RowSource, 2)
		for i := 0; i < 2; i++ {
			bufs[i] = &RowBuffer{}
			recvs[i] = bufs[i]
			srcs[i] = bufs[i]
		}
		mr, err := makeMirrorRouter(recvs)
		return mr, srcs, err
	}
	hashRouterFactory := func() (RowReceiver, []RowSource, error) {
		bufs := make([]*RowBuffer, 2)
		recvs := make([]RowReceiver, 2)
		srcs := make([]RowSource, 2)
		for i := 0; i < 2; i++ {
			bufs[i] = &RowBuffer{}
			recvs[i] = bufs[i]
			srcs[i] = bufs[i]
		}
		mr, err := makeHashRouter([]uint32{0} /* hashCols */, recvs)
		return mr, srcs, err
	}

	testCases := []struct {
		name          string
		routerFactory func() (RowReceiver, []RowSource, error)
	}{
		{"MirrorRouter", mirrorRouterFactory},
		{"HashRouter", hashRouterFactory},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			router, srcs, err := tc.routerFactory()
			if err != nil {
				t.Fatal(err)
			}
			// row0 will be a row that the hash router sends to the first stream, row1
			// to the 2nd stream.
			var row0, row1 sqlbase.EncDatumRow
			if hr, ok := router.(*hashRouter); ok {
				var err error
				row0, err = preimageAttack(hr, 0, len(srcs))
				if err != nil {
					t.Fatal(err)
				}
				row1, err = preimageAttack(hr, 1, len(srcs))
				if err != nil {
					t.Fatal(err)
				}
			} else {
				rng, _ := randutil.NewPseudoRand()
				vals := sqlbase.RandEncDatumSlices(rng, 1 /* numSets */, 1 /* numValsPerSet */)
				row0 = vals[0]
				row1 = row0
			}

			// Push a row and expect NeedMoreRows.
			consumerStatus := router.Push(row0, ProducerMetadata{})
			if consumerStatus != NeedMoreRows {
				t.Fatalf("expected status %d, got: %d", NeedMoreRows, consumerStatus)
			}

			// Start draining stream 0. Keep expecting NeedMoreRows, regardless on
			// which stream we send.
			srcs[0].ConsumerDone()
			consumerStatus = router.Push(row0, ProducerMetadata{})
			if consumerStatus != NeedMoreRows {
				t.Fatalf("expected status %d, got: %d", NeedMoreRows, consumerStatus)
			}
			consumerStatus = router.Push(row1, ProducerMetadata{})
			if consumerStatus != NeedMoreRows {
				t.Fatalf("expected status %d, got: %d", NeedMoreRows, consumerStatus)
			}

			// Close stream 0. Continue to expect NeedMoreRows.
			srcs[0].ConsumerClosed()
			consumerStatus = router.Push(row0, ProducerMetadata{})
			if consumerStatus != NeedMoreRows {
				t.Fatalf("expected status %d, got: %d", NeedMoreRows, consumerStatus)
			}
			consumerStatus = router.Push(row1, ProducerMetadata{})
			if consumerStatus != NeedMoreRows {
				t.Fatalf("expected status %d, got: %d", NeedMoreRows, consumerStatus)
			}

			// Start draining stream 1. Now that all streams are draining, expect
			// DrainRequested.
			srcs[1].ConsumerDone()
			consumerStatus = router.Push(row1, ProducerMetadata{})
			if consumerStatus != DrainRequested {
				t.Fatalf("expected status %d, got: %d", DrainRequested, consumerStatus)
			}

			// Close stream 1. Everything's closed now, but the routers currently
			// only detect this when trying to send metadata - so we still expect
			// DrainRequested.
			srcs[1].ConsumerClosed()
			consumerStatus = router.Push(row1, ProducerMetadata{})
			if consumerStatus != DrainRequested {
				t.Fatalf("expected status %d, got: %d", DrainRequested, consumerStatus)
			}

			// Attempt to send some metadata. This will cause the router to observe
			// that everything's closed now.
			srcs[1].ConsumerClosed()
			consumerStatus = router.Push(
				nil /* row */, ProducerMetadata{Err: errors.Errorf("test error")})
			if consumerStatus != ConsumerClosed {
				t.Fatalf("expected status %d, got: %d", ConsumerClosed, consumerStatus)
			}
		})
	}
}

// preimageAttack finds a row that hashes to a particular output stream. It's
// assumed that hr is configured for rows with one column.
func preimageAttack(hr *hashRouter, streamIdx int, numStreams int) (sqlbase.EncDatumRow, error) {
	rng, _ := randutil.NewPseudoRand()
	for {
		vals := sqlbase.RandEncDatumSlices(rng, 1 /* numSets */, 1 /* numValsPerSet */)
		curStreamIdx, err := hr.computeDestination(vals[0])
		if err != nil {
			return nil, err
		}
		if curStreamIdx == streamIdx {
			return vals[0], nil
		}
	}
}

// Test that metadata records get forwarded by routers. Regardless of the type
// of router, the records are supposed to be forwarded on the first output
// stream that's not closed.
func TestMetadataIsForwarded(t *testing.T) {
	defer leaktest.AfterTest(t)()
	mirrorRouterFactory := func() (RowReceiver, []RowSource, error) {
		bufs := make([]*RowBuffer, 2)
		recvs := make([]RowReceiver, 2)
		srcs := make([]RowSource, 2)
		for i := 0; i < 2; i++ {
			bufs[i] = &RowBuffer{}
			recvs[i] = bufs[i]
			srcs[i] = bufs[i]
		}
		mr, err := makeMirrorRouter(recvs)
		return mr, srcs, err
	}
	hashRouterFactory := func() (RowReceiver, []RowSource, error) {
		bufs := make([]*RowBuffer, 2)
		recvs := make([]RowReceiver, 2)
		srcs := make([]RowSource, 2)
		for i := 0; i < 2; i++ {
			bufs[i] = &RowBuffer{}
			recvs[i] = bufs[i]
			srcs[i] = bufs[i]
		}
		mr, err := makeHashRouter([]uint32{0} /* hashCols */, recvs)
		return mr, srcs, err
	}

	testCases := []struct {
		name          string
		routerFactory func() (RowReceiver, []RowSource, error)
	}{
		{"MirrorRouter", mirrorRouterFactory},
		{"HashRouter", hashRouterFactory},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			router, srcs, err := tc.routerFactory()
			if err != nil {
				t.Fatal(err)
			}

			// Push metadata. It should be fwd to src[0].
			consumerStatus := router.Push(
				nil /* row */, ProducerMetadata{Err: errors.Errorf("test error 1")})
			if consumerStatus != NeedMoreRows {
				t.Fatalf("expected status %d, got: %d", NeedMoreRows, consumerStatus)
			}

			// Call src[0].ConsumerDone() and then push metadata. It should still be
			// fwd to src[0].
			srcs[0].ConsumerDone()
			consumerStatus = router.Push(
				nil /* row */, ProducerMetadata{Err: errors.Errorf("test error 2")})
			if consumerStatus != NeedMoreRows {
				t.Fatalf("expected status %d, got: %d", NeedMoreRows, consumerStatus)
			}

			// Call src[0].ConsumerClosed() and then push metadata. It should be fwd
			// to src[1].
			srcs[0].ConsumerClosed()
			consumerStatus = router.Push(
				nil /* row */, ProducerMetadata{Err: errors.Errorf("test error 3")})
			if consumerStatus != NeedMoreRows {
				t.Fatalf("expected status %d, got: %d", NeedMoreRows, consumerStatus)
			}

			// Call src[1].ConsumerClosed() and then push metadata. It should not be
			// fwd anywhere.
			srcs[1].ConsumerClosed()
			consumerStatus = router.Push(
				nil /* row */, ProducerMetadata{Err: errors.Errorf("test error 4")})
			if consumerStatus != ConsumerClosed {
				t.Fatalf("expected status %d, got: %d", ConsumerClosed, consumerStatus)
			}

			r0 := getRecordsSummary(srcs[0])
			exp0 := "test error 1, test error 2"
			if r0 != exp0 {
				t.Fatalf("expected records: %q, got: %q", exp0, r0)
			}
			r1 := getRecordsSummary(srcs[1])
			exp1 := "test error 3"
			if r1 != exp1 {
				t.Fatalf("expected records: %q, got: %q", exp1, r1)
			}
		})
	}
}

func getRecordsSummary(src RowSource) string {
	var b bytes.Buffer
	first := true
	for {
		row, meta := src.Next()
		if row == nil && meta.Empty() {
			return b.String()
		}
		if !first {
			b.Write([]byte(", "))
		}
		first = false
		if row != nil {
			if len(row) != 1 {
				b.Write([]byte("<row>"))
			} else {
				b.Write([]byte(row[0].String()))
			}
		} else {
			if meta.Err != nil {
				b.Write([]byte(meta.Err.Error()))
			} else {
				b.Write([]byte("<ranges>"))
			}
		}
	}
}
