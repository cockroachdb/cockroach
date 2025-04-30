// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package workload_test

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/util/bufalloc"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/bank"
	"github.com/cockroachdb/cockroach/pkg/workload/tpcc"
	"github.com/stretchr/testify/require"
)

func TestHandleCSV(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		params, expected string
	}{
		{
			`?rows=1`, `
0,0,ZyRNnbshVrywysfEgoPqtlMVOZpmWXVoNOWrsmVEOfbdeUZwKgCQlBLypYvStwKSTsgmZiLZNJXwZSYGpKaaiqrxPqaaQejXPcNO`,
		},
		{
			`?rows=5&row-start=1&row-end=3&batch-size=1`, `
1,0,eHeVKnSfvnxRagcywSQgenyjqRrqFrklgyrbdZJKywAmLCJPpJylOvKcffUGzqjjkpwJhFddQOSIZXhzpgrTkvAnJKqqcWekyAsm
2,0,KRVBIpGWsjUSuxTvbnJAIAKvQMfnyKnfWQqNTUwTDaIXiUJDMGqdFUDadEzkOdmwnfDNBLFNHUoPlYRcEGjqQZrfnNYAnqxDlXqC`,
		},
	}

	// assertions depend on this seed
	bank.RandomSeed.Set(1)
	meta := bank.FromRows(0).Meta()
	for _, test := range tests {
		t.Run(test.params, func(t *testing.T) {
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if err := workload.HandleCSV(w, r, `/bank/`, meta); err != nil {
					panic(err)
				}
			}))
			defer ts.Close()

			res, err := httputil.Get(context.Background(), ts.URL+`/bank/bank`+test.params)
			if err != nil {
				t.Fatal(err)
			}
			data, err := io.ReadAll(res.Body)
			res.Body.Close()
			if err != nil {
				t.Fatal(err)
			}

			if d, e := strings.TrimSpace(string(data)), strings.TrimSpace(test.expected); d != e {
				t.Errorf("got [\n%s\n] expected [\n%s\n]", d, e)
			}
		})
	}
}

func TestHandleCSVTpcc(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		params, expected string
	}{
		{
			`?rows=1`, `
0,9,10,17,16,RG,955311111,0.1622,300000.00`,
		},
		{
			`?rows=5&row-start=1&row-end=3`, `
1,10,20,13,20,RG,327711111,0.1625,300000.00
2,9,18,12,10,QN,533211111,0.1000,300000.00`,
		},
	}

	// assertions depend on this seed
	tpcc.RandomSeed.Set(1)
	meta := tpcc.FromWarehouses(1).Meta()
	for _, test := range tests {
		t.Run(test.params, func(t *testing.T) {
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				require.NoError(t, workload.HandleCSV(w, r, `/tpcc/`, meta))
			}))
			defer ts.Close()

			res, err := httputil.Get(context.Background(), ts.URL+`/tpcc/warehouse`+test.params)
			require.NoError(t, err)

			data, err := io.ReadAll(res.Body)
			res.Body.Close()
			require.NoError(t, err)

			require.Equal(t, strings.TrimSpace(test.expected), strings.TrimSpace(string(data)))
		})
	}
}

func BenchmarkWriteCSVRows(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var batches []coldata.Batch
	for _, table := range tpcc.FromWarehouses(1).Tables() {
		cb := coldata.NewMemBatch(nil /* types */, coldata.StandardColumnFactory)
		var a bufalloc.ByteAllocator
		table.InitialRows.FillBatch(0, cb, &a)
		batches = append(batches, cb)
	}
	table := workload.Table{
		InitialRows: workload.BatchedTuples{
			FillBatch: func(batchIdx int, cb coldata.Batch, _ *bufalloc.ByteAllocator) {
				*cb.(*coldata.MemBatch) = *batches[batchIdx].(*coldata.MemBatch)
			},
		},
	}

	var buf bytes.Buffer
	fn := func() {
		const limit = -1
		if _, err := workload.WriteCSVRows(ctx, &buf, table, 0, len(batches), limit); err != nil {
			b.Fatalf(`%+v`, err)
		}
	}

	// Run fn once to pre-size buf.
	fn()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		fn()
	}
	b.StopTimer()
	b.SetBytes(int64(buf.Len()))
}
