// Copyright 2018 The Cockroach Authors.
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

package workload_test

import (
	"bytes"
	"context"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/bank"
	"github.com/cockroachdb/cockroach/pkg/workload/tpcc"
)

func TestHandleCSV(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		params, expected string
	}{
		{
			`?rows=1`, `
0,0,initial-58566c427a67626169434d52416a577768544863746375417868784b5146446146704c536a466263586f454666`,
		},
		{
			`?rows=5&row-start=1&row-end=3`, `
1,0,initial-494d6d4f61674d5378656f4e6a714b5270576a624155624d494e524a4373567743466c65446a6a717257536766
2,0,initial-6d54756f50526576475a4d6b485a6376496f415972646e4d587a464153574168555379576248515062696e754c`,
		},
	}

	meta := bank.FromRows(0).Meta()
	for _, test := range tests {
		t.Run(test.params, func(t *testing.T) {
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if err := workload.HandleCSV(w, r, `/bank/`, meta); err != nil {
					panic(err)
				}
			}))
			defer ts.Close()

			res, err := http.Get(ts.URL + `/bank/bank` + test.params)
			if err != nil {
				t.Fatal(err)
			}
			data, err := ioutil.ReadAll(res.Body)
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

func BenchmarkWriteCSVRows(b *testing.B) {
	ctx := context.Background()

	var rows [][][]interface{}
	for _, table := range tpcc.FromWarehouses(1).Tables() {
		rows = append(rows, table.InitialRows.Batch(0))
	}
	table := workload.Table{
		InitialRows: workload.BatchedTuples{
			Batch: func(rowIdx int) [][]interface{} { return rows[rowIdx] },
		},
	}

	var buf bytes.Buffer
	fn := func() {
		const limit = -1
		if _, err := workload.WriteCSVRows(ctx, &buf, table, 0, len(rows), limit); err != nil {
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
