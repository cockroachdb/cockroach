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

package tree_test

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"golang.org/x/sync/errgroup"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	_ "github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

var (
	flagWritePretty = flag.Bool("rewrite-pretty", false, "rewrite pretty test outputs")
)

func TestPrettyData(t *testing.T) {
	matches, err := filepath.Glob(filepath.Join("testdata", "pretty", "*.sql"))
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range matches {
		m := m
		t.Run(m, func(t *testing.T) {
			sql, err := ioutil.ReadFile(m)
			if err != nil {
				t.Fatal(err)
			}
			stmt, err := parser.ParseOne(string(sql))
			if err != nil {
				t.Fatal(err)
			}

			res := make([]string, len(sql)+10)
			work := make(chan int, len(res))
			for i := range res {
				work <- i + 1
			}
			close(work)
			g, _ := errgroup.WithContext(context.Background())
			worker := func() error {
				for i := range work {
					res[i-1] = tree.PrettyWithOpts(stmt, i, true, 4)
				}
				return nil
			}
			for i := 0; i < runtime.NumCPU(); i++ {
				g.Go(worker)
			}
			if err := g.Wait(); err != nil {
				t.Fatal(err)
			}
			var sb strings.Builder
			for i, s := range res {
				if i == 0 || s != res[i-1] {
					fmt.Fprintf(&sb, "%d:\n%s\n%s\n\n", i+1, strings.Repeat("-", i+1), s)
				}
			}
			got := strings.TrimSpace(sb.String()) + "\n"

			ext := filepath.Ext(m)
			outfile := m[:len(m)-len(ext)] + ".golden"

			if *flagWritePretty {
				if err := ioutil.WriteFile(outfile, []byte(got), 0666); err != nil {
					t.Fatal(err)
				}
				return
			}

			expect, err := ioutil.ReadFile(outfile)
			if err != nil {
				t.Fatal(err)
			}
			if string(expect) != got {
				t.Fatalf("unexpected:\n%s", got)
			}

			parser.VerifyStatementPrettyRoundtrip(t, string(sql))
		})
	}
}

func TestPrettyVerify(t *testing.T) {
	tests := map[string]string{
		`SELECT interval '-2µs'`: `SELECT '-2µs':::INTERVAL`,
	}
	for orig, pretty := range tests {
		t.Run(orig, func(t *testing.T) {
			parser.VerifyStatementPrettyRoundtrip(t, orig)

			stmt, err := parser.ParseOne(orig)
			if err != nil {
				t.Fatal(err)
			}
			got := tree.Pretty(stmt)
			if pretty != got {
				t.Fatalf("got: %s\nexpected: %s", got, pretty)
			}
		})
	}
}
