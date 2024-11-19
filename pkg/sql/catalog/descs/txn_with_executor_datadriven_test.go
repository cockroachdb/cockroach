// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package descs_test

import (
	"bytes"
	"context"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"text/tabwriter"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

// TestTxnWithExecutorDataDriven is a data-driven test format for testing
// the behavior of transaction in a descs.CollectionFactory.TxnWithExecutor.
//
// The commands are exec and query which take statements as input.
// The args are:
//
//	db: db name
//	search_path: csv of strings, optional
//	error: expected error, optional
func TestTxnWithExecutorDataDriven(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	datadriven.Walk(t, datapathutils.TestDataPath(t, ""), func(t *testing.T, path string) {
		s := serverutils.StartServerOnly(t, base.TestServerArgs{})
		defer s.Stopper().Stop(ctx)
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			stmts, err := parser.Parse(d.Input)
			require.NoError(t, err)
			var out strings.Builder
			var expErr string
			if d.HasArg("error") {
				d.ScanArgs(t, "error", &expErr)
			}
			var sd sessiondata.InternalExecutorOverride
			sd.User = username.NodeUserName()
			d.ScanArgs(t, "db", &sd.Database)
			searchPath := sessiondata.DefaultSearchPath
			if d.HasArg("search_path") {
				var sp string
				d.ScanArgs(t, "search_path", &sp)
				searchPath = sessiondata.MakeSearchPath(strings.Split(sp, ","))
			}
			sd.SearchPath = &searchPath
			ief := s.InternalDB().(descs.DB)
			err = ief.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
				for _, stmt := range stmts {
					switch d.Cmd {
					case "exec":
						n, err := txn.ExecEx(ctx, "test", txn.KV(), sd, stmt.SQL)
						if err != nil {
							return err
						}
						fmt.Fprintf(&out, "%d\t", n)
					case "query":
						rows, cols, err := txn.QueryBufferedExWithCols(
							ctx, "test", txn.KV(), sd, stmt.SQL,
						)
						if err != nil {
							return err
						}
						var buf bytes.Buffer
						w := tabwriter.NewWriter(&buf, 2, 3, 2, ' ', 0)
						var strs []string
						for _, col := range cols {
							strs = append(strs, col.Name)
						}
						_, _ = w.Write([]byte(strings.Join(strs, "\t") + "\n"))
						for _, row := range rows {
							strs = strs[:0]
							for _, c := range row {
								strs = append(strs, c.String())
							}
							_, _ = w.Write([]byte(strings.Join(strs, "\t") + "\n"))
						}
						if err := w.Flush(); err != nil {
							d.Fatalf(t, "flush: %v", err)
						}
						_, _ = out.Write(buf.Bytes())
					default:
						d.Fatalf(t, "unexpected command %q", d.Cmd)
					}
				}
				return nil
			})
			if err == nil {
				return out.String()
			}
			if expErr != "" && err != nil {
				re, reErr := regexp.Compile(expErr)
				if reErr != nil {
					d.Fatalf(t, "failed to compile regexp: %v", reErr)
				}
				if !re.MatchString(err.Error()) {
					d.Fatalf(t, "expected error %v, got %v", re, reErr)
				}
			}
			d.Fatalf(t, "unexpected error: %v", err)
			return "" // unreachable
		})
	})
}
