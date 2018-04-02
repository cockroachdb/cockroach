// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package allccl

import (
	"context"
	gosql "database/sql"
	"fmt"
	"net/http/httptest"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	_ "github.com/cockroachdb/cockroach/pkg/ccl"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/workload"
)

func hackSetupUsingImport(db *gosql.DB, gen workload.Generator) error {
	ts := httptest.NewServer(workload.CSVMux(workload.Registered()))
	defer ts.Close()

	for _, table := range gen.Tables() {
		csvTableURL := fmt.Sprintf(`%s/csv/%s/%s`, ts.URL, gen.Meta().Name, table.Name)
		importStmt := fmt.Sprintf(`IMPORT TABLE "%s" %s CSV DATA ('%s') WITH nullif='NULL'`,
			table.Name, table.Schema, csvTableURL)
		if _, err := db.Exec(importStmt); err != nil {
			return err
		}
	}
	return nil
}

func TestAllRegisteredWorkloadsValidate(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, meta := range workload.Registered() {
		gen := meta.New()

		var checkConsistencyFn func(context.Context, *gosql.DB) error
		if h, ok := gen.(workload.Hookser); ok {
			checkConsistencyFn = h.Hooks().CheckConsistency
		}
		if checkConsistencyFn == nil {
			// Not all workloads have CheckConsistency defined.
			continue
		}

		t.Run(meta.Name, func(t *testing.T) {
			ctx := context.Background()
			s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
				UseDatabase: "d",
			})
			defer s.Stopper().Stop(ctx)
			sqlutils.MakeSQLRunner(db).Exec(t, `CREATE DATABASE d`)

			if meta.Name == `tpcc` {
				// Special case-tpcc because Setup using the batched inserts
				// takes so long. Unfortunately, we can't do this for all
				// generators because some of them use things that IMPORT
				// doesn't yet handle, like foreign keys.
				if err := hackSetupUsingImport(db, gen); err != nil {
					t.Fatalf(`%+v`, err)
				}
			} else {
				const batchSize, concurrency = 0, 0
				if _, err := workload.Setup(ctx, db, gen, batchSize, concurrency); err != nil {
					t.Fatalf(`%+v`, err)
				}
			}

			if err := checkConsistencyFn(ctx, db); err != nil {
				t.Errorf(`%+v`, err)
			}
		})
	}
}
