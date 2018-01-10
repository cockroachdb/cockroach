// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package workloadccl_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/testutilsccl/workloadccl/roachmartccl"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/workload"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestSetup(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		meta      workload.Meta
		flags     []string
		batchSize int
	}{
		{
			meta:      workload.MustGet(`roachmart`),
			flags:     []string{"--users=10", "--orders=100"},
			batchSize: 100,
		},
	}

	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	for _, test := range tests {
		name := fmt.Sprintf("gen=%s/flags=%s/batchSize=%d", test.meta.Name, test.flags, test.batchSize)
		t.Run(name, func(t *testing.T) {
			sqlDB := sqlutils.MakeSQLRunner(db)
			sqlDB.Exec(t, `DROP DATABASE IF EXISTS test`)
			sqlDB.Exec(t, `CREATE DATABASE test`)
			sqlDB.Exec(t, `USE test`)

			gen := test.meta.New()
			if err := gen.Configure(test.flags); err != nil {
				t.Fatalf("%+v", err)
			}

			tables := gen.Tables()
			if _, err := workload.Setup(sqlDB.DB, tables, test.batchSize); err != nil {
				t.Fatalf("%+v", err)
			}

			for _, table := range tables {
				var c int
				sqlDB.QueryRow(t, fmt.Sprintf(`SELECT COUNT(*) FROM %s`, table.Name)).Scan(&c)
				if c != table.InitialRowCount {
					t.Errorf(`%s: got %d rows expected %d`, table.Name, c, table.InitialRowCount)
				}
			}
		})
	}
}
