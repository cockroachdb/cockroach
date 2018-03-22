// Copyright 2018 The Cockroach Authors.
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
	_ "github.com/cockroachdb/cockroach/pkg/ccl/workloadccl/roachmartccl"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/workload"
)

func TestSetup(t *testing.T) {
	defer leaktest.AfterTest(t)()

	get := func(name string) workload.Meta {
		meta, err := workload.Get(name)
		if err != nil {
			t.Fatal(err)
		}
		return meta
	}

	tests := []struct {
		meta        workload.Meta
		flags       []string
		batchSize   int
		concurrency int
	}{
		{
			meta:        get("roachmart"),
			flags:       []string{"--users=10", "--orders=100"},
			batchSize:   100,
			concurrency: 4,
		},
	}

	ctx := context.Background()
	args := base.TestServerArgs{UseDatabase: "test"}
	s, db, _ := serverutils.StartServer(t, args)
	defer s.Stopper().Stop(ctx)

	for _, test := range tests {
		name := fmt.Sprintf("gen=%s/flags=%s/batchSize=%d", test.meta.Name, test.flags, test.batchSize)
		t.Run(name, func(t *testing.T) {
			sqlDB := sqlutils.MakeSQLRunner(db)
			sqlDB.Exec(t, `DROP DATABASE IF EXISTS test`)
			sqlDB.Exec(t, `CREATE DATABASE test`)

			gen := test.meta.New()
			if f, ok := gen.(workload.Flagser); ok {
				if err := f.Flags().Parse(test.flags); err != nil {
					t.Fatalf("%+v", err)
				}
			}

			if _, err := workload.Setup(ctx, sqlDB.DB, gen, test.batchSize, test.concurrency); err != nil {
				t.Fatalf("%+v", err)
			}

			for _, table := range gen.Tables() {
				var c int
				sqlDB.QueryRow(t, fmt.Sprintf(`SELECT COUNT(*) FROM "%s"`, table.Name)).Scan(&c)
				if c != table.InitialRows.NumTotal {
					t.Errorf(`%s: got %d rows expected %d`,
						table.Name, c, table.InitialRows.NumTotal)
				}
			}
		})
	}
}
