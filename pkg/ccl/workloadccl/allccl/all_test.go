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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	_ "github.com/cockroachdb/cockroach/pkg/ccl"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/workload"
)

func TestAllRegisteredWorkloadsValidate(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, meta := range workload.Registered() {
		t.Run(meta.Name, func(t *testing.T) {
			ctx := context.Background()
			s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
				UseDatabase: "d",
			})
			defer s.Stopper().Stop(ctx)
			sqlutils.MakeSQLRunner(db).Exec(t, `CREATE DATABASE d`)

			gen := meta.New()
			const batchSize, concurrency = 0, 0
			if _, err := workload.Setup(ctx, db, gen, batchSize, concurrency); err != nil {
				t.Fatalf(`%+v`, err)
			}
			if err := workload.ValidateInitialData(ctx, db, gen.Tables()); err != nil {
				t.Errorf(`%+v`, err)
			}
		})
	}
}
