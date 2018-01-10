// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package acceptanceccl

import (
	"context"
	gosql "database/sql"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func BenchmarkPartitioning(b *testing.B) {
	if b.N != 1 {
		b.Fatal("b.N must be 1")
	}

	bt := benchmarkTest{
		b:      b,
		nodes:  3,
		prefix: "partitioning",
	}

	ctx := context.Background()
	bt.Start(ctx)
	defer bt.Close(ctx)

	db, err := gosql.Open("postgres", bt.f.PGUrl(ctx, 0))
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	log.Infof(ctx, "starting backup")
	row := db.QueryRow(`BACKUP DATABASE datablocks TO $1`, getAzureEphemeralURI(b))
	var unused string
	var dataSize int64
	if err := row.Scan(&unused, &unused, &unused, &unused, &unused, &unused, &dataSize); err != nil {
		bt.b.Fatal(err)
	}
	b.SetBytes(dataSize)
	log.Infof(ctx, "backed up %s", humanizeutil.IBytes(dataSize))
}
