// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	gosql "database/sql"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

// TODO(dan): This is just a sanity check for the avro stuff. We need an
// end-to-end test with the avro format that interprets the bytes using a schema
// it gets from the schema registry.
func TestEncoderAvro(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testFn := func(t *testing.T, db *gosql.DB, f testfeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(db)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (1, 'bar')`)

		foo := f.Feed(t, `CREATE CHANGEFEED FOR foo WITH format=$1`, optFormatAvroJSON)
		defer foo.Close(t)

		// There's randomized map iteration deep in the avro lib, so we can't use
		// the normal assertPayloads helper.
		table, _, key, value, _, ok := foo.Next(t)
		if !ok {
			t.Fatal(`expected row`)
		}
		require.Equal(t, `foo`, table)
		require.Equal(t, `{"a":1}`, string(key))
		require.Contains(t, string(value), `"a":1`)
		require.Contains(t, string(value), `"b":{"string":"bar"}`)
	}

	t.Run(`sinkless`, sinklessTest(testFn))
	t.Run(`enterprise`, enterpriseTest(testFn))
}
