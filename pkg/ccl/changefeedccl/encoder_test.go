// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"context"
	gosql "database/sql"
	"encoding/binary"
	gojson "encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/cockroachdb/cockroach-go/crdb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/linkedin/goavro"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

type testSchemaRegistry struct {
	server *httptest.Server
	mu     struct {
		syncutil.Mutex
		idAlloc int32
		schemas map[int32]string
	}
}

func makeTestSchemaRegistry() *testSchemaRegistry {
	r := &testSchemaRegistry{}
	r.mu.schemas = make(map[int32]string)
	r.server = httptest.NewServer(http.HandlerFunc(r.Register))
	return r
}

func (r *testSchemaRegistry) Close() {
	r.server.Close()
}

func (r *testSchemaRegistry) Register(hw http.ResponseWriter, hr *http.Request) {
	type confluentSchemaVersionRequest struct {
		Schema string `json:"schema"`
	}
	type confluentSchemaVersionResponse struct {
		ID int32 `json:"id"`
	}
	if err := func() error {
		defer hr.Body.Close()
		var req confluentSchemaVersionRequest
		if err := gojson.NewDecoder(hr.Body).Decode(&req); err != nil {
			return err
		}

		r.mu.Lock()
		id := r.mu.idAlloc
		r.mu.idAlloc++
		r.mu.schemas[id] = req.Schema
		r.mu.Unlock()

		res, err := gojson.Marshal(confluentSchemaVersionResponse{ID: id})
		if err != nil {
			return err
		}

		hw.Header().Set(`Content-type`, `application/json`)
		_, _ = hw.Write(res)
		return nil
	}(); err != nil {
		http.Error(hw, err.Error(), http.StatusInternalServerError)
	}
}

func (r *testSchemaRegistry) encodedAvroToNative(b []byte) (interface{}, error) {
	if len(b) == 0 || b[0] != confluentAvroWireFormatMagic {
		return ``, errors.Errorf(`bad magic byte`)
	}
	b = b[1:]
	if len(b) < 4 {
		return ``, errors.Errorf(`missing registry id`)
	}
	id := int32(binary.BigEndian.Uint32(b[:4]))
	b = b[4:]

	r.mu.Lock()
	jsonSchema := r.mu.schemas[id]
	r.mu.Unlock()
	codec, err := goavro.NewCodec(jsonSchema)
	if err != nil {
		return ``, err
	}
	native, _, err := codec.NativeFromBinary(b)
	return native, err
}

func TestAvroEncoder(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testFn := func(t *testing.T, db *gosql.DB, f testfeedFactory) {
		ctx := context.Background()
		reg := makeTestSchemaRegistry()
		defer reg.Close()

		sqlDB := sqlutils.MakeSQLRunner(db)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
		var ts1 string
		sqlDB.QueryRow(t,
			`INSERT INTO foo VALUES (1, 'bar'), (2, NULL) RETURNING cluster_logical_timestamp()`,
		).Scan(&ts1)

		foo := f.Feed(t,
			`CREATE CHANGEFEED FOR foo WITH format=$1, confluent_schema_registry=$2, resolved`,
			optFormatAvro, reg.server.URL)
		defer foo.Close(t)
		assertPayloadsAvro(t, reg, foo, []string{
			`foo: {"a":{"long":1}}->{"after":{"foo":{"a":{"long":1},"b":{"string":"bar"}}}}`,
			`foo: {"a":{"long":2}}->{"after":{"foo":{"a":{"long":2},"b":null}}}`,
		})
		resolved := expectResolvedTimestampAvro(t, reg, foo)
		if ts := parseTimeToHLC(t, ts1); !ts.Less(resolved) {
			t.Fatalf(`expected a resolved timestamp greater than %s got %s`, ts, resolved)
		}

		fooUpdated := f.Feed(t,
			`CREATE CHANGEFEED FOR foo WITH format=$1, confluent_schema_registry=$2, updated`,
			optFormatAvro, reg.server.URL)
		defer fooUpdated.Close(t)
		// Skip over the first two rows since we don't know the statement timestamp.
		_, _, _, _, _, ok := fooUpdated.Next(t)
		require.True(t, ok)
		_, _, _, _, _, ok = fooUpdated.Next(t)
		require.True(t, ok)

		var ts2 string
		require.NoError(t, crdb.ExecuteTx(ctx, db, nil /* txopts */, func(tx *gosql.Tx) error {
			return tx.QueryRow(
				`INSERT INTO foo VALUES (3, 'baz') RETURNING cluster_logical_timestamp()`,
			).Scan(&ts2)
		}))
		assertPayloadsAvro(t, reg, fooUpdated, []string{
			`foo: {"a":{"long":3}}->{"after":{"foo":{"a":{"long":3},"b":{"string":"baz"}}},` +
				`"updated":{"string":"` + ts2 + `"}}`,
		})
	}

	t.Run(`sinkless`, sinklessTest(testFn))
	t.Run(`enterprise`, enterpriseTest(testFn))
	t.Run(`rangefeed`, rangefeedTest(sinklessTest, testFn))
}

func TestAvroSchemaChange(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testFn := func(t *testing.T, db *gosql.DB, f testfeedFactory) {
		reg := makeTestSchemaRegistry()
		defer reg.Close()

		sqlDB := sqlutils.MakeSQLRunner(db)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY)`)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (1)`)

		foo := f.Feed(t, `CREATE CHANGEFEED FOR foo WITH format=$1, confluent_schema_registry=$2`,
			optFormatAvro, reg.server.URL)
		defer foo.Close(t)
		assertPayloadsAvro(t, reg, foo, []string{
			`foo: {"a":{"long":1}}->{"after":{"foo":{"a":{"long":1}}}}`,
		})

		sqlDB.Exec(t, `ALTER TABLE foo ADD COLUMN b UUID`)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (2, gen_random_uuid())`)
		if _, _, _, _, _, ok := foo.Next(t); ok {
			t.Fatal(`unexpected row`)
		}
		if err := foo.Err(); !testutils.IsError(err, `type UUID not yet supported with avro`) {
			t.Fatalf(`expected "type UUID not yet supported with avro" error got: %+v`, err)
		}
	}

	t.Run(`sinkless`, sinklessTest(testFn))
	t.Run(`enterprise`, enterpriseTest(testFn))
	t.Run(`rangefeed`, rangefeedTest(sinklessTest, testFn))
}
