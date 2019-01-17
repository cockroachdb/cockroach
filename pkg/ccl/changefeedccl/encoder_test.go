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
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/ledger"
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

func TestAvroLedger(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testFn := func(t *testing.T, db *gosql.DB, f testfeedFactory) {
		reg := makeTestSchemaRegistry()
		defer reg.Close()

		ctx := context.Background()
		gen := ledger.FromFlags(`--customers=1`)
		_, err := workload.Setup(ctx, db, gen, 0, 0)
		require.NoError(t, err)

		ledger := f.Feed(t, `CREATE CHANGEFEED FOR
	                       customer, transaction, entry, session
	                       WITH format=$1, confluent_schema_registry=$2
	               `, optFormatAvro, reg.server.URL)
		defer ledger.Close(t)

		assertPayloadsAvro(t, reg, ledger, []string{
			`customer: {"id":{"long":0}}->{"after":{"customer":{"balance":{"bytes.decimal":"0"},"created":{"long.timestamp-micros":"2114-03-27T13:14:27.287114Z"},"credit_limit":null,"currency_code":{"string":"XVL"},"id":{"long":0},"identifier":{"string":"0"},"is_active":{"boolean":true},"is_system_customer":{"boolean":true},"name":null,"sequence_number":{"long":-1}}}}`,
			`entry: {"id":{"long":1543039099823358511}}->{"after":{"entry":{"amount":{"bytes.decimal":"0"},"created_ts":{"long.timestamp-micros":"1990-12-09T23:47:23.811124Z"},"customer_id":{"long":0},"id":{"long":1543039099823358511},"money_type":{"string":"C"},"system_amount":{"bytes.decimal":"44061/500"},"transaction_id":{"string":"payment:a8c7f832-281a-39c5-8820-1fb960ff6465"}}}}`,
			`entry: {"id":{"long":2244708090865615074}}->{"after":{"entry":{"amount":{"bytes.decimal":"1/50"},"created_ts":{"long.timestamp-micros":"2075-11-08T22:07:12.055686Z"},"customer_id":{"long":0},"id":{"long":2244708090865615074},"money_type":{"string":"C"},"system_amount":{"bytes.decimal":"44061/500"},"transaction_id":{"string":"payment:a8c7f832-281a-39c5-8820-1fb960ff6465"}}}}`,
			`entry: {"id":{"long":3305628230121721621}}->{"after":{"entry":{"amount":{"bytes.decimal":"1/25"},"created_ts":{"long.timestamp-micros":"2185-01-30T21:38:15.06669Z"},"customer_id":{"long":0},"id":{"long":3305628230121721621},"money_type":{"string":"C"},"system_amount":{"bytes.decimal":"44061/500"},"transaction_id":{"string":"payment:e3757ca7-d646-66ea-2b8d-6116831cbb05"}}}}`,
			`entry: {"id":{"long":4151935814835861840}}->{"after":{"entry":{"amount":{"bytes.decimal":"3/50"},"created_ts":{"long.timestamp-micros":"1684-10-05T17:51:40.795101Z"},"customer_id":{"long":0},"id":{"long":4151935814835861840},"money_type":{"string":"C"},"system_amount":{"bytes.decimal":"44061/500"},"transaction_id":{"string":"payment:e3757ca7-d646-66ea-2b8d-6116831cbb05"}}}}`,
			`entry: {"id":{"long":5577006791947779410}}->{"after":{"entry":{"amount":{"bytes.decimal":"0"},"created_ts":{"long.timestamp-micros":"2185-11-07T09:42:42.666146Z"},"customer_id":{"long":0},"id":{"long":5577006791947779410},"money_type":{"string":"C"},"system_amount":{"bytes.decimal":"-88123/1000"},"transaction_id":{"string":"payment:a8c7f832-281a-39c5-8820-1fb960ff6465"}}}}`,
			`entry: {"id":{"long":6640668014774057861}}->{"after":{"entry":{"amount":{"bytes.decimal":"-1/50"},"created_ts":{"long.timestamp-micros":"1690-05-19T13:29:46.145044Z"},"customer_id":{"long":0},"id":{"long":6640668014774057861},"money_type":{"string":"C"},"system_amount":{"bytes.decimal":"-88123/1000"},"transaction_id":{"string":"payment:a8c7f832-281a-39c5-8820-1fb960ff6465"}}}}`,
			`entry: {"id":{"long":7414159922357799360}}->{"after":{"entry":{"amount":{"bytes.decimal":"-1/25"},"created_ts":{"long.timestamp-micros":"1706-02-05T02:38:08.15195Z"},"customer_id":{"long":0},"id":{"long":7414159922357799360},"money_type":{"string":"C"},"system_amount":{"bytes.decimal":"-88123/1000"},"transaction_id":{"string":"payment:e3757ca7-d646-66ea-2b8d-6116831cbb05"}}}}`,
			`entry: {"id":{"long":8475284246537043955}}->{"after":{"entry":{"amount":{"bytes.decimal":"-3/50"},"created_ts":{"long.timestamp-micros":"2048-07-21T10:02:40.114474Z"},"customer_id":{"long":0},"id":{"long":8475284246537043955},"money_type":{"string":"C"},"system_amount":{"bytes.decimal":"-88123/1000"},"transaction_id":{"string":"payment:e3757ca7-d646-66ea-2b8d-6116831cbb05"}}}}`,
			`session: {"session_id":{"string":"pLnfgDsc3WD9F3qNfHK6a95jjJkwzDkh0h3fhfUVuS0jZ9uVbhV4vC6AWX40IV"}}->{"after":{"session":{"data":{"string":"SP3NcHciWvqZTa3N06RxRTZHWUsaD7HEdz1ThbXfQ7pYSQ4n378l2VQKGNbSuJE0fQbzONJAAwdCxmM9BIabKERsUhPNmMmdf3eSJyYtqwcFiUILzXv3fcNIrWO8sToFgoilA1U2WxNeW2gdgUVDsEWJ88aX8tLF"},"expiry_timestamp":{"long.timestamp-micros":"2052-05-14T04:02:49.264975Z"},"last_update":{"long.timestamp-micros":"2070-03-19T02:10:22.552438Z"},"session_id":{"string":"pLnfgDsc3WD9F3qNfHK6a95jjJkwzDkh0h3fhfUVuS0jZ9uVbhV4vC6AWX40IV"}}}}`,
			`transaction: {"external_id":{"string":"payment:a8c7f832-281a-39c5-8820-1fb960ff6465"}}->{"after":{"transaction":{"context":{"string":"BpLnfgDsc3WD9F3qNfHK6a95jjJkwzDkh0h3fhfUVuS0jZ9uVbhV4vC6"},"created_ts":{"long.timestamp-micros":"2178-08-01T19:10:30.064819Z"},"external_id":{"string":"payment:a8c7f832-281a-39c5-8820-1fb960ff6465"},"response":{"bytes":"MDZSeFJUWkhXVXNhRDdIRWR6MVRoYlhmUTdwWVNRNG4zNzhsMlZRS0dOYlN1SkUwZlFiek9OSkFBd2RDeG1NOUJJYWJLRVJzVWhQTm1NbWRmM2VTSnlZdHF3Y0ZpVUlMelh2M2ZjTklyV084c1RvRmdvaWxBMVUyV3hOZVcyZ2RnVVZEc0VXSjg4YVg4dExGSjk1cVlVN1VyTjljdGVjd1p0NlM1empoRDF0WFJUbWtZS1FvTjAyRm1XblFTSzN3UkM2VUhLM0txQXR4alAzWm1EMmp0dDR6Z3I2TWVVam9BamNPMGF6TW10VTRZdHYxUDhPUG1tU05hOThkN3RzdGF4eTZuYWNuSkJTdUZwT2h5SVhFN1BKMURoVWtMWHFZWW5FTnVucWRzd3BUdzVVREdEUzM0bVNQWUs4dm11YjNYOXVYSXU3Rk5jSmpBUlFUM1JWaFZydDI0UDdpNnhDckw2RmM0R2N1SEMxNGthdW5BVFVQUkhqR211Vm14SHN5enpCYnlPb25xVlVTREsxVg=="},"reversed_by":null,"systimestamp":{"long.timestamp-micros":"2215-07-28T23:47:01.795499Z"},"tcomment":null,"transaction_type_reference":{"long":400},"username":{"string":"WX40IVUWSP3NcHciWvqZ"}}}}`,
			`transaction: {"external_id":{"string":"payment:e3757ca7-d646-66ea-2b8d-6116831cbb05"}}->{"after":{"transaction":{"context":{"string":"KSiOW5eQ8sklpgstrQZtAcrsGvPnYSXMOpFIpPzS8iI5N2gN7lD1rYjT"},"created_ts":{"long.timestamp-micros":"2062-07-27T13:21:35.213969Z"},"external_id":{"string":"payment:e3757ca7-d646-66ea-2b8d-6116831cbb05"},"response":{"bytes":"bWdkbHVWOFVvcWpRM1JBTTRTWjNzT0M4ZnlzZXN5NnRYeVZ5WTBnWkE1aVNJUjM4MFVPVWFwQTlPRmpuRWtiaHF6MlRZSlZIWUFtTHI5R0kyMlo3NFVmNjhDMFRRb2RDdWF0NmhmWmZSYmFlV1pJSFExMGJsSjVqQUd2VVRpWWJOWHZPcWowYlRUM24xNmNqQVNEN29qN2RPbVlVbTFua3AybnVvWTZGZlgzcVFHY09SbHZ2UHdHaHNDZWlZTmpvTVRoUXBFc0ZrSVpZVUxxNFFORzc1M25mamJYdENaUm4xSmVZV1hpUW1IWjJZMWIxb1lZbUtBS05aQjF1MGt1TU5ZbEFISW5hY1JoTkFzakd6bnBKSXZZdmZqWXk3MXV4OVI5SkRNQUMxRUtOSGFZVWNlekk4OHRHYmdwbWFGaXdIV09sUFQ5RUJVcHh6MHlCSnZGM1BKcW5jejVwMnpnVVhDcm9kZTV6UG5pNjJQV1dtMk5pSWVkSUxFaExLVVNHVWRNU1R5N1pmcjRyY2RJTw=="},"reversed_by":null,"systimestamp":{"long.timestamp-micros":"2229-01-11T00:56:37.706179Z"},"tcomment":null,"transaction_type_reference":{"long":400},"username":{"string":"XJXORIpfMGxOaIIFFFts"}}}}`,
		})
	}

	t.Run(`sinkless`, sinklessTest(testFn))
	t.Run(`enterprise`, enterpriseTest(testFn))
	t.Run(`rangefeed`, rangefeedTest(sinklessTest, testFn))
}
