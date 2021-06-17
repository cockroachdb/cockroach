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
	"fmt"
	"net/url"
	"testing"

	"github.com/cockroachdb/cockroach-go/crdb"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdctest"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/workload/ledger"
	"github.com/cockroachdb/cockroach/pkg/workload/workloadsql"
	"github.com/stretchr/testify/require"
)

func TestEncoders(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tableDesc, err := parseTableDesc(`CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
	require.NoError(t, err)
	row := rowenc.EncDatumRow{
		rowenc.EncDatum{Datum: tree.NewDInt(1)},
		rowenc.EncDatum{Datum: tree.NewDString(`bar`)},
	}
	ts := hlc.Timestamp{WallTime: 1, Logical: 2}

	var opts []map[string]string
	for _, f := range []string{string(changefeedbase.OptFormatJSON), string(changefeedbase.OptFormatAvro)} {
		for _, e := range []string{
			string(changefeedbase.OptEnvelopeKeyOnly), string(changefeedbase.OptEnvelopeRow), string(changefeedbase.OptEnvelopeWrapped),
		} {
			opts = append(opts,
				map[string]string{changefeedbase.OptFormat: f, changefeedbase.OptEnvelope: e},
				map[string]string{changefeedbase.OptFormat: f, changefeedbase.OptEnvelope: e, changefeedbase.OptDiff: ``},
				map[string]string{changefeedbase.OptFormat: f, changefeedbase.OptEnvelope: e, changefeedbase.OptUpdatedTimestamps: ``},
				map[string]string{changefeedbase.OptFormat: f, changefeedbase.OptEnvelope: e, changefeedbase.OptUpdatedTimestamps: ``, changefeedbase.OptDiff: ``},
			)
		}
	}

	expecteds := map[string]struct {
		// Either err is set or all of insert, delete, and resolved are.
		err      string
		insert   string
		delete   string
		resolved string
	}{
		`format=json,envelope=key_only`: {
			insert:   `[1]->`,
			delete:   `[1]->`,
			resolved: `{"__crdb__":{"resolved":"1.0000000002"}}`,
		},
		`format=json,envelope=key_only,updated`: {
			insert:   `[1]->`,
			delete:   `[1]->`,
			resolved: `{"__crdb__":{"resolved":"1.0000000002"}}`,
		},
		`format=json,envelope=key_only,diff`: {
			err: `diff is only usable with envelope=wrapped`,
		},
		`format=json,envelope=key_only,updated,diff`: {
			err: `diff is only usable with envelope=wrapped`,
		},
		`format=json,envelope=row`: {
			insert:   `[1]->{"a": 1, "b": "bar"}`,
			delete:   `[1]->`,
			resolved: `{"__crdb__":{"resolved":"1.0000000002"}}`,
		},
		`format=json,envelope=row,updated`: {
			insert:   `[1]->{"__crdb__": {"updated": "1.0000000002"}, "a": 1, "b": "bar"}`,
			delete:   `[1]->`,
			resolved: `{"__crdb__":{"resolved":"1.0000000002"}}`,
		},
		`format=json,envelope=row,diff`: {
			err: `diff is only usable with envelope=wrapped`,
		},
		`format=json,envelope=row,updated,diff`: {
			err: `diff is only usable with envelope=wrapped`,
		},
		`format=json,envelope=wrapped`: {
			insert:   `[1]->{"after": {"a": 1, "b": "bar"}}`,
			delete:   `[1]->{"after": null}`,
			resolved: `{"resolved":"1.0000000002"}`,
		},
		`format=json,envelope=wrapped,updated`: {
			insert:   `[1]->{"after": {"a": 1, "b": "bar"}, "updated": "1.0000000002"}`,
			delete:   `[1]->{"after": null, "updated": "1.0000000002"}`,
			resolved: `{"resolved":"1.0000000002"}`,
		},
		`format=json,envelope=wrapped,diff`: {
			insert:   `[1]->{"after": {"a": 1, "b": "bar"}, "before": null}`,
			delete:   `[1]->{"after": null, "before": {"a": 1, "b": "bar"}}`,
			resolved: `{"resolved":"1.0000000002"}`,
		},
		`format=json,envelope=wrapped,updated,diff`: {
			insert:   `[1]->{"after": {"a": 1, "b": "bar"}, "before": null, "updated": "1.0000000002"}`,
			delete:   `[1]->{"after": null, "before": {"a": 1, "b": "bar"}, "updated": "1.0000000002"}`,
			resolved: `{"resolved":"1.0000000002"}`,
		},
		`format=experimental_avro,envelope=key_only`: {
			insert:   `{"a":{"long":1}}->`,
			delete:   `{"a":{"long":1}}->`,
			resolved: `{"resolved":{"string":"1.0000000002"}}`,
		},
		`format=experimental_avro,envelope=key_only,updated`: {
			err: `updated is only usable with envelope=wrapped`,
		},
		`format=experimental_avro,envelope=key_only,diff`: {
			err: `diff is only usable with envelope=wrapped`,
		},
		`format=experimental_avro,envelope=key_only,updated,diff`: {
			err: `updated is only usable with envelope=wrapped`,
		},
		`format=experimental_avro,envelope=row`: {
			err: `envelope=row is not supported with format=experimental_avro`,
		},
		`format=experimental_avro,envelope=row,updated`: {
			err: `envelope=row is not supported with format=experimental_avro`,
		},
		`format=experimental_avro,envelope=row,diff`: {
			err: `envelope=row is not supported with format=experimental_avro`,
		},
		`format=experimental_avro,envelope=row,updated,diff`: {
			err: `envelope=row is not supported with format=experimental_avro`,
		},
		`format=experimental_avro,envelope=wrapped`: {
			insert: `{"a":{"long":1}}->` +
				`{"after":{"foo":{"a":{"long":1},"b":{"string":"bar"}}}}`,
			delete:   `{"a":{"long":1}}->{"after":null}`,
			resolved: `{"resolved":{"string":"1.0000000002"}}`,
		},
		`format=experimental_avro,envelope=wrapped,updated`: {
			insert: `{"a":{"long":1}}->` +
				`{"after":{"foo":{"a":{"long":1},"b":{"string":"bar"}}},` +
				`"updated":{"string":"1.0000000002"}}`,
			delete:   `{"a":{"long":1}}->{"after":null,"updated":{"string":"1.0000000002"}}`,
			resolved: `{"resolved":{"string":"1.0000000002"}}`,
		},
		`format=experimental_avro,envelope=wrapped,diff`: {
			insert: `{"a":{"long":1}}->` +
				`{"after":{"foo":{"a":{"long":1},"b":{"string":"bar"}}},` +
				`"before":null}`,
			delete: `{"a":{"long":1}}->` +
				`{"after":null,` +
				`"before":{"foo_before":{"a":{"long":1},"b":{"string":"bar"}}}}`,
			resolved: `{"resolved":{"string":"1.0000000002"}}`,
		},
		`format=experimental_avro,envelope=wrapped,updated,diff`: {
			insert: `{"a":{"long":1}}->` +
				`{"after":{"foo":{"a":{"long":1},"b":{"string":"bar"}}},` +
				`"before":null,` +
				`"updated":{"string":"1.0000000002"}}`,
			delete: `{"a":{"long":1}}->` +
				`{"after":null,` +
				`"before":{"foo_before":{"a":{"long":1},"b":{"string":"bar"}}},` +
				`"updated":{"string":"1.0000000002"}}`,
			resolved: `{"resolved":{"string":"1.0000000002"}}`,
		},
	}

	for _, o := range opts {
		name := fmt.Sprintf("format=%s,envelope=%s", o[changefeedbase.OptFormat], o[changefeedbase.OptEnvelope])
		if _, ok := o[changefeedbase.OptUpdatedTimestamps]; ok {
			name += `,updated`
		}
		if _, ok := o[changefeedbase.OptDiff]; ok {
			name += `,diff`
		}
		t.Run(name, func(t *testing.T) {
			expected := expecteds[name]

			var rowStringFn func([]byte, []byte) string
			var resolvedStringFn func([]byte) string
			switch o[changefeedbase.OptFormat] {
			case string(changefeedbase.OptFormatJSON):
				rowStringFn = func(k, v []byte) string { return fmt.Sprintf(`%s->%s`, k, v) }
				resolvedStringFn = func(r []byte) string { return string(r) }
			case string(changefeedbase.OptFormatAvro):
				reg := cdctest.StartTestSchemaRegistry()
				defer reg.Close()
				o[changefeedbase.OptConfluentSchemaRegistry] = reg.URL()
				rowStringFn = func(k, v []byte) string {
					key, value := avroToJSON(t, reg, k), avroToJSON(t, reg, v)
					return fmt.Sprintf(`%s->%s`, key, value)
				}
				resolvedStringFn = func(r []byte) string {
					return string(avroToJSON(t, reg, r))
				}
			default:
				t.Fatalf(`unknown format: %s`, o[changefeedbase.OptFormat])
			}

			target := jobspb.ChangefeedTarget{
				StatementTimeName: tableDesc.GetName(),
			}
			targets := jobspb.ChangefeedTargets{}
			targets[tableDesc.GetID()] = target

			e, err := getEncoder(context.Background(), o, targets)
			if len(expected.err) > 0 {
				require.EqualError(t, err, expected.err)
				return
			}
			require.NoError(t, err)

			rowInsert := encodeRow{
				datums:        row,
				updated:       ts,
				tableDesc:     tableDesc,
				prevDatums:    nil,
				prevTableDesc: tableDesc,
			}
			keyInsert, err := e.EncodeKey(context.Background(), rowInsert)
			require.NoError(t, err)
			keyInsert = append([]byte(nil), keyInsert...)
			valueInsert, err := e.EncodeValue(context.Background(), rowInsert)
			require.NoError(t, err)
			require.Equal(t, expected.insert, rowStringFn(keyInsert, valueInsert))

			rowDelete := encodeRow{
				datums:        row,
				deleted:       true,
				prevDatums:    row,
				updated:       ts,
				tableDesc:     tableDesc,
				prevTableDesc: tableDesc,
			}
			keyDelete, err := e.EncodeKey(context.Background(), rowDelete)
			require.NoError(t, err)
			keyDelete = append([]byte(nil), keyDelete...)
			valueDelete, err := e.EncodeValue(context.Background(), rowDelete)
			require.NoError(t, err)
			require.Equal(t, expected.delete, rowStringFn(keyDelete, valueDelete))

			resolved, err := e.EncodeResolvedTimestamp(context.Background(), tableDesc.GetName(), ts)
			require.NoError(t, err)
			require.Equal(t, expected.resolved, resolvedStringFn(resolved))
		})
	}
}

func TestAvroEncoder(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		ctx := context.Background()
		reg := cdctest.StartTestSchemaRegistry()
		defer reg.Close()

		sqlDB := sqlutils.MakeSQLRunner(db)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
		var ts1 string
		sqlDB.QueryRow(t,
			`INSERT INTO foo VALUES (1, 'bar'), (2, NULL) RETURNING cluster_logical_timestamp()`,
		).Scan(&ts1)

		foo := feed(t, f, `CREATE CHANGEFEED FOR foo `+
			`WITH format=$1, confluent_schema_registry=$2, diff, resolved`,
			changefeedbase.OptFormatAvro, reg.URL())
		defer closeFeed(t, foo)
		assertPayloadsAvro(t, reg, foo, []string{
			`foo: {"a":{"long":1}}->{"after":{"foo":{"a":{"long":1},"b":{"string":"bar"}}},"before":null}`,
			`foo: {"a":{"long":2}}->{"after":{"foo":{"a":{"long":2},"b":null}},"before":null}`,
		})
		resolved := expectResolvedTimestampAvro(t, reg, foo)
		if ts := parseTimeToHLC(t, ts1); resolved.LessEq(ts) {
			t.Fatalf(`expected a resolved timestamp greater than %s got %s`, ts, resolved)
		}

		fooUpdated := feed(t, f, `CREATE CHANGEFEED FOR foo `+
			`WITH format=$1, confluent_schema_registry=$2, diff, updated`,
			changefeedbase.OptFormatAvro, reg.URL())
		defer closeFeed(t, fooUpdated)
		// Skip over the first two rows since we don't know the statement timestamp.
		_, err := fooUpdated.Next()
		require.NoError(t, err)
		_, err = fooUpdated.Next()
		require.NoError(t, err)

		var ts2 string
		require.NoError(t, crdb.ExecuteTx(ctx, db, nil /* txopts */, func(tx *gosql.Tx) error {
			return tx.QueryRow(
				`INSERT INTO foo VALUES (3, 'baz') RETURNING cluster_logical_timestamp()`,
			).Scan(&ts2)
		}))
		assertPayloadsAvro(t, reg, fooUpdated, []string{
			`foo: {"a":{"long":3}}->{"after":{"foo":{"a":{"long":3},"b":{"string":"baz"}}},` +
				`"before":null,` +
				`"updated":{"string":"` + ts2 + `"}}`,
		})
	}

	t.Run(`sinkless`, sinklessTest(testFn))
	t.Run(`enterprise`, enterpriseTest(testFn))
}

func TestAvroEncoderWithTLS(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tableDesc, err := parseTableDesc(`CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
	require.NoError(t, err)
	row := rowenc.EncDatumRow{
		rowenc.EncDatum{Datum: tree.NewDInt(1)},
		rowenc.EncDatum{Datum: tree.NewDString(`bar`)},
	}
	ts := hlc.Timestamp{WallTime: 1, Logical: 2}

	opts := map[string]string{
		changefeedbase.OptFormat:   "experimental_avro",
		changefeedbase.OptEnvelope: "key_only",
	}
	expected := struct {
		insert   string
		delete   string
		resolved string
	}{
		insert:   `{"a":{"long":1}}->`,
		delete:   `{"a":{"long":1}}->`,
		resolved: `{"resolved":{"string":"1.0000000002"}}`,
	}

	t.Run("format=experimental_avro,envelope=key_only", func(t *testing.T) {
		cert, certBase64, err := cdctest.NewCACertBase64Encoded()
		require.NoError(t, err)

		var rowStringFn func([]byte, []byte) string
		var resolvedStringFn func([]byte) string
		reg, err := cdctest.StartTestSchemaRegistryWithTLS(cert)
		require.NoError(t, err)
		defer reg.Close()

		params := url.Values{}
		params.Add("ca_cert", certBase64)
		regURL, err := url.Parse(reg.URL())
		require.NoError(t, err)
		regURL.RawQuery = params.Encode()
		opts[changefeedbase.OptConfluentSchemaRegistry] = regURL.String()

		rowStringFn = func(k, v []byte) string {
			key, value := avroToJSON(t, reg, k), avroToJSON(t, reg, v)
			return fmt.Sprintf(`%s->%s`, key, value)
		}
		resolvedStringFn = func(r []byte) string {
			return string(avroToJSON(t, reg, r))
		}

		target := jobspb.ChangefeedTarget{
			StatementTimeName: tableDesc.GetName(),
		}
		targets := jobspb.ChangefeedTargets{}
		targets[tableDesc.GetID()] = target

		e, err := getEncoder(context.Background(), opts, targets)
		require.NoError(t, err)

		rowInsert := encodeRow{
			datums:        row,
			updated:       ts,
			tableDesc:     tableDesc,
			prevDatums:    nil,
			prevTableDesc: tableDesc,
		}
		keyInsert, err := e.EncodeKey(context.Background(), rowInsert)
		require.NoError(t, err)
		keyInsert = append([]byte(nil), keyInsert...)
		valueInsert, err := e.EncodeValue(context.Background(), rowInsert)
		require.NoError(t, err)
		require.Equal(t, expected.insert, rowStringFn(keyInsert, valueInsert))

		rowDelete := encodeRow{
			datums:        row,
			deleted:       true,
			prevDatums:    row,
			updated:       ts,
			tableDesc:     tableDesc,
			prevTableDesc: tableDesc,
		}
		keyDelete, err := e.EncodeKey(context.Background(), rowDelete)
		require.NoError(t, err)
		keyDelete = append([]byte(nil), keyDelete...)
		valueDelete, err := e.EncodeValue(context.Background(), rowDelete)
		require.NoError(t, err)
		require.Equal(t, expected.delete, rowStringFn(keyDelete, valueDelete))

		resolved, err := e.EncodeResolvedTimestamp(context.Background(), tableDesc.GetName(), ts)
		require.NoError(t, err)
		require.Equal(t, expected.resolved, resolvedStringFn(resolved))

		noCertReg, err := cdctest.StartTestSchemaRegistryWithTLS(nil)
		require.NoError(t, err)
		defer noCertReg.Close()
		opts[changefeedbase.OptConfluentSchemaRegistry] = noCertReg.URL()

		_, err = getEncoder(context.Background(), opts, targets)
		require.EqualError(t, err, fmt.Sprintf("schema registry unavailable: retryable changefeed error: "+
			"Get \"%s/mode\": x509: certificate signed by unknown authority",
			opts[changefeedbase.OptConfluentSchemaRegistry]))

		wrongCert, _, err := cdctest.NewCACertBase64Encoded()
		require.NoError(t, err)

		wrongCertReg, err := cdctest.StartTestSchemaRegistryWithTLS(wrongCert)
		require.NoError(t, err)
		defer wrongCertReg.Close()
		opts[changefeedbase.OptConfluentSchemaRegistry] = wrongCertReg.URL()

		_, err = getEncoder(context.Background(), opts, targets)
		require.EqualError(t, err, fmt.Sprintf("schema registry unavailable: retryable changefeed error: "+
			"Get \"%s/mode\": x509: certificate signed by unknown authority",
			opts[changefeedbase.OptConfluentSchemaRegistry]))
	})
}

func TestAvroArray(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		reg := cdctest.StartTestSchemaRegistry()
		defer reg.Close()

		sqlDB := sqlutils.MakeSQLRunner(db)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b INT[])`)
		sqlDB.Exec(t,
			`INSERT INTO foo VALUES
			(1, ARRAY[10,20,30]),
			(2, NULL),
			(3, ARRAY[42, NULL, 42, 43]),
			(4, ARRAY[]),
			(5, ARRAY[1,2,3,4,NULL,6]),
			(6, ARRAY[1,2,3,4,NULL,6,7,NULL,9])`,
		)

		foo := feed(t, f, `CREATE CHANGEFEED FOR foo `+
			`WITH format=$1, confluent_schema_registry=$2, diff, resolved`,
			changefeedbase.OptFormatAvro, reg.URL())
		defer closeFeed(t, foo)
		assertPayloadsAvro(t, reg, foo, []string{
			`foo: {"a":{"long":1}}->{"after":{"foo":{"a":{"long":1},"b":{"array":[{"long":10},{"long":20},{"long":30}]}}},"before":null}`,
			`foo: {"a":{"long":2}}->{"after":{"foo":{"a":{"long":2},"b":null}},"before":null}`,
			`foo: {"a":{"long":3}}->{"after":{"foo":{"a":{"long":3},"b":{"array":[{"long":42},null,{"long":42},{"long":43}]}}},"before":null}`,
			`foo: {"a":{"long":4}}->{"after":{"foo":{"a":{"long":4},"b":{"array":[]}}},"before":null}`,
			`foo: {"a":{"long":5}}->{"after":{"foo":{"a":{"long":5},"b":{"array":[{"long":1},{"long":2},{"long":3},{"long":4},null,{"long":6}]}}},"before":null}`,
			`foo: {"a":{"long":6}}->{"after":{"foo":{"a":{"long":6},"b":{"array":[{"long":1},{"long":2},{"long":3},{"long":4},null,{"long":6},{"long":7},null,{"long":9}]}}},"before":null}`,
		})

		sqlDB.Exec(t, `UPDATE foo SET b = ARRAY[0,0,0] where a=1`)
		sqlDB.Exec(t, `UPDATE foo SET b = ARRAY[0,0,0,0] where a=2`)

		assertPayloadsAvro(t, reg, foo, []string{
			`foo: {"a":{"long":1}}->{"after":{"foo":{"a":{"long":1},"b":{"array":[{"long":0},{"long":0},{"long":0}]}}},` +
				`"before":{"foo_before":{"a":{"long":1},"b":{"array":[{"long":10},{"long":20},{"long":30}]}}}}`,
			`foo: {"a":{"long":2}}->{"after":{"foo":{"a":{"long":2},"b":{"array":[{"long":0},{"long":0},{"long":0},{"long":0}]}}},` +
				`"before":{"foo_before":{"a":{"long":2},"b":null}}}`,
		})

	}

	t.Run(`sinkless`, sinklessTest(testFn))
	t.Run(`enterprise`, enterpriseTest(testFn))
}

func TestAvroArrayCap(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		reg := cdctest.StartTestSchemaRegistry()
		defer reg.Close()

		sqlDB := sqlutils.MakeSQLRunner(db)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b INT[])`)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (0, ARRAY[])`)

		foo := feed(t, f, `CREATE CHANGEFEED FOR foo `+
			`WITH format=$1, confluent_schema_registry=$2`,
			changefeedbase.OptFormatAvro, reg.URL())
		defer closeFeed(t, foo)
		assertPayloadsAvro(t, reg, foo, []string{
			`foo: {"a":{"long":0}}->{"after":{"foo":{"a":{"long":0},"b":{"array":[]}}}}`,
		})

		sqlDB.Exec(t, `INSERT INTO foo VALUES (8, ARRAY[null,null,null,null,null,null,null,null])`)

		assertPayloadsAvro(t, reg, foo, []string{
			`foo: {"a":{"long":8}}->{"after":{"foo":{"a":{"long":8},"b":{"array":[null,null,null,null,null,null,null,null]}}}}`,
		})

		sqlDB.Exec(t, `INSERT INTO foo VALUES (4, ARRAY[null,null,null,null])`)

		assertPayloadsAvro(t, reg, foo, []string{
			`foo: {"a":{"long":4}}->{"after":{"foo":{"a":{"long":4},"b":{"array":[null,null,null,null]}}}}`,
		})

		sqlDB.Exec(t, `INSERT INTO foo VALUES (5, ARRAY[null,null,null,null,null])`)

		assertPayloadsAvro(t, reg, foo, []string{
			`foo: {"a":{"long":5}}->{"after":{"foo":{"a":{"long":5},"b":{"array":[null,null,null,null,null]}}}}`,
		})

	}

	t.Run(`sinkless`, sinklessTest(testFn))
	t.Run(`enterprise`, enterpriseTest(testFn))
}

func TestAvroCollatedString(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		reg := cdctest.StartTestSchemaRegistry()
		defer reg.Close()

		sqlDB := sqlutils.MakeSQLRunner(db)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b string collate "fr-CA")`)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (1, 'désolée' collate "fr-CA")`)

		foo := feed(t, f, `CREATE CHANGEFEED FOR foo `+
			`WITH format=$1, confluent_schema_registry=$2`,
			changefeedbase.OptFormatAvro, reg.URL())
		defer closeFeed(t, foo)
		assertPayloadsAvro(t, reg, foo, []string{
			`foo: {"a":{"long":1}}->{"after":{"foo":{"a":{"long":1},"b":{"string":"désolée"}}}}`,
		})
	}

	t.Run(`sinkless`, sinklessTest(testFn))
	t.Run(`enterprise`, enterpriseTest(testFn))
}

func TestAvroEnum(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		reg := cdctest.StartTestSchemaRegistry()
		defer reg.Close()

		sqlDB := sqlutils.MakeSQLRunner(db)
		sqlDB.Exec(t, `CREATE TYPE status AS ENUM ('open', 'closed', 'inactive')`)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b status, c int default 0)`)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (1, 'open')`)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (2, null)`)

		foo := feed(t, f, `CREATE CHANGEFEED FOR foo `+
			`WITH format=$1, confluent_schema_registry=$2`,
			changefeedbase.OptFormatAvro, reg.URL())
		defer closeFeed(t, foo)
		assertPayloadsAvro(t, reg, foo, []string{
			`foo: {"a":{"long":1}}->{"after":{"foo":{"a":{"long":1},"b":{"string":"open"},"c":{"long":0}}}}`,
			`foo: {"a":{"long":2}}->{"after":{"foo":{"a":{"long":2},"b":null,"c":{"long":0}}}}`,
		})

		sqlDB.Exec(t, `ALTER TYPE status ADD value 'review'`)
		sqlDB.Exec(t, `INSERT INTO foo values (4, 'review')`)

		assertPayloadsAvro(t, reg, foo, []string{
			`foo: {"a":{"long":4}}->{"after":{"foo":{"a":{"long":4},"b":{"string":"review"},"c":{"long":0}}}}`,
		})

		// Renaming an enum type doesn't count as a change itself but gets picked up by the encoder
		sqlDB.Exec(t, `ALTER TYPE status RENAME value 'open' to 'active'`)
		sqlDB.Exec(t, `INSERT INTO foo values (3, 'active')`)
		sqlDB.Exec(t, `UPDATE foo set c=1 where a=1`)

		assertPayloadsAvro(t, reg, foo, []string{
			`foo: {"a":{"long":3}}->{"after":{"foo":{"a":{"long":3},"b":{"string":"active"},"c":{"long":0}}}}`,
			`foo: {"a":{"long":1}}->{"after":{"foo":{"a":{"long":1},"b":{"string":"active"},"c":{"long":1}}}}`,
		})

		// Enum can be part of a compound primary key
		sqlDB.Exec(t, `CREATE TABLE soft_deletes (a INT, b status, c INT default 0, PRIMARY KEY (a,b))`)
		sqlDB.Exec(t, `INSERT INTO soft_deletes values (0, 'active')`)

		sd := feed(t, f, `CREATE CHANGEFEED FOR soft_deletes `+
			`WITH format=$1, confluent_schema_registry=$2`,
			changefeedbase.OptFormatAvro, reg.URL())
		defer closeFeed(t, sd)
		assertPayloadsAvro(t, reg, sd, []string{
			`soft_deletes: {"a":{"long":0},"b":{"string":"active"}}->{"after":{"soft_deletes":{"a":{"long":0},"b":{"string":"active"},"c":{"long":0}}}}`,
		})

		sqlDB.Exec(t, `ALTER TYPE status RENAME value 'active' to 'open'`)
		sqlDB.Exec(t, `UPDATE soft_deletes set c=1 where a=0`)

		assertPayloadsAvro(t, reg, sd, []string{
			`soft_deletes: {"a":{"long":0},"b":{"string":"open"}}->{"after":{"soft_deletes":{"a":{"long":0},"b":{"string":"open"},"c":{"long":1}}}}`,
		})

	}

	t.Run(`sinkless`, sinklessTest(testFn))
	t.Run(`enterprise`, enterpriseTest(testFn))
}

func TestAvroSchemaNaming(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		reg := cdctest.StartTestSchemaRegistry()
		defer reg.Close()

		sqlDB := sqlutils.MakeSQLRunner(db)
		sqlDB.Exec(t, `CREATE DATABASE movr`)
		sqlDB.Exec(t, `CREATE TABLE movr.drivers (id INT PRIMARY KEY, name STRING)`)
		sqlDB.Exec(t,
			`INSERT INTO movr.drivers VALUES (1, 'Alice')`,
		)

		movrFeed := feed(t, f, `CREATE CHANGEFEED FOR movr.drivers `+
			`WITH format=$1, confluent_schema_registry=$2`,
			changefeedbase.OptFormatAvro, reg.URL())
		defer closeFeed(t, movrFeed)

		assertPayloadsAvro(t, reg, movrFeed, []string{
			`drivers: {"id":{"long":1}}->{"after":{"drivers":{"id":{"long":1},"name":{"string":"Alice"}}}}`,
		})

		assertRegisteredSubjects(t, reg, []string{
			`drivers-key`,
			`drivers-value`,
		})

		fqnFeed := feed(t, f, `CREATE CHANGEFEED FOR movr.drivers `+
			`WITH format=$1, confluent_schema_registry=$2, full_table_name`,
			changefeedbase.OptFormatAvro, reg.URL())
		defer closeFeed(t, fqnFeed)

		assertPayloadsAvro(t, reg, fqnFeed, []string{
			`movr.public.drivers: {"id":{"long":1}}->{"after":{"drivers":{"id":{"long":1},"name":{"string":"Alice"}}}}`,
		})

		assertRegisteredSubjects(t, reg, []string{
			`drivers-key`,
			`drivers-value`,
			`movr.public.drivers-key`,
			`movr.public.drivers-value`,
		})

		prefixFeed := feed(t, f, `CREATE CHANGEFEED FOR movr.drivers `+
			`WITH format=$1, confluent_schema_registry=$2, avro_schema_prefix=super`,
			changefeedbase.OptFormatAvro, reg.URL())
		defer closeFeed(t, prefixFeed)

		assertPayloadsAvro(t, reg, prefixFeed, []string{
			`drivers: {"id":{"long":1}}->{"after":{"super.drivers":{"id":{"long":1},"name":{"string":"Alice"}}}}`,
		})

		assertRegisteredSubjects(t, reg, []string{
			`drivers-key`,
			`drivers-value`,
			`movr.public.drivers-key`,
			`movr.public.drivers-value`,
			`superdrivers-key`,
			`superdrivers-value`,
		})

		prefixFQNFeed := feed(t, f, `CREATE CHANGEFEED FOR movr.drivers `+
			`WITH format=$1, confluent_schema_registry=$2, avro_schema_prefix=super, full_table_name`,
			changefeedbase.OptFormatAvro, reg.URL())
		defer closeFeed(t, prefixFQNFeed)

		assertPayloadsAvro(t, reg, prefixFQNFeed, []string{
			`movr.public.drivers: {"id":{"long":1}}->{"after":{"super.drivers":{"id":{"long":1},"name":{"string":"Alice"}}}}`,
		})

		assertRegisteredSubjects(t, reg, []string{
			`drivers-key`,
			`drivers-value`,
			`movr.public.drivers-key`,
			`movr.public.drivers-value`,
			`superdrivers-key`,
			`superdrivers-value`,
			`supermovr.public.drivers-key`,
			`supermovr.public.drivers-value`,
		})

		//Both changes to the subject are also reflected in the schema name in the posted schemas
		require.Contains(t, reg.SchemaForSubject(`supermovr.public.drivers-key`), `supermovr`)
		require.Contains(t, reg.SchemaForSubject(`supermovr.public.drivers-value`), `supermovr`)
	}

	t.Run(`enterprise`, enterpriseTest(testFn))
}

func TestAvroSchemaNamespace(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		reg := cdctest.StartTestSchemaRegistry()
		defer reg.Close()

		sqlDB := sqlutils.MakeSQLRunner(db)
		sqlDB.Exec(t, `CREATE DATABASE movr`)
		sqlDB.Exec(t, `CREATE TABLE movr.drivers (id INT PRIMARY KEY, name STRING)`)
		sqlDB.Exec(t,
			`INSERT INTO movr.drivers VALUES (1, 'Alice')`,
		)

		noNamespaceFeed := feed(t, f, `CREATE CHANGEFEED FOR movr.drivers `+
			`WITH format=$1, confluent_schema_registry=$2`,
			changefeedbase.OptFormatAvro, reg.URL())
		defer closeFeed(t, noNamespaceFeed)

		assertPayloadsAvro(t, reg, noNamespaceFeed, []string{
			`drivers: {"id":{"long":1}}->{"after":{"drivers":{"id":{"long":1},"name":{"string":"Alice"}}}}`,
		})

		namespaceFeed := feed(t, f, `CREATE CHANGEFEED FOR movr.drivers `+
			`WITH format=$1, confluent_schema_registry=$2, avro_schema_prefix=super`,
			changefeedbase.OptFormatAvro, reg.URL())
		defer closeFeed(t, namespaceFeed)

		assertPayloadsAvro(t, reg, namespaceFeed, []string{
			`drivers: {"id":{"long":1}}->{"after":{"super.drivers":{"id":{"long":1},"name":{"string":"Alice"}}}}`,
		})

		require.NotContains(t, reg.SchemaForSubject(`drivers-key`), `namespace`)
		require.NotContains(t, reg.SchemaForSubject(`drivers-value`), `namespace`)
		require.Contains(t, reg.SchemaForSubject(`superdrivers-key`), `"namespace":"super"`)
		require.Contains(t, reg.SchemaForSubject(`superdrivers-value`), `"namespace":"super"`)
	}

	t.Run(`enterprise`, enterpriseTest(testFn))
}

func TestTableNameCollision(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		reg := cdctest.StartTestSchemaRegistry()
		defer reg.Close()

		sqlDB := sqlutils.MakeSQLRunner(db)
		sqlDB.Exec(t, `CREATE DATABASE movr`)
		sqlDB.Exec(t, `CREATE DATABASE printr`)
		sqlDB.Exec(t, `CREATE TABLE movr.drivers (id INT PRIMARY KEY, name STRING)`)
		sqlDB.Exec(t, `CREATE TABLE printr.drivers (id INT PRIMARY KEY, version INT)`)
		sqlDB.Exec(t,
			`INSERT INTO movr.drivers VALUES (1, 'Alice'), (2, NULL)`,
		)
		sqlDB.Exec(t,
			`INSERT INTO printr.drivers VALUES (1, 100), (2, NULL)`,
		)

		movrFeed := feed(t, f, `CREATE CHANGEFEED FOR movr.drivers `+
			`WITH format=$1, confluent_schema_registry=$2, diff, resolved`,
			changefeedbase.OptFormatAvro, reg.URL())
		defer closeFeed(t, movrFeed)

		printrFeed := feed(t, f, `CREATE CHANGEFEED FOR printr.drivers `+
			`WITH format=$1, confluent_schema_registry=$2, diff, resolved`,
			changefeedbase.OptFormatAvro, reg.URL())
		defer closeFeed(t, printrFeed)

		comboFeed := feed(t, f, `CREATE CHANGEFEED FOR printr.drivers, movr.drivers `+
			`WITH format=$1, confluent_schema_registry=$2, diff, resolved, full_table_name`,
			changefeedbase.OptFormatAvro, reg.URL())
		defer closeFeed(t, comboFeed)

		assertPayloadsAvro(t, reg, movrFeed, []string{
			`drivers: {"id":{"long":1}}->{"after":{"drivers":{"id":{"long":1},"name":{"string":"Alice"}}},"before":null}`,
			`drivers: {"id":{"long":2}}->{"after":{"drivers":{"id":{"long":2},"name":null}},"before":null}`,
		})

		assertPayloadsAvro(t, reg, printrFeed, []string{
			`drivers: {"id":{"long":1}}->{"after":{"drivers":{"id":{"long":1},"version":{"long":100}}},"before":null}`,
			`drivers: {"id":{"long":2}}->{"after":{"drivers":{"id":{"long":2},"version":null}},"before":null}`,
		})

		assertPayloadsAvro(t, reg, comboFeed, []string{
			`movr.public.drivers: {"id":{"long":1}}->{"after":{"drivers":{"id":{"long":1},"name":{"string":"Alice"}}},"before":null}`,
			`movr.public.drivers: {"id":{"long":2}}->{"after":{"drivers":{"id":{"long":2},"name":null}},"before":null}`,
			`printr.public.drivers: {"id":{"long":1}}->{"after":{"drivers":{"id":{"long":1},"version":{"long":100}}},"before":null}`,
			`printr.public.drivers: {"id":{"long":2}}->{"after":{"drivers":{"id":{"long":2},"version":null}},"before":null}`,
		})
	}

	t.Run(`enterprise`, enterpriseTest(testFn))
}

func TestAvroMigrateToUnsupportedColumn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		reg := cdctest.StartTestSchemaRegistry()
		defer reg.Close()

		sqlDB := sqlutils.MakeSQLRunner(db)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY)`)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (1)`)

		foo := feed(t, f, `CREATE CHANGEFEED FOR foo `+
			`WITH format=$1, confluent_schema_registry=$2`,
			changefeedbase.OptFormatAvro, reg.URL())
		defer closeFeed(t, foo)
		assertPayloadsAvro(t, reg, foo, []string{
			`foo: {"a":{"long":1}}->{"after":{"foo":{"a":{"long":1}}}}`,
		})

		sqlDB.Exec(t, `ALTER TABLE foo ADD COLUMN b OID`)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (2, 3::OID)`)
		if _, err := foo.Next(); !testutils.IsError(err, `type OID not yet supported with avro`) {
			t.Fatalf(`expected "type OID not yet supported with avro" error got: %+v`, err)
		}
	}

	t.Run(`sinkless`, sinklessTest(testFn))
	t.Run(`enterprise`, enterpriseTest(testFn))
}

func TestAvroLedger(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, db *gosql.DB, f cdctest.TestFeedFactory) {
		reg := cdctest.StartTestSchemaRegistry()
		defer reg.Close()

		ctx := context.Background()
		gen := ledger.FromFlags(`--customers=1`)
		var l workloadsql.InsertsDataLoader
		_, err := workloadsql.Setup(ctx, db, gen, l)
		require.NoError(t, err)

		ledger := feed(t, f, `CREATE CHANGEFEED FOR customer, transaction, entry, session
	                       WITH format=$1, confluent_schema_registry=$2
	               `, changefeedbase.OptFormatAvro, reg.URL())
		defer closeFeed(t, ledger)

		assertPayloadsAvro(t, reg, ledger, []string{
			`customer: {"id":{"long":0}}->{"after":{"customer":{"balance":{"bytes.decimal":"0"},"created":{"long.timestamp-micros":"2114-03-27T13:14:27.287114Z"},"credit_limit":null,"currency_code":{"string":"XVL"},"id":{"long":0},"identifier":{"string":"0"},"is_active":{"boolean":true},"is_system_customer":{"boolean":true},"name":null,"sequence_number":{"long":-1}}}}`,
			`entry: {"id":{"long":1543039099823358511}}->{"after":{"entry":{"amount":{"bytes.decimal":"0"},"created_ts":{"long.timestamp-micros":"1990-12-09T23:47:23.811124Z"},"customer_id":{"long":0},"id":{"long":1543039099823358511},"money_type":{"string":"C"},"system_amount":{"bytes.decimal":"44061/500"},"transaction_id":{"string":"payment:a8c7f832-281a-39c5-8820-1fb960ff6465"}}}}`,
			`entry: {"id":{"long":2244708090865615074}}->{"after":{"entry":{"amount":{"bytes.decimal":"1/50"},"created_ts":{"long.timestamp-micros":"2075-11-08T22:07:12.055686Z"},"customer_id":{"long":0},"id":{"long":2244708090865615074},"money_type":{"string":"C"},"system_amount":{"bytes.decimal":"44061/500"},"transaction_id":{"string":"payment:a8c7f832-281a-39c5-8820-1fb960ff6465"}}}}`,
			`entry: {"id":{"long":3305628230121721621}}->{"after":{"entry":{"amount":{"bytes.decimal":"1/25"},"created_ts":{"long.timestamp-micros":"2185-01-30T21:38:15.06669Z"},"customer_id":{"long":0},"id":{"long":3305628230121721621},"money_type":{"string":"C"},"system_amount":{"bytes.decimal":"44061/500"},"transaction_id":{"string":"payment:e3757ca7-d646-66ea-2b8d-6116831cbb05"}}}}`,
			`entry: {"id":{"long":4151935814835861840}}->{"after":{"entry":{"amount":{"bytes.decimal":"3/50"},"created_ts":{"long.timestamp-micros":"2269-04-26T17:26:14.504652Z"},"customer_id":{"long":0},"id":{"long":4151935814835861840},"money_type":{"string":"C"},"system_amount":{"bytes.decimal":"44061/500"},"transaction_id":{"string":"payment:e3757ca7-d646-66ea-2b8d-6116831cbb05"}}}}`,
			`entry: {"id":{"long":5577006791947779410}}->{"after":{"entry":{"amount":{"bytes.decimal":"0"},"created_ts":{"long.timestamp-micros":"2185-11-07T09:42:42.666146Z"},"customer_id":{"long":0},"id":{"long":5577006791947779410},"money_type":{"string":"C"},"system_amount":{"bytes.decimal":"-88123/1000"},"transaction_id":{"string":"payment:a8c7f832-281a-39c5-8820-1fb960ff6465"}}}}`,
			`entry: {"id":{"long":6640668014774057861}}->{"after":{"entry":{"amount":{"bytes.decimal":"-1/50"},"created_ts":{"long.timestamp-micros":"2274-12-08T13:04:19.854595Z"},"customer_id":{"long":0},"id":{"long":6640668014774057861},"money_type":{"string":"C"},"system_amount":{"bytes.decimal":"-88123/1000"},"transaction_id":{"string":"payment:a8c7f832-281a-39c5-8820-1fb960ff6465"}}}}`,
			`entry: {"id":{"long":7414159922357799360}}->{"after":{"entry":{"amount":{"bytes.decimal":"-1/25"},"created_ts":{"long.timestamp-micros":"2290-08-26T02:12:41.861501Z"},"customer_id":{"long":0},"id":{"long":7414159922357799360},"money_type":{"string":"C"},"system_amount":{"bytes.decimal":"-88123/1000"},"transaction_id":{"string":"payment:e3757ca7-d646-66ea-2b8d-6116831cbb05"}}}}`,
			`entry: {"id":{"long":8475284246537043955}}->{"after":{"entry":{"amount":{"bytes.decimal":"-3/50"},"created_ts":{"long.timestamp-micros":"2048-07-21T10:02:40.114474Z"},"customer_id":{"long":0},"id":{"long":8475284246537043955},"money_type":{"string":"C"},"system_amount":{"bytes.decimal":"-88123/1000"},"transaction_id":{"string":"payment:e3757ca7-d646-66ea-2b8d-6116831cbb05"}}}}`,
			`session: {"session_id":{"string":"pLnfgDsc3WD9F3qNfHK6a95jjJkwzDkh0h3fhfUVuS0jZ9uVbhV4vC6AWX40IV"}}->{"after":{"session":{"data":{"string":"SP3NcHciWvqZTa3N06RxRTZHWUsaD7HEdz1ThbXfQ7pYSQ4n378l2VQKGNbSuJE0fQbzONJAAwdCxmM9BIabKERsUhPNmMmdf3eSJyYtqwcFiUILzXv3fcNIrWO8sToFgoilA1U2WxNeW2gdgUVDsEWJ88aX8tLF"},"expiry_timestamp":{"long.timestamp-micros":"2052-05-14T04:02:49.264975Z"},"last_update":{"long.timestamp-micros":"2070-03-19T02:10:22.552438Z"},"session_id":{"string":"pLnfgDsc3WD9F3qNfHK6a95jjJkwzDkh0h3fhfUVuS0jZ9uVbhV4vC6AWX40IV"}}}}`,
			`transaction: {"external_id":{"string":"payment:a8c7f832-281a-39c5-8820-1fb960ff6465"}}->{"after":{"transaction":{"context":{"string":"BpLnfgDsc3WD9F3qNfHK6a95jjJkwzDkh0h3fhfUVuS0jZ9uVbhV4vC6"},"created_ts":{"long.timestamp-micros":"2178-08-01T19:10:30.064819Z"},"external_id":{"string":"payment:a8c7f832-281a-39c5-8820-1fb960ff6465"},"response":{"bytes":"MDZSeFJUWkhXVXNhRDdIRWR6MVRoYlhmUTdwWVNRNG4zNzhsMlZRS0dOYlN1SkUwZlFiek9OSkFBd2RDeG1NOUJJYWJLRVJzVWhQTm1NbWRmM2VTSnlZdHF3Y0ZpVUlMelh2M2ZjTklyV084c1RvRmdvaWxBMVUyV3hOZVcyZ2RnVVZEc0VXSjg4YVg4dExGSjk1cVlVN1VyTjljdGVjd1p0NlM1empoRDF0WFJUbWtZS1FvTjAyRm1XblFTSzN3UkM2VUhLM0txQXR4alAzWm1EMmp0dDR6Z3I2TWVVam9BamNPMGF6TW10VTRZdHYxUDhPUG1tU05hOThkN3RzdGF4eTZuYWNuSkJTdUZwT2h5SVhFN1BKMURoVWtMWHFZWW5FTnVucWRzd3BUdzVVREdEUzM0bVNQWUs4dm11YjNYOXVYSXU3Rk5jSmpBUlFUM1JWaFZydDI0UDdpNnhDckw2RmM0R2N1SEMxNGthdW5BVFVQUkhqR211Vm14SHN5enpCYnlPb25xVlVTREsxVg=="},"reversed_by":null,"systimestamp":{"long.timestamp-micros":"2215-07-28T23:47:01.795499Z"},"tcomment":null,"transaction_type_reference":{"long":400},"username":{"string":"WX40IVUWSP3NcHciWvqZ"}}}}`,
			`transaction: {"external_id":{"string":"payment:e3757ca7-d646-66ea-2b8d-6116831cbb05"}}->{"after":{"transaction":{"context":{"string":"KSiOW5eQ8sklpgstrQZtAcrsGvPnYSXMOpFIpPzS8iI5N2gN7lD1rYjT"},"created_ts":{"long.timestamp-micros":"2062-07-27T13:21:35.213969Z"},"external_id":{"string":"payment:e3757ca7-d646-66ea-2b8d-6116831cbb05"},"response":{"bytes":"bWdkbHVWOFVvcWpRM1JBTTRTWjNzT0M4ZnlzZXN5NnRYeVZ5WTBnWkE1aVNJUjM4MFVPVWFwQTlPRmpuRWtiaHF6MlRZSlZIWUFtTHI5R0kyMlo3NFVmNjhDMFRRb2RDdWF0NmhmWmZSYmFlV1pJSFExMGJsSjVqQUd2VVRpWWJOWHZPcWowYlRUM24xNmNqQVNEN29qN2RPbVlVbTFua3AybnVvWTZGZlgzcVFHY09SbHZ2UHdHaHNDZWlZTmpvTVRoUXBFc0ZrSVpZVUxxNFFORzc1M25mamJYdENaUm4xSmVZV1hpUW1IWjJZMWIxb1lZbUtBS05aQjF1MGt1TU5ZbEFISW5hY1JoTkFzakd6bnBKSXZZdmZqWXk3MXV4OVI5SkRNQUMxRUtOSGFZVWNlekk4OHRHYmdwbWFGaXdIV09sUFQ5RUJVcHh6MHlCSnZGM1BKcW5jejVwMnpnVVhDcm9kZTV6UG5pNjJQV1dtMk5pSWVkSUxFaExLVVNHVWRNU1R5N1pmcjRyY2RJTw=="},"reversed_by":null,"systimestamp":{"long.timestamp-micros":"2229-01-11T00:56:37.706179Z"},"tcomment":null,"transaction_type_reference":{"long":400},"username":{"string":"XJXORIpfMGxOaIIFFFts"}}}}`,
		})
	}

	t.Run(`sinkless`, sinklessTest(testFn))
	t.Run(`enterprise`, enterpriseTest(testFn))
}
