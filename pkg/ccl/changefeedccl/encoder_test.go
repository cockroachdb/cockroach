// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"context"
	gosql "database/sql"
	"encoding/base64"
	gojson "encoding/json"
	"fmt"
	"math/rand"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach-go/v2/crdb"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcevent"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdctest"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/enum"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
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

	var opts []changefeedbase.EncodingOptions
	for _, f := range []changefeedbase.FormatType{changefeedbase.OptFormatJSON, changefeedbase.OptFormatAvro} {
		for _, e := range []changefeedbase.EnvelopeType{
			changefeedbase.OptEnvelopeKeyOnly, changefeedbase.OptEnvelopeRow, changefeedbase.OptEnvelopeWrapped,
		} {
			opts = append(opts,
				changefeedbase.EncodingOptions{Format: f, Envelope: e, UpdatedTimestamps: false, Diff: false},
				changefeedbase.EncodingOptions{Format: f, Envelope: e, UpdatedTimestamps: false, Diff: true},
				changefeedbase.EncodingOptions{Format: f, Envelope: e, UpdatedTimestamps: true, Diff: false},
				changefeedbase.EncodingOptions{Format: f, Envelope: e, UpdatedTimestamps: true, Diff: true},
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
			insert:   `[1]->`,
			delete:   `[1]->`,
			resolved: `{"__crdb__":{"resolved":"1.0000000002"}}`,
		},
		`format=json,envelope=key_only,updated,diff`: {
			insert:   `[1]->`,
			delete:   `[1]->`,
			resolved: `{"__crdb__":{"resolved":"1.0000000002"}}`,
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
			insert:   `[1]->{"a": 1, "b": "bar"}`,
			delete:   `[1]->`,
			resolved: `{"__crdb__":{"resolved":"1.0000000002"}}`,
		},
		`format=json,envelope=row,updated,diff`: {
			insert:   `[1]->{"__crdb__": {"updated": "1.0000000002"}, "a": 1, "b": "bar"}`,
			delete:   `[1]->`,
			resolved: `{"__crdb__":{"resolved":"1.0000000002"}}`,
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
		`format=avro,envelope=key_only`: {
			insert:   `{"a":{"long":1}}->`,
			delete:   `{"a":{"long":1}}->`,
			resolved: `{"resolved":{"string":"1.0000000002"}}`,
		},
		`format=avro,envelope=key_only,updated`: {
			err: `updated is only usable with envelope=wrapped`,
		},
		`format=avro,envelope=key_only,diff`: {
			err: `diff is only usable with envelope=wrapped`,
		},
		`format=avro,envelope=key_only,updated,diff`: {
			err: `updated is only usable with envelope=wrapped`,
		},
		`format=avro,envelope=row`: {
			err: `envelope=row is not supported with format=avro`,
		},
		`format=avro,envelope=row,updated`: {
			err: `envelope=row is not supported with format=avro`,
		},
		`format=avro,envelope=row,diff`: {
			err: `envelope=row is not supported with format=avro`,
		},
		`format=avro,envelope=row,updated,diff`: {
			err: `envelope=row is not supported with format=avro`,
		},
		`format=avro,envelope=wrapped`: {
			insert: `{"a":{"long":1}}->` +
				`{"after":{"foo":{"a":{"long":1},"b":{"string":"bar"}}}}`,
			delete:   `{"a":{"long":1}}->{"after":null}`,
			resolved: `{"resolved":{"string":"1.0000000002"}}`,
		},
		`format=avro,envelope=wrapped,updated`: {
			insert: `{"a":{"long":1}}->` +
				`{"after":{"foo":{"a":{"long":1},"b":{"string":"bar"}}},` +
				`"updated":{"string":"1.0000000002"}}`,
			delete:   `{"a":{"long":1}}->{"after":null,"updated":{"string":"1.0000000002"}}`,
			resolved: `{"resolved":{"string":"1.0000000002"}}`,
		},
		`format=avro,envelope=wrapped,diff`: {
			insert: `{"a":{"long":1}}->` +
				`{"after":{"foo":{"a":{"long":1},"b":{"string":"bar"}}},` +
				`"before":null}`,
			delete: `{"a":{"long":1}}->` +
				`{"after":null,` +
				`"before":{"foo_before":{"a":{"long":1},"b":{"string":"bar"}}}}`,
			resolved: `{"resolved":{"string":"1.0000000002"}}`,
		},
		`format=avro,envelope=wrapped,updated,diff`: {
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
		name := fmt.Sprintf("format=%s,envelope=%s", o.Format, o.Envelope)
		if o.UpdatedTimestamps {
			name += `,updated`
		}
		if o.Diff {
			name += `,diff`
		}
		t.Run(name, func(t *testing.T) {
			expected := expecteds[name]

			var rowStringFn func([]byte, []byte) string
			var resolvedStringFn func([]byte) string
			switch o.Format {
			case changefeedbase.OptFormatJSON:
				rowStringFn = func(k, v []byte) string { return fmt.Sprintf(`%s->%s`, k, v) }
				resolvedStringFn = func(r []byte) string { return string(r) }
			case changefeedbase.OptFormatAvro, changefeedbase.DeprecatedOptFormatAvro:
				reg := cdctest.StartTestSchemaRegistry()
				defer reg.Close()
				o.SchemaRegistryURI = reg.URL()
				rowStringFn = func(k, v []byte) string {
					key, value := avroToJSON(t, reg, k), avroToJSON(t, reg, v)
					return fmt.Sprintf(`%s->%s`, key, value)
				}
				resolvedStringFn = func(r []byte) string {
					return string(avroToJSON(t, reg, r))
				}
			default:
				t.Fatalf(`unknown format: %s`, o.Format)
			}

			targets := changefeedbase.Targets{}
			targets.Add(changefeedbase.Target{
				Type:              jobspb.ChangefeedTargetSpecification_PRIMARY_FAMILY_ONLY,
				TableID:           tableDesc.GetID(),
				StatementTimeName: changefeedbase.StatementTimeName(tableDesc.GetName()),
			})

			if len(expected.err) > 0 {
				require.EqualError(t, o.Validate(), expected.err)
				return
			}
			require.NoError(t, o.Validate())
			e, err := getEncoder(context.Background(), o, targets, false, nil, nil, getTestingEnrichedSourceProvider(o))
			require.NoError(t, err)

			rowInsert := cdcevent.TestingMakeEventRow(tableDesc, 0, row, false)
			prevRow := cdcevent.TestingMakeEventRow(tableDesc, 0, nil, false)
			evCtx := eventContext{updated: ts}

			keyInsert, err := e.EncodeKey(context.Background(), rowInsert)
			require.NoError(t, err)
			keyInsert = append([]byte(nil), keyInsert...)
			valueInsert, err := e.EncodeValue(context.Background(), evCtx, rowInsert, prevRow)
			require.NoError(t, err)
			require.Equal(t, expected.insert, rowStringFn(keyInsert, valueInsert))

			rowDelete := cdcevent.TestingMakeEventRow(tableDesc, 0, row, true)
			prevRow = cdcevent.TestingMakeEventRow(tableDesc, 0, row, false)

			keyDelete, err := e.EncodeKey(context.Background(), rowDelete)
			require.NoError(t, err)
			keyDelete = append([]byte(nil), keyDelete...)
			valueDelete, err := e.EncodeValue(context.Background(), evCtx, rowDelete, prevRow)
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

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		ctx := context.Background()

		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
		var ts1 string
		sqlDB.QueryRow(t,
			`INSERT INTO foo VALUES (1, 'bar'), (2, NULL) RETURNING cluster_logical_timestamp()`,
		).Scan(&ts1)

		foo := feed(t, f, fmt.Sprintf(`CREATE CHANGEFEED FOR foo `+
			`WITH format=%s, diff, resolved`, changefeedbase.OptFormatAvro))
		defer closeFeed(t, foo)
		assertPayloads(t, foo, []string{
			`foo: {"a":{"long":1}}->{"after":{"foo":{"a":{"long":1},"b":{"string":"bar"}}},"before":null}`,
			`foo: {"a":{"long":2}}->{"after":{"foo":{"a":{"long":2},"b":null}},"before":null}`,
		})
		resolved := expectResolvedTimestampAvro(t, foo)
		if ts := parseTimeToHLC(t, ts1); resolved.LessEq(ts) {
			t.Fatalf(`expected a resolved timestamp greater than %s got %s`, ts, resolved)
		}

		fooUpdated := feed(t, f, fmt.Sprintf(`CREATE CHANGEFEED FOR foo `+
			`WITH format=%s, diff, updated`, changefeedbase.OptFormatAvro))
		defer closeFeed(t, fooUpdated)
		// Skip over the first two rows since we don't know the statement timestamp.
		_, err := fooUpdated.Next()
		require.NoError(t, err)
		_, err = fooUpdated.Next()
		require.NoError(t, err)

		var ts2 string
		require.NoError(t, crdb.ExecuteTx(ctx, s.DB, nil /* txopts */, func(tx *gosql.Tx) error {
			return tx.QueryRow(
				`INSERT INTO foo VALUES (3, 'baz') RETURNING cluster_logical_timestamp()`,
			).Scan(&ts2)
		}))
		assertPayloads(t, fooUpdated, []string{
			`foo: {"a":{"long":3}}->{"after":{"foo":{"a":{"long":3},"b":{"string":"baz"}}},` +
				`"before":null,` +
				`"updated":{"string":"` + ts2 + `"}}`,
		})
	}

	cdcTest(t, testFn, feedTestForceSink("kafka"))
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

	opts := changefeedbase.EncodingOptions{
		Format:   changefeedbase.OptFormatAvro,
		Envelope: changefeedbase.OptEnvelopeKeyOnly,
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

	for _, setClientCert := range []bool{true, false} {
		t.Run(fmt.Sprintf("setClientCert=%t", setClientCert), func(t *testing.T) {
			cert, certBase64, err := cdctest.NewCACertBase64Encoded()
			require.NoError(t, err)

			var rowStringFn func([]byte, []byte) string
			var resolvedStringFn func([]byte) string
			reg, err := cdctest.StartTestSchemaRegistryWithTLS(cert, setClientCert)
			require.NoError(t, err)
			defer reg.Close()

			params := url.Values{}
			params.Add("ca_cert", certBase64)
			if setClientCert {
				clientCertPEM, clientKeyPEM, err := cdctest.GenerateClientCertAndKey(cert)
				require.NoError(t, err)
				params.Add("client_cert", base64.StdEncoding.EncodeToString(clientCertPEM))
				params.Add("client_key", base64.StdEncoding.EncodeToString(clientKeyPEM))
			}
			regURL, err := url.Parse(reg.URL())
			require.NoError(t, err)
			regURL.RawQuery = params.Encode()
			opts.SchemaRegistryURI = regURL.String()

			rowStringFn = func(k, v []byte) string {
				key, value := avroToJSON(t, reg, k), avroToJSON(t, reg, v)
				return fmt.Sprintf(`%s->%s`, key, value)
			}
			resolvedStringFn = func(r []byte) string {
				return string(avroToJSON(t, reg, r))
			}

			targets := changefeedbase.Targets{}
			targets.Add(changefeedbase.Target{
				Type:              jobspb.ChangefeedTargetSpecification_PRIMARY_FAMILY_ONLY,
				TableID:           tableDesc.GetID(),
				StatementTimeName: changefeedbase.StatementTimeName(tableDesc.GetName()),
			})

			e, err := getEncoder(context.Background(), opts, targets, false, nil, nil, getTestingEnrichedSourceProvider(opts))
			require.NoError(t, err)

			rowInsert := cdcevent.TestingMakeEventRow(tableDesc, 0, row, false)
			var prevRow cdcevent.Row
			evCtx := eventContext{updated: ts}
			keyInsert, err := e.EncodeKey(context.Background(), rowInsert)
			require.NoError(t, err)
			keyInsert = append([]byte(nil), keyInsert...)
			valueInsert, err := e.EncodeValue(context.Background(), evCtx, rowInsert, prevRow)
			require.NoError(t, err)
			require.Equal(t, expected.insert, rowStringFn(keyInsert, valueInsert))

			rowDelete := cdcevent.TestingMakeEventRow(tableDesc, 0, row, true)
			prevRow = cdcevent.TestingMakeEventRow(tableDesc, 0, row, false)

			keyDelete, err := e.EncodeKey(context.Background(), rowDelete)
			require.NoError(t, err)
			keyDelete = append([]byte(nil), keyDelete...)
			valueDelete, err := e.EncodeValue(context.Background(), evCtx, rowDelete, prevRow)
			require.NoError(t, err)
			require.Equal(t, expected.delete, rowStringFn(keyDelete, valueDelete))

			resolved, err := e.EncodeResolvedTimestamp(context.Background(), tableDesc.GetName(), ts)
			require.NoError(t, err)
			require.Equal(t, expected.resolved, resolvedStringFn(resolved))

			noCertReg, err := cdctest.StartTestSchemaRegistryWithTLS(nil, false)
			require.NoError(t, err)
			defer noCertReg.Close()
			opts.SchemaRegistryURI = noCertReg.URL()

			enc, err := getEncoder(context.Background(), opts, targets, false, nil, nil, getTestingEnrichedSourceProvider(opts))
			require.NoError(t, err)
			_, err = enc.EncodeKey(context.Background(), rowInsert)
			require.Regexp(t, "x509", err)

			wrongCert, _, err := cdctest.NewCACertBase64Encoded()
			require.NoError(t, err)

			wrongCertReg, err := cdctest.StartTestSchemaRegistryWithTLS(wrongCert, false)
			require.NoError(t, err)
			defer wrongCertReg.Close()
			opts.SchemaRegistryURI = wrongCertReg.URL()

			enc, err = getEncoder(context.Background(), opts, targets, false, nil, nil, getTestingEnrichedSourceProvider(opts))
			require.NoError(t, err)
			_, err = enc.EncodeKey(context.Background(), rowInsert)
			require.Regexp(t, `contacting confluent schema registry.*: x509`, err)
		})
	}
}

func TestAvroArray(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
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

		foo := feed(t, f, fmt.Sprintf(`CREATE CHANGEFEED FOR foo `+
			`WITH format=%s, diff, resolved`, changefeedbase.OptFormatAvro))
		defer closeFeed(t, foo)
		assertPayloads(t, foo, []string{
			`foo: {"a":{"long":1}}->{"after":{"foo":{"a":{"long":1},"b":{"array":[{"long":10},{"long":20},{"long":30}]}}},"before":null}`,
			`foo: {"a":{"long":2}}->{"after":{"foo":{"a":{"long":2},"b":null}},"before":null}`,
			`foo: {"a":{"long":3}}->{"after":{"foo":{"a":{"long":3},"b":{"array":[{"long":42},null,{"long":42},{"long":43}]}}},"before":null}`,
			`foo: {"a":{"long":4}}->{"after":{"foo":{"a":{"long":4},"b":{"array":[]}}},"before":null}`,
			`foo: {"a":{"long":5}}->{"after":{"foo":{"a":{"long":5},"b":{"array":[{"long":1},{"long":2},{"long":3},{"long":4},null,{"long":6}]}}},"before":null}`,
			`foo: {"a":{"long":6}}->{"after":{"foo":{"a":{"long":6},"b":{"array":[{"long":1},{"long":2},{"long":3},{"long":4},null,{"long":6},{"long":7},null,{"long":9}]}}},"before":null}`,
		})

		sqlDB.Exec(t, `UPDATE foo SET b = ARRAY[0,0,0] where a=1`)
		sqlDB.Exec(t, `UPDATE foo SET b = ARRAY[0,0,0,0] where a=2`)

		assertPayloads(t, foo, []string{
			`foo: {"a":{"long":1}}->{"after":{"foo":{"a":{"long":1},"b":{"array":[{"long":0},{"long":0},{"long":0}]}}},` +
				`"before":{"foo_before":{"a":{"long":1},"b":{"array":[{"long":10},{"long":20},{"long":30}]}}}}`,
			`foo: {"a":{"long":2}}->{"after":{"foo":{"a":{"long":2},"b":{"array":[{"long":0},{"long":0},{"long":0},{"long":0}]}}},` +
				`"before":{"foo_before":{"a":{"long":2},"b":null}}}`,
		})

	}

	cdcTest(t, testFn, feedTestForceSink("kafka"))
}

func TestAvroArrayCap(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b INT[])`)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (0, ARRAY[])`)

		foo := feed(t, f, fmt.Sprintf(`CREATE CHANGEFEED FOR foo `+
			`WITH format=%s`, changefeedbase.OptFormatAvro))
		defer closeFeed(t, foo)
		assertPayloads(t, foo, []string{
			`foo: {"a":{"long":0}}->{"after":{"foo":{"a":{"long":0},"b":{"array":[]}}}}`,
		})

		sqlDB.Exec(t, `INSERT INTO foo VALUES (8, ARRAY[null,null,null,null,null,null,null,null])`)

		assertPayloads(t, foo, []string{
			`foo: {"a":{"long":8}}->{"after":{"foo":{"a":{"long":8},"b":{"array":[null,null,null,null,null,null,null,null]}}}}`,
		})

		sqlDB.Exec(t, `INSERT INTO foo VALUES (4, ARRAY[null,null,null,null])`)

		assertPayloads(t, foo, []string{
			`foo: {"a":{"long":4}}->{"after":{"foo":{"a":{"long":4},"b":{"array":[null,null,null,null]}}}}`,
		})

		sqlDB.Exec(t, `INSERT INTO foo VALUES (5, ARRAY[null,null,null,null,null])`)

		assertPayloads(t, foo, []string{
			`foo: {"a":{"long":5}}->{"after":{"foo":{"a":{"long":5},"b":{"array":[null,null,null,null,null]}}}}`,
		})

	}

	cdcTest(t, testFn, feedTestForceSink("kafka"))
}

func TestCollatedString(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	for _, tc := range []struct {
		format   changefeedbase.FormatType
		expected string
	}{
		{
			format:   changefeedbase.OptFormatAvro,
			expected: `foo: {"a":{"long":1}}->{"after":{"foo":{"a":{"long":1},"b":{"string":"désolée"}}}}`,
		},
		{
			format:   changefeedbase.OptFormatJSON,
			expected: `foo: [1]->{"after": {"a": 1, "b": "désolée"}}`,
		},
		{
			format:   changefeedbase.OptFormatCSV,
			expected: `foo: ->1,désolée`,
		},
	} {
		testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
			sqlDB := sqlutils.MakeSQLRunner(s.DB)
			sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b string collate "fr-CA")`)
			sqlDB.Exec(t, `INSERT INTO foo VALUES (1, 'désolée' collate "fr-CA")`)

			foo := feed(t, f, fmt.Sprintf(`CREATE CHANGEFEED FOR foo `+
				`WITH format=%s, initial_scan_only`, tc.format))
			defer closeFeed(t, foo)
			assertPayloads(t, foo, []string{
				tc.expected,
			})
		}
		cdcTest(t, testFn, feedTestForceSink("kafka"))
	}
}

func TestAvroEnum(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TYPE status AS ENUM ('open', 'closed', 'inactive')`)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b status, c int default 0)`)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (1, 'open')`)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (2, null)`)

		foo := feed(t, f, fmt.Sprintf(`CREATE CHANGEFEED FOR foo `+
			`WITH format=%s`, changefeedbase.OptFormatAvro))
		defer closeFeed(t, foo)
		assertPayloads(t, foo, []string{
			`foo: {"a":{"long":1}}->{"after":{"foo":{"a":{"long":1},"b":{"string":"open"},"c":{"long":0}}}}`,
			`foo: {"a":{"long":2}}->{"after":{"foo":{"a":{"long":2},"b":null,"c":{"long":0}}}}`,
		})

		sqlDB.Exec(t, `ALTER TYPE status ADD value 'review'`)
		sqlDB.Exec(t, `INSERT INTO foo values (4, 'review')`)

		assertPayloads(t, foo, []string{
			`foo: {"a":{"long":4}}->{"after":{"foo":{"a":{"long":4},"b":{"string":"review"},"c":{"long":0}}}}`,
		})

		// Renaming an enum type doesn't count as a change itself but gets picked up by the encoder
		sqlDB.Exec(t, `ALTER TYPE status RENAME value 'open' to 'active'`)
		sqlDB.Exec(t, `INSERT INTO foo values (3, 'active')`)
		sqlDB.Exec(t, `UPDATE foo set c=1 where a=1`)

		assertPayloads(t, foo, []string{
			`foo: {"a":{"long":3}}->{"after":{"foo":{"a":{"long":3},"b":{"string":"active"},"c":{"long":0}}}}`,
			`foo: {"a":{"long":1}}->{"after":{"foo":{"a":{"long":1},"b":{"string":"active"},"c":{"long":1}}}}`,
		})

		// Enum can be part of a compound primary key
		sqlDB.Exec(t, `CREATE TABLE soft_deletes (a INT, b status, c INT default 0, PRIMARY KEY (a,b))`)
		sqlDB.Exec(t, `INSERT INTO soft_deletes values (0, 'active')`)

		sd := feed(t, f, fmt.Sprintf(`CREATE CHANGEFEED FOR soft_deletes `+
			`WITH format=%s`, changefeedbase.OptFormatAvro))
		defer closeFeed(t, sd)
		assertPayloads(t, sd, []string{
			`soft_deletes: {"a":{"long":0},"b":{"string":"active"}}->{"after":{"soft_deletes":{"a":{"long":0},"b":{"string":"active"},"c":{"long":0}}}}`,
		})

		sqlDB.Exec(t, `ALTER TYPE status RENAME value 'active' to 'open'`)
		sqlDB.Exec(t, `UPDATE soft_deletes set c=1 where a=0`)

		assertPayloads(t, sd, []string{
			`soft_deletes: {"a":{"long":0},"b":{"string":"open"}}->{"after":{"soft_deletes":{"a":{"long":0},"b":{"string":"open"},"c":{"long":1}}}}`,
		})

	}

	cdcTest(t, testFn, feedTestForceSink("kafka"))
}

func TestAvroSchemaNaming(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)

		// The expected results depend on caching in the avro encoder.
		// With multiple workers, there are multiple encoders which each
		// maintain their own caches. Depending on the number of
		// workers, the results below may change, so disable parallel workers
		// here for simplicity.
		changefeedbase.EventConsumerWorkers.Override(
			context.Background(), &s.Server.ClusterSettings().SV, -1)

		sqlDB.Exec(t, `CREATE DATABASE movr`)
		sqlDB.Exec(t, `CREATE TABLE movr.drivers (id INT PRIMARY KEY, name STRING)`)
		sqlDB.Exec(t,
			`INSERT INTO movr.drivers VALUES (1, 'Alice')`,
		)

		movrFeed := feed(t, f, fmt.Sprintf(`CREATE CHANGEFEED FOR movr.drivers `+
			`WITH format=%s`, changefeedbase.OptFormatAvro))
		defer closeFeed(t, movrFeed)

		foo := movrFeed.(*kafkaFeed)

		assertPayloads(t, movrFeed, []string{
			`drivers: {"id":{"long":1}}->{"after":{"drivers":{"id":{"long":1},"name":{"string":"Alice"}}}}`,
		})

		assertRegisteredSubjects(t, foo.registry, []string{
			`drivers-key`,
			`drivers-value`,
		})

		fqnFeed := feed(t, f, fmt.Sprintf(`CREATE CHANGEFEED FOR movr.drivers `+
			`WITH format=%s, full_table_name`, changefeedbase.OptFormatAvro))
		defer closeFeed(t, fqnFeed)

		foo = fqnFeed.(*kafkaFeed)

		assertPayloads(t, fqnFeed, []string{
			`movr.public.drivers: {"id":{"long":1}}->{"after":{"drivers":{"id":{"long":1},"name":{"string":"Alice"}}}}`,
		})

		assertRegisteredSubjects(t, foo.registry, []string{
			`movr.public.drivers-key`,
			`movr.public.drivers-value`,
		})

		prefixFeed := feed(t, f, fmt.Sprintf(`CREATE CHANGEFEED FOR movr.drivers `+
			`WITH format=%s, avro_schema_prefix=super`,
			changefeedbase.OptFormatAvro))
		defer closeFeed(t, prefixFeed)

		foo = prefixFeed.(*kafkaFeed)

		assertPayloads(t, prefixFeed, []string{
			`drivers: {"id":{"long":1}}->{"after":{"super.drivers":{"id":{"long":1},"name":{"string":"Alice"}}}}`,
		})

		assertRegisteredSubjects(t, foo.registry, []string{
			`superdrivers-key`,
			`superdrivers-value`,
		})

		prefixFQNFeed := feed(t, f, fmt.Sprintf(`CREATE CHANGEFEED FOR movr.drivers `+
			`WITH format=%s, avro_schema_prefix=super, full_table_name`, changefeedbase.OptFormatAvro))
		defer closeFeed(t, prefixFQNFeed)

		foo = prefixFQNFeed.(*kafkaFeed)

		assertPayloads(t, prefixFQNFeed, []string{
			`movr.public.drivers: {"id":{"long":1}}->{"after":{"super.drivers":{"id":{"long":1},"name":{"string":"Alice"}}}}`,
		})

		assertRegisteredSubjects(t, foo.registry, []string{
			`supermovr.public.drivers-key`,
			`supermovr.public.drivers-value`,
		})

		//Both changes to the subject are also reflected in the schema name in the posted schemas
		require.Contains(t, foo.registry.SchemaForSubject(`supermovr.public.drivers-key`), `supermovr`)
		require.Contains(t, foo.registry.SchemaForSubject(`supermovr.public.drivers-value`), `supermovr`)

		sqlDB.Exec(t, `ALTER TABLE movr.drivers ADD COLUMN vehicle_id int CREATE FAMILY volatile`)
		multiFamilyFeed := feed(t, f, fmt.Sprintf(`CREATE CHANGEFEED FOR movr.drivers `+
			`WITH format=%s, %s`, changefeedbase.OptFormatAvro, changefeedbase.OptSplitColumnFamilies))
		defer closeFeed(t, multiFamilyFeed)
		foo = multiFamilyFeed.(*kafkaFeed)

		sqlDB.Exec(t, `UPDATE movr.drivers SET vehicle_id = 1 WHERE id=1`)

		assertPayloads(t, multiFamilyFeed, []string{
			`drivers.primary: {"id":{"long":1}}->{"after":{"drivers_u002e_primary":{"id":{"long":1},"name":{"string":"Alice"}}}}`,
			`drivers.volatile: {"id":{"long":1}}->{"after":{"drivers_u002e_volatile":{"vehicle_id":{"long":1}}}}`,
		})

		assertRegisteredSubjects(t, foo.registry, []string{
			`drivers.primary-key`,
			`drivers.primary-value`,
			`drivers.volatile-value`,
		})

	}

	cdcTest(t, testFn, feedTestForceSink("kafka"), feedTestUseRootUserConnection)
}

func TestAvroSchemaNamespace(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE DATABASE movr`)
		sqlDB.Exec(t, `CREATE TABLE movr.drivers (id INT PRIMARY KEY, name STRING)`)
		sqlDB.Exec(t,
			`INSERT INTO movr.drivers VALUES (1, 'Alice')`,
		)

		noNamespaceFeed := feed(t, f, fmt.Sprintf(`CREATE CHANGEFEED FOR movr.drivers `+
			`WITH format=%s`, changefeedbase.OptFormatAvro))
		defer closeFeed(t, noNamespaceFeed)

		assertPayloads(t, noNamespaceFeed, []string{
			`drivers: {"id":{"long":1}}->{"after":{"drivers":{"id":{"long":1},"name":{"string":"Alice"}}}}`,
		})

		foo := noNamespaceFeed.(*kafkaFeed)

		require.NotContains(t, foo.registry.SchemaForSubject(`drivers-key`), `namespace`)
		require.NotContains(t, foo.registry.SchemaForSubject(`drivers-value`), `namespace`)

		namespaceFeed := feed(t, f, fmt.Sprintf(`CREATE CHANGEFEED FOR movr.drivers `+
			`WITH format=%s, avro_schema_prefix=super`, changefeedbase.OptFormatAvro))
		defer closeFeed(t, namespaceFeed)

		foo = namespaceFeed.(*kafkaFeed)

		assertPayloads(t, namespaceFeed, []string{
			`drivers: {"id":{"long":1}}->{"after":{"super.drivers":{"id":{"long":1},"name":{"string":"Alice"}}}}`,
		})

		require.Contains(t, foo.registry.SchemaForSubject(`superdrivers-key`), `"namespace":"super"`)
		require.Contains(t, foo.registry.SchemaForSubject(`superdrivers-value`), `"namespace":"super"`)
	}

	cdcTest(t, testFn, feedTestForceSink("kafka"), feedTestUseRootUserConnection)
}

func TestAvroSchemaHasExpectedTopLevelFields(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE DATABASE movr`)
		sqlDB.Exec(t, `CREATE TABLE movr.drivers (id INT PRIMARY KEY, name STRING)`)
		sqlDB.Exec(t,
			`INSERT INTO movr.drivers VALUES (1, 'Alice')`,
		)

		foo := feed(t, f, fmt.Sprintf(`CREATE CHANGEFEED FOR movr.drivers `+
			`WITH format=%s`, changefeedbase.OptFormatAvro))
		defer closeFeed(t, foo)

		assertPayloads(t, foo, []string{
			`drivers: {"id":{"long":1}}->{"after":{"drivers":{"id":{"long":1},"name":{"string":"Alice"}}}}`,
		})

		reg := foo.(*kafkaFeed).registry

		schemaJSON := reg.SchemaForSubject(`drivers-value`)
		var schema map[string]any
		require.NoError(t, gojson.Unmarshal([]byte(schemaJSON), &schema))
		var keys []string
		for k := range schema {
			keys = append(keys, k)
		}
		require.ElementsMatch(t, keys, []string{"type", "name", "fields"})
	}

	cdcTest(t, testFn, feedTestForceSink("kafka"), feedTestUseRootUserConnection)
}

func TestTableNameCollision(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
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

		movrFeed := feed(t, f, fmt.Sprintf(`CREATE CHANGEFEED FOR movr.drivers `+
			`WITH format=%s, diff, resolved`, changefeedbase.OptFormatAvro))
		defer closeFeed(t, movrFeed)

		printrFeed := feed(t, f, fmt.Sprintf(`CREATE CHANGEFEED FOR printr.drivers `+
			`WITH format=%s, diff, resolved`, changefeedbase.OptFormatAvro))
		defer closeFeed(t, printrFeed)

		comboFeed := feed(t, f, fmt.Sprintf(`CREATE CHANGEFEED FOR printr.drivers, movr.drivers `+
			`WITH format=%s, diff, resolved, full_table_name`, changefeedbase.OptFormatAvro))
		defer closeFeed(t, comboFeed)

		assertPayloads(t, movrFeed, []string{
			`drivers: {"id":{"long":1}}->{"after":{"drivers":{"id":{"long":1},"name":{"string":"Alice"}}},"before":null}`,
			`drivers: {"id":{"long":2}}->{"after":{"drivers":{"id":{"long":2},"name":null}},"before":null}`,
		})

		assertPayloads(t, printrFeed, []string{
			`drivers: {"id":{"long":1}}->{"after":{"drivers":{"id":{"long":1},"version":{"long":100}}},"before":null}`,
			`drivers: {"id":{"long":2}}->{"after":{"drivers":{"id":{"long":2},"version":null}},"before":null}`,
		})

		assertPayloads(t, comboFeed, []string{
			`movr.public.drivers: {"id":{"long":1}}->{"after":{"drivers":{"id":{"long":1},"name":{"string":"Alice"}}},"before":null}`,
			`movr.public.drivers: {"id":{"long":2}}->{"after":{"drivers":{"id":{"long":2},"name":null}},"before":null}`,
			`printr.public.drivers: {"id":{"long":1}}->{"after":{"drivers":{"id":{"long":1},"version":{"long":100}}},"before":null}`,
			`printr.public.drivers: {"id":{"long":2}}->{"after":{"drivers":{"id":{"long":2},"version":null}},"before":null}`,
		})
	}

	cdcTest(t, testFn, feedTestForceSink("kafka"), feedTestUseRootUserConnection, withDebugUseAfterFinish)
}

func TestAvroMigrateToUnsupportedColumn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY)`)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (1)`)

		foo := feed(t, f, fmt.Sprintf(`CREATE CHANGEFEED FOR foo `+
			`WITH format=%s`, changefeedbase.OptFormatAvro))
		defer closeFeed(t, foo)
		assertPayloads(t, foo, []string{
			`foo: {"a":{"long":1}}->{"after":{"foo":{"a":{"long":1}}}}`,
		})

		sqlDB.Exec(t, `ALTER TABLE foo ADD COLUMN b OID`)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (2, 3::OID)`)
		if _, err := foo.Next(); !testutils.IsError(err, `type OID not yet supported with avro`) {
			t.Fatalf(`expected "type OID not yet supported with avro" error got: %+v`, err)
		}
	}

	cdcTest(t, testFn, feedTestForceSink("kafka"))
}

func TestAvroLedger(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		ctx := context.Background()
		// assertions depend on this seed
		ledger.RandomSeed.Set(1)
		gen := ledger.FromFlags(`--customers=1`)
		var l workloadsql.InsertsDataLoader
		_, err := workloadsql.Setup(ctx, s.DB, gen, l)
		require.NoError(t, err)

		ledger := feed(t, f, fmt.Sprintf(`CREATE CHANGEFEED FOR customer, transaction, entry, session
	                       WITH format=%s`, changefeedbase.OptFormatAvro))
		defer closeFeed(t, ledger)

		assertPayloads(t, ledger, []string{
			`customer: {"id":{"long":0}}->{"after":{"customer":{"balance":{"bytes.decimal":"0"},"created":{"long.timestamp-micros":"2114-03-27T13:14:27.287114Z"},"credit_limit":null,"currency_code":{"string":"XVL"},"id":{"long":0},"identifier":{"string":"0"},"is_active":{"boolean":true},"is_system_customer":{"boolean":true},"name":null,"sequence_number":{"long":-1}}}}`,
			`entry: {"id":{"long":1543039099823358511}}->{"after":{"entry":{"amount":{"bytes.decimal":"0"},"created_ts":{"long.timestamp-micros":"1990-12-09T23:47:23.811124Z"},"customer_id":{"long":0},"id":{"long":1543039099823358511},"money_type":{"string":"C"},"system_amount":{"bytes.decimal":"88122259/1000000"},"transaction_id":{"string":"payment:a8c7f832-281a-39c5-8820-1fb960ff6465"}}}}`,
			`entry: {"id":{"long":2244708090865615074}}->{"after":{"entry":{"amount":{"bytes.decimal":"1/50"},"created_ts":{"long.timestamp-micros":"2075-11-08T22:07:12.055686Z"},"customer_id":{"long":0},"id":{"long":2244708090865615074},"money_type":{"string":"C"},"system_amount":{"bytes.decimal":"88122259/1000000"},"transaction_id":{"string":"payment:a8c7f832-281a-39c5-8820-1fb960ff6465"}}}}`,
			`entry: {"id":{"long":3305628230121721621}}->{"after":{"entry":{"amount":{"bytes.decimal":"1/25"},"created_ts":{"long.timestamp-micros":"2185-01-30T21:38:15.06669Z"},"customer_id":{"long":0},"id":{"long":3305628230121721621},"money_type":{"string":"C"},"system_amount":{"bytes.decimal":"88122259/1000000"},"transaction_id":{"string":"payment:e3757ca7-d646-66ea-2b8d-6116831cbb05"}}}}`,
			`entry: {"id":{"long":4151935814835861840}}->{"after":{"entry":{"amount":{"bytes.decimal":"3/50"},"created_ts":{"long.timestamp-micros":"2269-04-26T17:26:14.504652Z"},"customer_id":{"long":0},"id":{"long":4151935814835861840},"money_type":{"string":"C"},"system_amount":{"bytes.decimal":"88122259/1000000"},"transaction_id":{"string":"payment:e3757ca7-d646-66ea-2b8d-6116831cbb05"}}}}`,
			`entry: {"id":{"long":5577006791947779410}}->{"after":{"entry":{"amount":{"bytes.decimal":"0"},"created_ts":{"long.timestamp-micros":"2185-11-07T09:42:42.666146Z"},"customer_id":{"long":0},"id":{"long":5577006791947779410},"money_type":{"string":"C"},"system_amount":{"bytes.decimal":"-88122259/1000000"},"transaction_id":{"string":"payment:a8c7f832-281a-39c5-8820-1fb960ff6465"}}}}`,
			`entry: {"id":{"long":6640668014774057861}}->{"after":{"entry":{"amount":{"bytes.decimal":"-1/50"},"created_ts":{"long.timestamp-micros":"2274-12-08T13:04:19.854595Z"},"customer_id":{"long":0},"id":{"long":6640668014774057861},"money_type":{"string":"C"},"system_amount":{"bytes.decimal":"-88122259/1000000"},"transaction_id":{"string":"payment:a8c7f832-281a-39c5-8820-1fb960ff6465"}}}}`,
			`entry: {"id":{"long":7414159922357799360}}->{"after":{"entry":{"amount":{"bytes.decimal":"-1/25"},"created_ts":{"long.timestamp-micros":"2290-08-26T02:12:41.861501Z"},"customer_id":{"long":0},"id":{"long":7414159922357799360},"money_type":{"string":"C"},"system_amount":{"bytes.decimal":"-88122259/1000000"},"transaction_id":{"string":"payment:e3757ca7-d646-66ea-2b8d-6116831cbb05"}}}}`,
			`entry: {"id":{"long":8475284246537043955}}->{"after":{"entry":{"amount":{"bytes.decimal":"-3/50"},"created_ts":{"long.timestamp-micros":"2048-07-21T10:02:40.114474Z"},"customer_id":{"long":0},"id":{"long":8475284246537043955},"money_type":{"string":"C"},"system_amount":{"bytes.decimal":"-88122259/1000000"},"transaction_id":{"string":"payment:e3757ca7-d646-66ea-2b8d-6116831cbb05"}}}}`,
			`session: {"session_id":{"string":"pLnfgDsc3WD9F3qNfHK6a95jjJkwzDkh0h3fhfUVuS0jZ9uVbhV4vC6AWX40IV"}}->{"after":{"session":{"data":{"string":"SP3NcHciWvqZTa3N06RxRTZHWUsaD7HEdz1ThbXfQ7pYSQ4n378l2VQKGNbSuJE0fQbzONJAAwdCxmM9BIabKERsUhPNmMmdf3eSJyYtqwcFiUILzXv3fcNIrWO8sToFgoilA1U2WxNeW2gdgUVDsEWJ88aX8tLF"},"expiry_timestamp":{"long.timestamp-micros":"2052-05-14T04:02:49.264975Z"},"last_update":{"long.timestamp-micros":"2070-03-19T02:10:22.552438Z"},"session_id":{"string":"pLnfgDsc3WD9F3qNfHK6a95jjJkwzDkh0h3fhfUVuS0jZ9uVbhV4vC6AWX40IV"}}}}`,
			`transaction: {"external_id":{"string":"payment:a8c7f832-281a-39c5-8820-1fb960ff6465"}}->{"after":{"transaction":{"context":{"string":"BpLnfgDsc3WD9F3qNfHK6a95jjJkwzDkh0h3fhfUVuS0jZ9uVbhV4vC6"},"created_ts":{"long.timestamp-micros":"2178-08-01T19:10:30.064819Z"},"external_id":{"string":"payment:a8c7f832-281a-39c5-8820-1fb960ff6465"},"response":{"bytes":"MDZSeFJUWkhXVXNhRDdIRWR6MVRoYlhmUTdwWVNRNG4zNzhsMlZRS0dOYlN1SkUwZlFiek9OSkFBd2RDeG1NOUJJYWJLRVJzVWhQTm1NbWRmM2VTSnlZdHF3Y0ZpVUlMelh2M2ZjTklyV084c1RvRmdvaWxBMVUyV3hOZVcyZ2RnVVZEc0VXSjg4YVg4dExGSjk1cVlVN1VyTjljdGVjd1p0NlM1empoRDF0WFJUbWtZS1FvTjAyRm1XblFTSzN3UkM2VUhLM0txQXR4alAzWm1EMmp0dDR6Z3I2TWVVam9BamNPMGF6TW10VTRZdHYxUDhPUG1tU05hOThkN3RzdGF4eTZuYWNuSkJTdUZwT2h5SVhFN1BKMURoVWtMWHFZWW5FTnVucWRzd3BUdzVVREdEUzM0bVNQWUs4dm11YjNYOXVYSXU3Rk5jSmpBUlFUM1JWaFZydDI0UDdpNnhDckw2RmM0R2N1SEMxNGthdW5BVFVQUkhqR211Vm14SHN5enpCYnlPb25xVlVTREsxVg=="},"reversed_by":null,"systimestamp":{"long.timestamp-micros":"2215-07-28T23:47:01.795499Z"},"tcomment":null,"transaction_type_reference":{"long":400},"username":{"string":"WX40IVUWSP3NcHciWvqZ"}}}}`,
			`transaction: {"external_id":{"string":"payment:e3757ca7-d646-66ea-2b8d-6116831cbb05"}}->{"after":{"transaction":{"context":{"string":"KSiOW5eQ8sklpgstrQZtAcrsGvPnYSXMOpFIpPzS8iI5N2gN7lD1rYjT"},"created_ts":{"long.timestamp-micros":"2062-07-27T13:21:35.213969Z"},"external_id":{"string":"payment:e3757ca7-d646-66ea-2b8d-6116831cbb05"},"response":{"bytes":"bWdkbHVWOFVvcWpRM1JBTTRTWjNzT0M4ZnlzZXN5NnRYeVZ5WTBnWkE1aVNJUjM4MFVPVWFwQTlPRmpuRWtiaHF6MlRZSlZIWUFtTHI5R0kyMlo3NFVmNjhDMFRRb2RDdWF0NmhmWmZSYmFlV1pJSFExMGJsSjVqQUd2VVRpWWJOWHZPcWowYlRUM24xNmNqQVNEN29qN2RPbVlVbTFua3AybnVvWTZGZlgzcVFHY09SbHZ2UHdHaHNDZWlZTmpvTVRoUXBFc0ZrSVpZVUxxNFFORzc1M25mamJYdENaUm4xSmVZV1hpUW1IWjJZMWIxb1lZbUtBS05aQjF1MGt1TU5ZbEFISW5hY1JoTkFzakd6bnBKSXZZdmZqWXk3MXV4OVI5SkRNQUMxRUtOSGFZVWNlekk4OHRHYmdwbWFGaXdIV09sUFQ5RUJVcHh6MHlCSnZGM1BKcW5jejVwMnpnVVhDcm9kZTV6UG5pNjJQV1dtMk5pSWVkSUxFaExLVVNHVWRNU1R5N1pmcjRyY2RJTw=="},"reversed_by":null,"systimestamp":{"long.timestamp-micros":"2229-01-11T00:56:37.706179Z"},"tcomment":null,"transaction_type_reference":{"long":400},"username":{"string":"XJXORIpfMGxOaIIFFFts"}}}}`,
		})
	}

	cdcTest(t, testFn, feedTestForceSink("kafka"))
}

func BenchmarkEncoders(b *testing.B) {
	rng := randutil.NewTestRandWithSeed(2365865412074131521)

	// Initialize column types for tests; this is done before
	// benchmark runs so that all reruns use the same types as generated
	// by random seed above.
	const maxKeyCols = 4
	const maxValCols = 2<<maxKeyCols - maxKeyCols // 28 columns for a maximum of 32 columns in a row
	keyColTypes, valColTypes := makeTestTypes(rng, maxKeyCols, maxValCols)

	const numRows = 1024
	makeRowPool := func(numKeyCols int) (updatedRows, prevRows []cdcevent.Row, _ []*types.T) {
		colTypes := keyColTypes[:numKeyCols]
		colTypes = append(colTypes, valColTypes[:2<<numKeyCols-numKeyCols]...)
		encRows := randgen.RandEncDatumRowsOfTypes(rng, numRows, colTypes)

		for _, r := range encRows {
			updatedRow := cdcevent.TestingMakeEventRowFromEncDatums(r, colTypes, numKeyCols, false)
			updatedRows = append(updatedRows, updatedRow)
			// Generate previous row -- use same key datums, but update other datums.
			prevRowDatums := append(r[:numKeyCols], randgen.RandEncDatumRowOfTypes(rng, colTypes[numKeyCols:])...)
			prevRow := cdcevent.TestingMakeEventRowFromEncDatums(prevRowDatums, colTypes, numKeyCols, false)
			prevRows = append(prevRows, prevRow)
		}
		return updatedRows, prevRows, colTypes
	}

	type encodeFn func(encoder Encoder, updatedRow, prevRow cdcevent.Row) error
	encodeKey := func(encoder Encoder, updatedRow cdcevent.Row, _ cdcevent.Row) error {
		_, err := encoder.EncodeKey(context.Background(), updatedRow)
		return err
	}
	encodeValue := func(encoder Encoder, updatedRow, prevRow cdcevent.Row) error {
		evCtx := eventContext{
			updated: hlc.Timestamp{WallTime: 42},
			mvcc:    hlc.Timestamp{WallTime: 17},
			topic:   "testtopic",
		}
		_, err := encoder.EncodeValue(context.Background(), evCtx, updatedRow, prevRow)
		return err
	}

	var targets changefeedbase.Targets
	targets.Add(changefeedbase.Target{
		Type:              0,
		TableID:           42,
		FamilyName:        "primary",
		StatementTimeName: "table",
	})

	// bench executes benchmark.
	bench := func(b *testing.B, fn encodeFn, opts changefeedbase.EncodingOptions, updatedRows, prevRows []cdcevent.Row) {
		b.ReportAllocs()
		b.StopTimer()

		encoder, err := getEncoder(context.Background(), opts, targets, false, nil, nil,
			getTestingEnrichedSourceProvider(opts))
		if err != nil {
			b.Fatal(err)
		}
		b.StartTimer()

		for i := 0; i < b.N; i++ {
			if err := fn(encoder, updatedRows[i%len(updatedRows)], prevRows[i%len(prevRows)]); err != nil {
				b.Fatal(err)
			}
		}
	}

	rowOnly := []changefeedbase.EnvelopeType{changefeedbase.OptEnvelopeRow}
	rowAndWrapped := []changefeedbase.EnvelopeType{
		changefeedbase.OptEnvelopeRow, changefeedbase.OptEnvelopeWrapped,
	}

	for _, tc := range []struct {
		format         changefeedbase.FormatType
		benchEncodeKey bool
		supportsDiff   bool
		envelopes      []changefeedbase.EnvelopeType
	}{
		{
			format:         changefeedbase.OptFormatJSON,
			benchEncodeKey: true,
			supportsDiff:   true,
			envelopes:      rowAndWrapped,
		},
		{
			format:         changefeedbase.OptFormatCSV,
			benchEncodeKey: false,
			supportsDiff:   false,
			envelopes:      rowOnly,
		},
	} {
		b.Run(string(tc.format), func(b *testing.B) {
			for numKeyCols := 1; numKeyCols <= maxKeyCols; numKeyCols++ {
				updatedRows, prevRows, colTypes := makeRowPool(numKeyCols)
				b.Logf("column types: %v, keys: %v", colTypes, colTypes[:numKeyCols])

				if tc.benchEncodeKey {
					b.Run(fmt.Sprintf("encodeKey/%dcols", numKeyCols),
						func(b *testing.B) {
							opts := changefeedbase.EncodingOptions{Format: tc.format}
							bench(b, encodeKey, opts, updatedRows, prevRows)
						},
					)
				}

				for _, envelope := range tc.envelopes {
					for _, diff := range []bool{false, true} {
						// Run benchmark with/without diff (unless encoder does not support
						// diff, in which case we only run benchmark once).
						if tc.supportsDiff || !diff {
							b.Run(fmt.Sprintf("encodeValue/%dcols/envelope=%s/diff=%t",
								numKeyCols, envelope, diff),
								func(b *testing.B) {
									opts := changefeedbase.EncodingOptions{
										Format:            tc.format,
										Envelope:          envelope,
										KeyInValue:        envelope == changefeedbase.OptEnvelopeWrapped,
										TopicInValue:      envelope == changefeedbase.OptEnvelopeWrapped,
										UpdatedTimestamps: true,
										MVCCTimestamps:    true,
										Diff:              diff,
									}
									bench(b, encodeValue, opts, updatedRows, prevRows)
								},
							)
						}
					}
				}
			}
		})
	}
}

// makeTestTypes returns list of key and column types to test against.
func makeTestTypes(rng *rand.Rand, numKeyTypes, numColTypes int) (keyTypes, valTypes []*types.T) {
	var allTypes []*types.T
	for _, typ := range randgen.SeedTypes {
		switch typ {
		case types.AnyTuple:
		// Ignore AnyTuple -- it's not very interesting; we'll generate test tuples below.
		case types.RegClass, types.RegNamespace, types.RegProc, types.RegProcedure, types.RegRole, types.RegType:
		// Ignore a bunch of pseudo-OID types (just want regular OID)
		case types.Geometry, types.Geography:
		// Ignore geometry/geography: these types are insanely inefficient;
		// AsJson(Geo) -> MarshalGeo -> go JSON bytes ->  ParseJSON -> Go native -> json.JSON
		// Benchmarking this generates too much noise.
		// TODO: fix this.
		case types.Void:
		// Just not a very interesting thing to encode
		default:
			allTypes = append(allTypes, typ)
		}
	}

	// Add tuple types.
	var tupleTypes []*types.T
	makeTupleType := func() *types.T {
		contents := make([]*types.T, rng.Intn(6)) // Up to 6 fields
		for i := range contents {
			contents[i] = randgen.RandTypeFromSlice(rng, allTypes)
		}
		candidateTuple := types.MakeTuple(contents)
		// Ensure tuple type is unique.
		for _, t := range tupleTypes {
			if t.Equal(candidateTuple) {
				return nil
			}
		}
		tupleTypes = append(tupleTypes, candidateTuple)
		return candidateTuple
	}

	const numTupleTypes = 5
	for i := 0; i < numTupleTypes; i++ {
		var typ *types.T
		for typ == nil {
			typ = makeTupleType()
		}
		allTypes = append(allTypes, typ)
	}

	randTypes := func(numTypes int, mustBeKeyType bool) []*types.T {
		typs := make([]*types.T, numTypes)
		for i := range typs {
			typ := randgen.RandTypeFromSlice(rng, allTypes)
			for mustBeKeyType && colinfo.MustBeValueEncoded(typ) {
				typ = randgen.RandTypeFromSlice(rng, allTypes)
			}
			typs[i] = typ
		}
		return typs
	}

	return randTypes(numKeyTypes, true), randTypes(numColTypes, false)
}

func TestParquetEncoder(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		tests := []struct {
			name           string
			changefeedStmt string
		}{
			{
				name: "Without Compression",
				changefeedStmt: fmt.Sprintf(`CREATE CHANGEFEED FOR foo `+
					`WITH format=%s`, changefeedbase.OptFormatParquet),
			},
			{
				name: "With Compression",
				changefeedStmt: fmt.Sprintf(`CREATE CHANGEFEED FOR foo `+
					`WITH format=%s, compression='gzip'`, changefeedbase.OptFormatParquet),
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				sqlDB := sqlutils.MakeSQLRunner(s.DB)
				sqlDB.Exec(t, `CREATE TABLE foo (i INT PRIMARY KEY, x STRING, y INT, z FLOAT NOT NULL, a BOOL, c INT[], d REFCURSOR)`)
				defer sqlDB.Exec(t, `DROP TABLE FOO`)
				sqlDB.Exec(t, `INSERT INTO foo VALUES (1, 'Alice', 3, 0.5032135844230652, true,  ARRAY[], 'foo'), (2, 'Bob',
	2, CAST('nan' AS FLOAT),false, NULL, 'bar'),(3, NULL, NULL, 4.5, NULL,  ARRAY[1,NULL,3], NULL)`)
				foo := feed(t, f, test.changefeedStmt)
				defer closeFeed(t, foo)

				assertPayloads(t, foo, []string{
					`foo: [1]->{"after": {"a": true, "c": [], "d": "foo", "i": 1, "x": "Alice", "y": 3, "z": 0.5032135844230652}}`,
					`foo: [2]->{"after": {"a": false, "c": null, "d": "bar", "i": 2, "x": "Bob", "y": 2, "z": "NaN"}}`,
					`foo: [3]->{"after": {"a": null, "c": [1, null, 3], "d": null, "i": 3, "x": null, "y": null, "z": 4.5}}`,
				})

				sqlDB.Exec(t, `UPDATE foo SET x='wonderland' where i=1`)
				assertPayloads(t, foo, []string{
					`foo: [1]->{"after": {"a": true, "c": [], "d": "foo", "i": 1, "x": "wonderland", "y": 3, "z": 0.5032135844230652}}`,
				})

				sqlDB.Exec(t, `DELETE from foo where i=1`)
				assertPayloads(t, foo, []string{
					`foo: [1]->{"after": null}`,
				})
			})
		}
	}
	cdcTest(t, testFn, feedTestForceSink("cloudstorage"))
}

func TestJsonRountrip(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	rng, _ := randutil.NewTestRand()

	isFloatOrDecimal := func(typ *types.T) bool {
		return typ.Identical(types.Float4) || typ.Identical(types.Float) || typ.Identical(types.Decimal)
	}

	type test struct {
		name   string
		schema string
		datum  tree.Datum
	}
	tests := make([]test, 0)

	typesToTest := make([]*types.T, 0, 256)

	// Start with a set of all scalar types.
	for _, typ := range randgen.SeedTypes {
		switch typ.Family() {
		case types.ArrayFamily, types.TupleFamily:
		case types.VoidFamily, types.AnyFamily:
		default:
			typesToTest = append(typesToTest, typ)
		}
	}

	// Add arrays of all the scalar types which are supported.
	arrayTypesToTest := make([]*types.T, 0, 256)
	for oid := range types.ArrayOids {
		arrayTyp := types.OidToType[oid]
		for _, typ := range typesToTest {
			switch typ {
			case types.Jsonb:
				// Unsupported by sql/catalog/colinfo
			case types.TSQuery, types.TSVector, types.PGVector:
				// Unsupported by pkg/sql/parser
			default:
				if arrayTyp.InternalType.ArrayContents == typ {
					arrayTypesToTest = append(arrayTypesToTest, arrayTyp)
				}
			}
		}
	}
	typesToTest = append(typesToTest, arrayTypesToTest...)

	// Add enums.
	testEnum := createEnum(
		tree.EnumValueList{`open`, `closed`},
		tree.MakeUnqualifiedTypeName(`switch`),
	)
	typesToTest = append(typesToTest, testEnum)

	// Generate a test for each type with a random datum of that type.
	for _, typ := range typesToTest {
		datum := randgen.RandDatum(rng, typ, true /* nullOk */)

		// name can be "char" (with quotes), so needs to be escaped.
		escapedName := fmt.Sprintf("%s_table", strings.Replace(typ.String(), "\"", "", -1))

		randTypeTest := test{
			name:   escapedName,
			schema: fmt.Sprintf(`(a INT PRIMARY KEY, b %s)`, typ.SQLString()),
			datum:  datum,
		}
		tests = append(tests, randTypeTest)
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tableDesc, err := parseTableDesc(
				fmt.Sprintf(`CREATE TABLE "%s" %s`, test.name, test.schema))
			require.NoError(t, err)

			dRow := rowenc.EncDatumRow{rowenc.EncDatum{Datum: tree.NewDInt(1)}, rowenc.EncDatum{Datum: test.datum}}
			cdcRow := cdcevent.TestingMakeEventRow(tableDesc, 0, dRow, false)

			// TODO(#139660): test this with other envelopes.
			opts := jsonEncoderOptions{EncodingOptions: changefeedbase.EncodingOptions{Envelope: changefeedbase.OptEnvelopeBare}}
			encoder, err := makeJSONEncoder(context.Background(), opts, getTestingEnrichedSourceProvider(opts.EncodingOptions))
			require.NoError(t, err)

			// Encode the value to a string and parse it. Assert that the parsed json matches the
			// datum as JSON.
			bytes, err := encoder.EncodeValue(context.Background(), eventContext{}, cdcRow, cdcevent.Row{})
			require.NoError(t, err)

			j, err := json.ParseJSON(string(bytes))
			require.NoError(t, err)

			d, err := j.FetchValKey("b")
			require.NoError(t, err)

			j, err = tree.AsJSON(test.datum, sessiondatapb.DataConversionConfig{}, time.UTC)
			require.NoError(t, err)

			// Using JSON.Compare for Infinity or NaN equality does not work well.
			// In this case, we can just compare strings.
			if isFloatOrDecimal(test.datum.ResolvedType()) {
				require.Equal(t, d.String(), j.String())
			} else if dArr, ok := tree.AsDArray(test.datum); ok && isFloatOrDecimal(dArr.ParamTyp) {
				require.Equal(t, d.String(), j.String())
			} else {
				cmp, err := d.Compare(j)
				require.NoError(t, err)
				require.Equal(t, cmp, 0)
			}
		})
	}
}

// TestAvroWithRegionalTable tests how the avro encoder works with regional
// tables and with different envelope formats. This is a regression test for
// #119428.
func TestAvroWithRegionalTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tests := []struct {
		envelope string
		payload  []string
	}{
		{
			envelope: "wrapped",
			payload: []string{
				`table1: {"a":{"long":1},"crdb_region":{"string":"us-east1"}}->{"after":{"table1":{"a":{"long":1},"crdb_region":{"string":"us-east1"}}}}`,
				`table1: {"a":{"long":2},"crdb_region":{"string":"us-east1"}}->{"after":{"table1":{"a":{"long":2},"crdb_region":{"string":"us-east1"}}}}`,
				`table1: {"a":{"long":3},"crdb_region":{"string":"us-east1"}}->{"after":{"table1":{"a":{"long":3},"crdb_region":{"string":"us-east1"}}}}`,
			},
		},
		{
			envelope: "bare",
			payload: []string{
				`table1: {"a":{"long":1},"crdb_region":{"string":"us-east1"}}->{"record":{"table1":{"a":{"long":1},"crdb_region":{"string":"us-east1"}}}}`,
				`table1: {"a":{"long":2},"crdb_region":{"string":"us-east1"}}->{"record":{"table1":{"a":{"long":2},"crdb_region":{"string":"us-east1"}}}}`,
				`table1: {"a":{"long":3},"crdb_region":{"string":"us-east1"}}->{"record":{"table1":{"a":{"long":3},"crdb_region":{"string":"us-east1"}}}}`,
			},
		},
		{
			envelope: "key_only",
			payload: []string{
				`table1: {"a":{"long":1},"crdb_region":{"string":"us-east1"}}->`,
				`table1: {"a":{"long":2},"crdb_region":{"string":"us-east1"}}->`,
				`table1: {"a":{"long":3},"crdb_region":{"string":"us-east1"}}->`,
			},
		},
	}
	for _, test := range tests {
		t.Run(test.envelope, func(t *testing.T) {
			// Run the test with one and three(default) workers to test both the
			// default behaviour and the behaviour when
			// confluentAvroEncoder.keyCache and confluentAvroEncoder.valueCache
			// are used. With one worker, the cache is forced to be used during
			// encoding for the second row.
			testutils.RunTrueAndFalse(t, "overrideWithSingleWorker", func(t *testing.T, overrideWithSingleWorker bool) {
				// Clear the singleton cache to avoid pollution from other tests.
				// This needs to be done here since this test doesn't use the
				// cdcTest helper function.
				TestingClearSchemaRegistrySingleton()
				cluster, db, cleanup := startTestCluster(t)
				defer cleanup()
				if overrideWithSingleWorker {
					t.Logf("overriding number of parallel workers to one")
					changefeedbase.EventConsumerWorkers.Override(
						context.Background(), &cluster.ApplicationLayer(0).ClusterSettings().SV, 1)
				}

				sqlDB := sqlutils.MakeSQLRunner(db)
				sqlDB.Exec(t, `CREATE TABLE table1 (a INT PRIMARY KEY) LOCALITY REGIONAL BY ROW`)
				schemaReg := cdctest.StartTestSchemaRegistry()
				defer schemaReg.Close()
				stmt := fmt.Sprintf(`CREATE CHANGEFEED FOR TABLE table1 WITH format = avro, envelope = %s,
	confluent_schema_registry = "%s", schema_change_events = column_changes, schema_change_policy = nobackfill`,
					test.envelope, schemaReg.URL())

				f := makeKafkaFeedFactory(t, cluster, db)
				testFeed := feed(t, f, stmt)
				defer closeFeed(t, testFeed)

				sqlDB.Exec(t, `INSERT INTO table1(a) values(1), (2), (3)`)
				assertPayloads(t, testFeed, test.payload)
			})
		})
	}
}

// Create a thin, in-memory user-defined enum type
func createEnum(enumLabels tree.EnumValueList, typeName tree.TypeName) *types.T {

	members := make([]descpb.TypeDescriptor_EnumMember, len(enumLabels))
	physReps := enum.GenerateNEvenlySpacedBytes(len(enumLabels))
	for i := range enumLabels {
		members[i] = descpb.TypeDescriptor_EnumMember{
			LogicalRepresentation:  string(enumLabels[i]),
			PhysicalRepresentation: physReps[i],
			Capability:             descpb.TypeDescriptor_EnumMember_ALL,
		}
	}

	enumKind := descpb.TypeDescriptor_ENUM

	typeDesc := typedesc.NewBuilder(&descpb.TypeDescriptor{
		Name:        typeName.Type(),
		ID:          0,
		Kind:        enumKind,
		EnumMembers: members,
		Version:     1,
	}).BuildCreatedMutableType()

	typ, _ := typedesc.HydratedTFromDesc(context.Background(), &typeName, typeDesc, nil /* res */)

	testTypes[typeName.SQLString()] = typ

	return typ

}
