// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package avro

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcevent"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/linkedin/goavro/v2"
	"github.com/stretchr/testify/require"
)

type notDraining struct{}

func (notDraining) IsDraining() bool { return false }

// requireRetryable asserts that err is non-nil and would be retried by the
// changefeed rather than terminating it.
func requireRetryable(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err)
	require.Nil(t, changefeedbase.AsTerminalError(context.Background(), notDraining{}, err))
}

// wrappedEnvelope builds a wrapped-envelope value schema for the given table
// the same way the avro encoder does.
func wrappedEnvelope(
	t *testing.T, tableDesc catalog.TableDescriptor, opts EnvelopeOpts,
) *EnvelopeRecord {
	t.Helper()
	env, err := newWrappedEnvelope(t, tableDesc, opts, "")
	require.NoError(t, err)
	return env
}

func wrappedEnvelopeWithRegisteredSchema(
	t *testing.T, tableDesc catalog.TableDescriptor, opts EnvelopeOpts, registeredSchemaJSON string,
) (*EnvelopeRecord, error) {
	t.Helper()
	return newWrappedEnvelope(t, tableDesc, opts, registeredSchemaJSON)
}

func newWrappedEnvelope(
	t *testing.T, tableDesc catalog.TableDescriptor, opts EnvelopeOpts, registeredSchemaJSON string,
) (*EnvelopeRecord, error) {
	t.Helper()
	rowRecord, err := TableToAvroSchema(
		cdcevent.TestingMakeEventRow(tableDesc, 0, nil, false), SchemaNoSuffix, "", "")
	require.NoError(t, err)
	var before *DataRecord
	if opts.BeforeField {
		before, err = TableToAvroSchema(
			cdcevent.TestingMakeEventRow(tableDesc, 0, nil, false), `before`, "", "")
		require.NoError(t, err)
	}
	var after, record *DataRecord
	if opts.AfterField {
		after = rowRecord
	}
	if opts.RecordField {
		record = rowRecord
	}
	env, err := NewEnvelopeRecord(tableDesc.GetName(), opts, before, after, record, nil, "")
	if err != nil {
		return nil, err
	}
	if registeredSchemaJSON != "" {
		return NewEnvelopeRecordWithRegisteredSchema(env, registeredSchemaJSON)
	}
	return env, nil
}

// addRegisteredAfterField returns schemaJSON with field appended to the
// record nested in the "after" envelope field's union.
func addRegisteredAfterField(t *testing.T, schemaJSON string, field map[string]interface{}) string {
	t.Helper()
	var env map[string]interface{}
	require.NoError(t, json.Unmarshal([]byte(schemaJSON), &env))
	found := false
	for _, f := range env["fields"].([]interface{}) {
		fm := f.(map[string]interface{})
		if fm["name"] != "after" {
			continue
		}
		for _, member := range fm["type"].([]interface{}) {
			if rec, ok := member.(map[string]interface{}); ok && rec["type"] == "record" {
				rec["fields"] = append(rec["fields"].([]interface{}), field)
				found = true
			}
		}
	}
	require.True(t, found, "no after record found in %s", schemaJSON)
	out, err := json.Marshal(env)
	require.NoError(t, err)
	return string(out)
}

// decodeWithRegistered decodes an envelope message with a codec built from
// the registered schema, i.e. the way a consumer that fetched the stamped
// schema ID would, and returns the row record inside the "after" field.
func decodeWithRegistered(
	t *testing.T, registeredSchema string, encoded []byte, afterRecordName string,
) map[string]interface{} {
	t.Helper()
	codec, err := goavro.NewCodec(registeredSchema)
	require.NoError(t, err)
	native, rest, err := codec.NativeFromBinary(encoded)
	require.NoError(t, err)
	require.Empty(t, rest)
	env := native.(map[string]interface{})
	afterUnion := env["after"].(map[string]interface{})
	return afterUnion[afterRecordName].(map[string]interface{})
}

func TestNewEnvelopeRecordWithRegisteredSchema(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	narrowDesc, err := parseTableDesc(`CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
	require.NoError(t, err)
	narrowRows, err := parseValues(narrowDesc, `VALUES (1, 'x')`)
	require.NoError(t, err)
	afterRow := cdcevent.TestingMakeEventRow(narrowDesc, 0, narrowRows[0], false)
	var nilRow cdcevent.Row

	t.Run("fills registered-only row columns with their null defaults", func(t *testing.T) {
		wideDesc, err := parseTableDesc(`CREATE TABLE foo (a INT PRIMARY KEY, c INT, b STRING, d STRING)`)
		require.NoError(t, err)
		registered := wrappedEnvelope(t, wideDesc, EnvelopeOpts{AfterField: true}).Schema()

		adopted, err := wrappedEnvelopeWithRegisteredSchema(
			t, narrowDesc, EnvelopeOpts{AfterField: true}, registered)
		require.NoError(t, err)

		// The envelope encodes with the registered schema verbatim.
		registeredFP, err := SchemaFingerprint(registered)
		require.NoError(t, err)
		require.Equal(t, registeredFP, adopted.codec.Rabin)

		encoded, err := adopted.BinaryFromRow(nil, Metadata{}, nilRow, afterRow, nilRow, "")
		require.NoError(t, err)
		after := decodeWithRegistered(t, registered, encoded, "foo")
		require.Equal(t, map[string]interface{}{"long": int64(1)}, after["a"])
		require.Equal(t, map[string]interface{}{"string": "x"}, after["b"])
		require.Nil(t, after["c"])
		require.Nil(t, after["d"])

		again, err := adopted.BinaryFromRow(nil, Metadata{}, nilRow, afterRow, nilRow, "")
		require.NoError(t, err)
		require.Equal(t, encoded, again)
	})

	t.Run("fills a registered-only array column with its null default", func(t *testing.T) {
		wideDesc, err := parseTableDesc(`CREATE TABLE foo (a INT PRIMARY KEY, b STRING, c INT[])`)
		require.NoError(t, err)
		registered := wrappedEnvelope(t, wideDesc, EnvelopeOpts{AfterField: true}).Schema()

		adopted, err := wrappedEnvelopeWithRegisteredSchema(
			t, narrowDesc, EnvelopeOpts{AfterField: true}, registered)
		require.NoError(t, err)

		encoded, err := adopted.BinaryFromRow(nil, Metadata{}, nilRow, afterRow, nilRow, "")
		require.NoError(t, err)
		after := decodeWithRegistered(t, registered, encoded, "foo")
		require.Equal(t, map[string]interface{}{"long": int64(1)}, after["a"])
		require.Equal(t, map[string]interface{}{"string": "x"}, after["b"])
		require.Nil(t, after["c"])
	})

	t.Run("covers before and after records", func(t *testing.T) {
		wideDesc, err := parseTableDesc(`CREATE TABLE foo (a INT PRIMARY KEY, b STRING, c STRING)`)
		require.NoError(t, err)
		opts := EnvelopeOpts{AfterField: true, BeforeField: true}
		registered := wrappedEnvelope(t, wideDesc, opts).Schema()

		adopted, err := wrappedEnvelopeWithRegisteredSchema(t, narrowDesc, opts, registered)
		require.NoError(t, err)

		encoded, err := adopted.BinaryFromRow(nil, Metadata{}, afterRow, afterRow, nilRow, "")
		require.NoError(t, err)
		codec, err := goavro.NewCodec(registered)
		require.NoError(t, err)
		native, _, err := codec.NativeFromBinary(encoded)
		require.NoError(t, err)
		env := native.(map[string]interface{})
		before := env["before"].(map[string]interface{})["foo_before"].(map[string]interface{})
		require.Equal(t, map[string]interface{}{"string": "x"}, before["b"])
		require.Nil(t, before["c"])
	})

	t.Run("covers the record envelope field", func(t *testing.T) {
		wideDesc, err := parseTableDesc(`CREATE TABLE foo (a INT PRIMARY KEY, b STRING, c STRING)`)
		require.NoError(t, err)
		opts := EnvelopeOpts{RecordField: true}
		registered := wrappedEnvelope(t, wideDesc, opts).Schema()

		adopted, err := wrappedEnvelopeWithRegisteredSchema(t, narrowDesc, opts, registered)
		require.NoError(t, err)

		encoded, err := adopted.BinaryFromRow(nil, Metadata{}, nilRow, nilRow, afterRow, "")
		require.NoError(t, err)
		codec, err := goavro.NewCodec(registered)
		require.NoError(t, err)
		native, _, err := codec.NativeFromBinary(encoded)
		require.NoError(t, err)
		env := native.(map[string]interface{})
		record := env["record"].(map[string]interface{})["foo"].(map[string]interface{})
		require.Equal(t, map[string]interface{}{"string": "x"}, record["b"])
		require.Nil(t, record["c"])
	})

	t.Run("adopts registered column order", func(t *testing.T) {
		reorderedDesc, err := parseTableDesc(`CREATE TABLE foo (b STRING, a INT PRIMARY KEY)`)
		require.NoError(t, err)
		registered := wrappedEnvelope(t, reorderedDesc, EnvelopeOpts{AfterField: true}).Schema()

		adopted, err := wrappedEnvelopeWithRegisteredSchema(
			t, narrowDesc, EnvelopeOpts{AfterField: true}, registered)
		require.NoError(t, err)

		encoded, err := adopted.BinaryFromRow(nil, Metadata{}, nilRow, afterRow, nilRow, "")
		require.NoError(t, err)
		after := decodeWithRegistered(t, registered, encoded, "foo")
		require.Equal(t, map[string]interface{}{"long": int64(1)}, after["a"])
		require.Equal(t, map[string]interface{}{"string": "x"}, after["b"])
	})

	t.Run("decodes its own encodings back into table columns", func(t *testing.T) {
		wideDesc, err := parseTableDesc(`CREATE TABLE foo (a INT PRIMARY KEY, b STRING, c STRING)`)
		require.NoError(t, err)
		registered := wrappedEnvelope(t, wideDesc, EnvelopeOpts{AfterField: true}).Schema()

		adopted, err := wrappedEnvelopeWithRegisteredSchema(
			t, narrowDesc, EnvelopeOpts{AfterField: true}, registered)
		require.NoError(t, err)

		// The row record keeps its locally generated codec, so it still
		// round-trips the table's columns.
		encoded, err := adopted.After.BinaryFromRow(nil, afterRow.ForEachColumn())
		require.NoError(t, err)
		decoded, err := adopted.After.RowFromBinary(encoded)
		require.NoError(t, err)
		require.Len(t, decoded, 2)
		require.Equal(t, narrowRows[0][0].Datum, decoded[0].Datum)
		require.Equal(t, narrowRows[0][1].Datum, decoded[1].Datum)
	})

	t.Run("does not modify the local envelope", func(t *testing.T) {
		wideDesc, err := parseTableDesc(`CREATE TABLE foo (a INT PRIMARY KEY, b STRING, c STRING)`)
		require.NoError(t, err)
		registered := wrappedEnvelope(t, wideDesc, EnvelopeOpts{AfterField: true}).Schema()
		local := wrappedEnvelope(t, narrowDesc, EnvelopeOpts{AfterField: true})
		localSchema := local.Schema()

		envelope, err := NewEnvelopeRecordWithRegisteredSchema(local, registered)
		require.NoError(t, err)
		require.NotSame(t, local, envelope)
		require.Equal(t, localSchema, local.Schema())
		require.Equal(t, registered, envelope.Schema())
	})

	t.Run("uses the registered schema when it is identical", func(t *testing.T) {
		registered := wrappedEnvelope(t, narrowDesc, EnvelopeOpts{AfterField: true}).Schema()
		adopted, err := wrappedEnvelopeWithRegisteredSchema(
			t, narrowDesc, EnvelopeOpts{AfterField: true}, registered)
		require.NoError(t, err)
		require.Equal(t, registered, adopted.codec.Schema())
	})

	t.Run("uses a reordered shared union and encodes both null and non-null", func(t *testing.T) {
		// Reorder the "b" column's union in the registered schema so null is not
		// its first branch. goavro looks union branches up by type name, so the
		// changefeed's encodings must still round-trip under the registered
		// codec even though the wire branch index differs from the local schema.
		local := wrappedEnvelope(t, narrowDesc, EnvelopeOpts{AfterField: true})
		var env map[string]interface{}
		require.NoError(t, json.Unmarshal([]byte(local.Schema()), &env))
		for _, f := range env["fields"].([]interface{}) {
			fm := f.(map[string]interface{})
			if fm["name"] != "after" {
				continue
			}
			for _, member := range fm["type"].([]interface{}) {
				rec, ok := member.(map[string]interface{})
				if !ok || rec["type"] != "record" {
					continue
				}
				for _, rf := range rec["fields"].([]interface{}) {
					rfm := rf.(map[string]interface{})
					if rfm["name"] == "b" {
						rfm["type"] = []interface{}{"string", "null"}
						// A null default is only valid when null is the union's
						// first branch, which it no longer is.
						delete(rfm, "default")
					}
				}
			}
		}
		out, err := json.Marshal(env)
		require.NoError(t, err)
		registered := string(out)
		adopted, err := wrappedEnvelopeWithRegisteredSchema(
			t, narrowDesc, EnvelopeOpts{AfterField: true}, registered)
		require.NoError(t, err)

		encoded, err := adopted.BinaryFromRow(nil, Metadata{}, nilRow, afterRow, nilRow, "")
		require.NoError(t, err)
		after := decodeWithRegistered(t, registered, encoded, "foo")
		require.Equal(t, map[string]interface{}{"string": "x"}, after["b"])

		// A null value for the reordered union must also encode and decode.
		nullRows, err := parseValues(narrowDesc, `VALUES (2, NULL)`)
		require.NoError(t, err)
		nullAfterRow := cdcevent.TestingMakeEventRow(narrowDesc, 0, nullRows[0], false)
		encoded, err = adopted.BinaryFromRow(nil, Metadata{}, nilRow, nullAfterRow, nilRow, "")
		require.NoError(t, err)
		after = decodeWithRegistered(t, registered, encoded, "foo")
		require.Nil(t, after["b"])
	})

	t.Run("errors when the registered schema omits a local column", func(t *testing.T) {
		missingColDesc, err := parseTableDesc(`CREATE TABLE foo (a INT PRIMARY KEY)`)
		require.NoError(t, err)
		registered := wrappedEnvelope(t, missingColDesc, EnvelopeOpts{AfterField: true}).Schema()

		_, err = wrappedEnvelopeWithRegisteredSchema(
			t, narrowDesc, EnvelopeOpts{AfterField: true}, registered)
		requireRetryable(t, err)
		require.Contains(t, err.Error(), `field "b" of record "foo"`)
		require.Contains(t, err.Error(), "exists only in this changefeed's schema")
	})

	t.Run("encodes the registered default for an extra non-nullable field", func(t *testing.T) {
		local := wrappedEnvelope(t, narrowDesc, EnvelopeOpts{AfterField: true})
		registered := addRegisteredAfterField(t, local.Schema(),
			map[string]interface{}{"name": "c", "type": "string", "default": "hello"})
		adopted, err := wrappedEnvelopeWithRegisteredSchema(
			t, narrowDesc, EnvelopeOpts{AfterField: true}, registered)
		require.NoError(t, err)

		encoded, err := adopted.BinaryFromRow(nil, Metadata{}, nilRow, afterRow, nilRow, "")
		require.NoError(t, err)
		after := decodeWithRegistered(t, registered, encoded, "foo")
		require.Equal(t, map[string]interface{}{"string": "x"}, after["b"])
		require.Equal(t, "hello", after["c"])
	})

	t.Run("encodes the registered default for an extra defaulted union field", func(t *testing.T) {
		local := wrappedEnvelope(t, narrowDesc, EnvelopeOpts{AfterField: true})
		registered := addRegisteredAfterField(t, local.Schema(),
			map[string]interface{}{"name": "c", "type": []interface{}{"string", "null"}, "default": "world"})
		adopted, err := wrappedEnvelopeWithRegisteredSchema(
			t, narrowDesc, EnvelopeOpts{AfterField: true}, registered)
		require.NoError(t, err)

		encoded, err := adopted.BinaryFromRow(nil, Metadata{}, nilRow, afterRow, nilRow, "")
		require.NoError(t, err)
		after := decodeWithRegistered(t, registered, encoded, "foo")
		require.Equal(t, map[string]interface{}{"string": "world"}, after["c"])
	})

	t.Run("errors on an extra nullable field without a default", func(t *testing.T) {
		local := wrappedEnvelope(t, narrowDesc, EnvelopeOpts{AfterField: true})
		registered := addRegisteredAfterField(t, local.Schema(),
			map[string]interface{}{"name": "c", "type": []interface{}{"string", "null"}})
		_, err := wrappedEnvelopeWithRegisteredSchema(
			t, narrowDesc, EnvelopeOpts{AfterField: true}, registered)
		requireRetryable(t, err)
		require.Contains(t, err.Error(), "has no default value")
	})

	t.Run("errors on a shared column type mismatch", func(t *testing.T) {
		mismatchDesc, err := parseTableDesc(`CREATE TABLE foo (a INT PRIMARY KEY, b INT)`)
		require.NoError(t, err)
		registered := wrappedEnvelope(t, mismatchDesc, EnvelopeOpts{AfterField: true}).Schema()

		_, err = wrappedEnvelopeWithRegisteredSchema(
			t, narrowDesc, EnvelopeOpts{AfterField: true}, registered)
		requireRetryable(t, err)
		require.Contains(t, err.Error(), "does not match")
	})

	t.Run("errors on an extra envelope field", func(t *testing.T) {
		registered := wrappedEnvelope(t, narrowDesc,
			EnvelopeOpts{AfterField: true, UpdatedField: true}).Schema()

		_, err := wrappedEnvelopeWithRegisteredSchema(
			t, narrowDesc, EnvelopeOpts{AfterField: true}, registered)
		requireRetryable(t, err)
		require.Contains(t, err.Error(), `envelope field "updated"`)
	})

	t.Run("errors when registered is missing an envelope field", func(t *testing.T) {
		registered := wrappedEnvelope(t, narrowDesc, EnvelopeOpts{AfterField: true}).Schema()

		_, err := wrappedEnvelopeWithRegisteredSchema(
			t, narrowDesc, EnvelopeOpts{AfterField: true, UpdatedField: true}, registered)
		requireRetryable(t, err)
		require.Contains(t, err.Error(), `missing envelope field "updated"`)
	})

	t.Run("errors on a record name mismatch", func(t *testing.T) {
		renamedDesc, err := parseTableDesc(`CREATE TABLE bar (a INT PRIMARY KEY, b STRING)`)
		require.NoError(t, err)
		after, err := TableToAvroSchema(
			cdcevent.TestingMakeEventRow(renamedDesc, 0, nil, false), SchemaNoSuffix, "", "")
		require.NoError(t, err)
		// Same envelope name, different nested record name.
		registeredEnv, err := NewEnvelopeRecord("foo", EnvelopeOpts{AfterField: true}, nil, after, nil, nil, "")
		require.NoError(t, err)

		_, err = wrappedEnvelopeWithRegisteredSchema(
			t, narrowDesc, EnvelopeOpts{AfterField: true}, registeredEnv.Schema())
		requireRetryable(t, err)
		require.Contains(t, err.Error(), "does not match")
	})

	t.Run("errors on invalid registered avro", func(t *testing.T) {
		_, err := wrappedEnvelopeWithRegisteredSchema(
			t, narrowDesc, EnvelopeOpts{AfterField: true}, `{"not": "avro"}`)
		requireRetryable(t, err)
		require.Contains(t, err.Error(), "not valid avro")
	})

	t.Run("errors on valid avro that is not a record", func(t *testing.T) {
		_, err := wrappedEnvelopeWithRegisteredSchema(
			t, narrowDesc, EnvelopeOpts{AfterField: true},
			`{"type": "enum", "name": "e", "symbols": ["A"]}`)
		requireRetryable(t, err)
		require.Contains(t, err.Error(), "not an avro record")
	})

	t.Run("errors when a logical type annotation differs", func(t *testing.T) {
		tsDesc, err := parseTableDesc(`CREATE TABLE foo (a INT PRIMARY KEY, t TIMESTAMP)`)
		require.NoError(t, err)
		local := wrappedEnvelope(t, tsDesc, EnvelopeOpts{AfterField: true})
		// Identical except the timestamp annotation, which the structural walk
		// must reach inside the column's union.
		registered := strings.Replace(local.Schema(), `"timestamp-micros"`, `"timestamp-millis"`, 1)
		require.NotEqual(t, local.Schema(), registered)
		_, err = wrappedEnvelopeWithRegisteredSchema(
			t, tsDesc, EnvelopeOpts{AfterField: true}, registered)
		requireRetryable(t, err)
		require.Contains(t, err.Error(), `logicalType`)
		require.Contains(t, err.Error(), `timestamp-millis`)
	})

	t.Run("errors on a decimal scale mismatch", func(t *testing.T) {
		decDesc, err := parseTableDesc(`CREATE TABLE foo (a INT PRIMARY KEY, d DECIMAL(10,4))`)
		require.NoError(t, err)
		local := wrappedEnvelope(t, decDesc, EnvelopeOpts{AfterField: true})
		registered := strings.Replace(local.Schema(), `"scale":4`, `"scale":2`, 1)
		require.NotEqual(t, local.Schema(), registered)
		_, err = wrappedEnvelopeWithRegisteredSchema(
			t, decDesc, EnvelopeOpts{AfterField: true}, registered)
		requireRetryable(t, err)
		require.Contains(t, err.Error(), `"scale"`)
	})

	t.Run("errors on a decimal scale mismatch inside array items", func(t *testing.T) {
		decArrDesc, err := parseTableDesc(`CREATE TABLE foo (a INT PRIMARY KEY, d DECIMAL(10,4)[])`)
		require.NoError(t, err)
		local := wrappedEnvelope(t, decArrDesc, EnvelopeOpts{AfterField: true})
		// The annotations live inside the array's items, nested two unions
		// deep; the check must reach them there.
		registered := strings.Replace(local.Schema(), `"scale":4`, `"scale":2`, 1)
		require.NotEqual(t, local.Schema(), registered)
		_, err = wrappedEnvelopeWithRegisteredSchema(
			t, decArrDesc, EnvelopeOpts{AfterField: true}, registered)
		requireRetryable(t, err)
		require.Contains(t, err.Error(), `"scale"`)
		require.Contains(t, err.Error(), `items`)
	})

	t.Run("resolves nested record fields against the registered schema", func(t *testing.T) {
		// The compatibility rules rely on goavro applying record resolution
		// recursively: a codec built from the registered schema must encode
		// defaults for fields the native map does not supply, at any depth.
		registered := `{"type":"record","name":"env","fields":[
			{"name":"profile","type":{"type":"record","name":"profile_t","fields":[
				{"name":"first","type":"string"},
				{"name":"country","type":"string","default":"US"}]}}]}`
		codec, err := goavro.NewCodec(registered)
		require.NoError(t, err)
		// The nested native map lacks a field the schema defaults (country).
		encoded, err := codec.BinaryFromNative(nil, map[string]interface{}{
			"profile": map[string]interface{}{"first": "Alice"},
		})
		require.NoError(t, err)
		native, rest, err := codec.NativeFromBinary(encoded)
		require.NoError(t, err)
		require.Empty(t, rest)
		profile := native.(map[string]interface{})["profile"].(map[string]interface{})
		require.Equal(t, "Alice", profile["first"])
		require.Equal(t, "US", profile["country"])
	})

	t.Run("errors when a registered envelope field is not a record", func(t *testing.T) {
		local := wrappedEnvelope(t, narrowDesc, EnvelopeOpts{AfterField: true})
		var env map[string]interface{}
		require.NoError(t, json.Unmarshal([]byte(local.Schema()), &env))
		for _, f := range env["fields"].([]interface{}) {
			fm := f.(map[string]interface{})
			if fm["name"] == "after" {
				fm["type"] = []interface{}{"null", "string"}
			}
		}
		out, err := json.Marshal(env)
		require.NoError(t, err)
		_, err = wrappedEnvelopeWithRegisteredSchema(
			t, narrowDesc, EnvelopeOpts{AfterField: true}, string(out))
		requireRetryable(t, err)
		require.Contains(t, err.Error(), "does not match")
	})
}
