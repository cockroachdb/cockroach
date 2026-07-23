// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package avro

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// afterEnv builds an envelope schema with a single row-record field "after"
// wrapping a record named recName whose fields are the given JSON snippet.
func afterEnv(recName, fields string) string {
	return `{"type":"record","name":"foo_envelope","fields":[` +
		`{"name":"after","type":["null",{"type":"record","name":"` + recName + `","fields":[` +
		fields + `]}]}]}`
}

// acmeEnv builds a namespaced wrapped envelope: record "ExampleEnvelope" in
// namespace "acme" with a single "after" field wrapping record "Example"
// (which inherits the "acme" namespace) whose fields are the given JSON
// snippet.
func acmeEnv(fields string) string {
	return `{"type":"record","name":"ExampleEnvelope","namespace":"acme","fields":[` +
		`{"name":"after","type":["null",{"type":"record","name":"Example","fields":[` +
		fields + `]}],"default":null}]}`
}

func TestCheckRegisteredEnvelopeCompat(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const fa = `{"name":"a","type":["null","long"]}`
	const fb = `{"name":"b","type":["null","string"]}`

	// A namespaced row record fieldA/fieldB, with the registered schema adding
	// a map-typed fieldC that the changefeed never supplies.
	const acmeLocal = `{"name":"fieldA","type":["null","string"],"default":null},` +
		`{"name":"fieldB","type":"int"}`
	const acmeRegistered = acmeLocal +
		`,{"name":"fieldC","type":["null",{"type":"map","values":"string"}],"default":null}`
	const acmeRegisteredNoDefault = acmeLocal +
		`,{"name":"fieldC","type":["null",{"type":"map","values":"string"}]}`

	tests := []struct {
		name       string
		local      string
		registered string
		expectErr  string
	}{
		// --- Basic passes ---
		{
			name:       "identical",
			local:      afterEnv("foo", fa+`,`+fb),
			registered: afterEnv("foo", fa+`,`+fb),
		},
		{
			name:       "shared field union branch reorder",
			local:      afterEnv("foo", `{"name":"a","type":["null","string"]}`),
			registered: afterEnv("foo", `{"name":"a","type":["string","null"]}`),
		},
		{
			name:       "registered union adds a branch the changefeed never emits",
			local:      afterEnv("foo", `{"name":"a","type":["null","long"]}`),
			registered: afterEnv("foo", `{"name":"a","type":["null","long","string"]}`),
		},
		{
			name:       "registered adopts a different field order",
			local:      afterEnv("foo", fa+`,`+fb),
			registered: afterEnv("foo", fb+`,`+fa),
		},
		{
			name:       "field attribute order does not matter",
			local:      afterEnv("foo", `{"name":"a","type":["null","long"]}`),
			registered: afterEnv("foo", `{"type":["null","long"],"name":"a"}`),
		},

		// --- Add / drop / default resolution ---
		{
			name:  "registered adds a nullable field with a null default",
			local: afterEnv("foo", fa),
			registered: afterEnv("foo",
				fa+`,`+`{"name":"c","type":["null","string"],"default":null}`),
		},
		{
			name:       "registered adds a non-nullable field with a default",
			local:      afterEnv("foo", fa),
			registered: afterEnv("foo", fa+`,`+`{"name":"c","type":"string","default":"x"}`),
		},
		{
			name:  "registered adds a defaulted union field with null last",
			local: afterEnv("foo", fa),
			registered: afterEnv("foo",
				fa+`,`+`{"name":"c","type":["string","null"],"default":"x"}`),
		},
		{
			name:       "registered adds a defaulted map field",
			local:      acmeEnv(acmeLocal),
			registered: acmeEnv(acmeRegistered),
		},
		{
			name:       "registered drops a field the changefeed produces",
			local:      afterEnv("foo", fa+`,`+fb),
			registered: afterEnv("foo", fa),
			expectErr:  `field "b" of record "foo" at "after" exists only in this changefeed's schema`,
		},
		{
			name:       "registered adds a map field without a default",
			local:      acmeEnv(acmeLocal),
			registered: acmeEnv(acmeRegisteredNoDefault),
			expectErr:  "has no default value",
		},
		{
			name:       "registered adds a nullable field without a default",
			local:      afterEnv("foo", fa),
			registered: afterEnv("foo", fa+`,`+`{"name":"c","type":["null","string"]}`),
			expectErr:  "has no default value",
		},
		{
			name: "nested record adds a field without a default",
			local: afterEnv("foo",
				`{"name":"profile","type":["null",{"type":"record","name":"profile_t","fields":[`+
					`{"name":"first","type":["null","string"]}]}]}`),
			registered: afterEnv("foo",
				`{"name":"profile","type":["null",{"type":"record","name":"profile_t","fields":[`+
					`{"name":"first","type":["null","string"]},`+
					`{"name":"country","type":"string"}]}]}`),
			expectErr: "has no default value",
		},
		{
			name: "nested record drops a field",
			local: afterEnv("foo",
				`{"name":"profile","type":["null",{"type":"record","name":"profile_t","fields":[`+
					`{"name":"first","type":["null","string"]},`+
					`{"name":"last","type":["null","string"]}]}]}`),
			registered: afterEnv("foo",
				`{"name":"profile","type":["null",{"type":"record","name":"profile_t","fields":[`+
					`{"name":"first","type":["null","string"]}]}]}`),
			expectErr: `field "last" of record "profile_t" at "after.profile"`,
		},

		// --- Named type references ---
		{
			name: "registered resolves a named type reference",
			local: afterEnv("foo",
				`{"name":"a","type":["null",{"type":"record","name":"amount","fields":[`+
					`{"name":"v","type":["null","long"]}]}]},`+
					`{"name":"b","type":["null","amount"]}`),
			registered: afterEnv("foo",
				`{"name":"a","type":["null",{"type":"record","name":"amount","fields":[`+
					`{"name":"v","type":["null","long"]}]}]},`+
					`{"name":"b","type":["null","amount"]},`+
					`{"name":"c","type":["null","string"],"default":null}`),
		},

		// --- Logical types ---
		{
			name: "logical type with an added defaulted field",
			local: afterEnv("foo",
				`{"name":"t","type":["null",{"type":"long","logicalType":"timestamp-micros"}]}`),
			registered: afterEnv("foo",
				`{"name":"t","type":["null",{"type":"long","logicalType":"timestamp-micros"}]},`+
					`{"name":"c","type":"string","default":"x"}`),
		},
		{
			name: "decimal scale absent equals scale zero",
			local: afterEnv("foo",
				`{"name":"d","type":["null",{"type":"bytes","logicalType":"decimal","precision":10,"scale":0}]}`),
			registered: afterEnv("foo",
				`{"name":"d","type":["null",{"type":"bytes","logicalType":"decimal","precision":10}]}`),
		},
		{
			name: "logical type mismatch",
			local: afterEnv("foo",
				`{"name":"t","type":["null",{"type":"long","logicalType":"timestamp-micros"}]}`),
			registered: afterEnv("foo",
				`{"name":"t","type":["null",{"type":"long","logicalType":"timestamp-millis"}]}`),
			expectErr: "logicalType",
		},
		{
			name: "decimal scale mismatch",
			local: afterEnv("foo",
				`{"name":"d","type":["null",{"type":"bytes","logicalType":"decimal","precision":10,"scale":4}]}`),
			registered: afterEnv("foo",
				`{"name":"d","type":["null",{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}]}`),
			expectErr: `"scale"`,
		},

		// --- Enums ---
		{
			name: "enum with a reader-only symbol",
			local: afterEnv("foo",
				`{"name":"e","type":["null",{"type":"enum","name":"color","symbols":["A","B"]}]}`),
			registered: afterEnv("foo",
				`{"name":"e","type":["null",{"type":"enum","name":"color","symbols":["A","B","C"]}]}`),
		},
		{
			name: "enum with a writer-only symbol",
			local: afterEnv("foo",
				`{"name":"e","type":["null",{"type":"enum","name":"color","symbols":["A","B","C"]}]}`),
			registered: afterEnv("foo",
				`{"name":"e","type":["null",{"type":"enum","name":"color","symbols":["A","B"]}]}`),
			expectErr: "missing symbol",
		},

		// --- Fixed ---
		{
			name: "fixed size matches",
			local: afterEnv("foo",
				`{"name":"h","type":["null",{"type":"fixed","name":"md5","size":16}]}`),
			registered: afterEnv("foo",
				`{"name":"h","type":["null",{"type":"fixed","name":"md5","size":16}]}`),
		},
		{
			name: "fixed size mismatch",
			local: afterEnv("foo",
				`{"name":"h","type":["null",{"type":"fixed","name":"md5","size":16}]}`),
			registered: afterEnv("foo",
				`{"name":"h","type":["null",{"type":"fixed","name":"md5","size":8}]}`),
			expectErr: "size",
		},

		// --- Arrays / maps ---
		{
			name: "map value type mismatch",
			local: afterEnv("foo",
				`{"name":"m","type":["null",{"type":"map","values":"string"}]}`),
			registered: afterEnv("foo",
				`{"name":"m","type":["null",{"type":"map","values":"long"}]}`),
			expectErr: "does not match",
		},

		// --- Type mismatches ---
		{
			name:       "shared field type mismatch",
			local:      afterEnv("foo", `{"name":"a","type":["null","long"]}`),
			registered: afterEnv("foo", `{"name":"a","type":["null","string"]}`),
			expectErr:  "does not match",
		},
		{
			name:       "no type promotion",
			local:      afterEnv("foo", `{"name":"a","type":["null","long"]}`),
			registered: afterEnv("foo", `{"name":"a","type":["null","double"]}`),
			expectErr:  "does not match",
		},
		{
			name:       "row record name mismatch",
			local:      afterEnv("foo", fa),
			registered: afterEnv("bar", fa),
			expectErr:  "does not match",
		},

		// --- Envelope field set (before / after / record) ---
		{
			name: "wrapped envelope with before and after",
			local: `{"type":"record","name":"foo_envelope","fields":[` +
				`{"name":"before","type":["null",{"type":"record","name":"foo_before","fields":[` + fa + `]}]},` +
				`{"name":"after","type":["null",{"type":"record","name":"foo","fields":[` + fa + `]}]}]}`,
			registered: `{"type":"record","name":"foo_envelope","fields":[` +
				`{"name":"before","type":["null",{"type":"record","name":"foo_before","fields":[` + fa + `]}]},` +
				`{"name":"after","type":["null",{"type":"record","name":"foo","fields":[` + fa + `]}]}]}`,
		},
		{
			name: "bare envelope with a record field",
			local: `{"type":"record","name":"foo_envelope","fields":[` +
				`{"name":"record","type":["null",{"type":"record","name":"foo","fields":[` + fa + `]}]}]}`,
			registered: `{"type":"record","name":"foo_envelope","fields":[` +
				`{"name":"record","type":["null",{"type":"record","name":"foo","fields":[` + fa + `]}]}]}`,
		},
		{
			name:  "envelope has an extra field",
			local: afterEnv("foo", fa),
			registered: `{"type":"record","name":"foo_envelope","fields":[` +
				`{"name":"after","type":["null",{"type":"record","name":"foo","fields":[` + fa + `]}]},` +
				`{"name":"updated","type":["null","string"]}]}`,
			expectErr: `envelope field "updated"`,
		},
		{
			name: "envelope is missing a field",
			local: `{"type":"record","name":"foo_envelope","fields":[` +
				`{"name":"after","type":["null",{"type":"record","name":"foo","fields":[` + fa + `]}]},` +
				`{"name":"updated","type":["null","string"]}]}`,
			registered: afterEnv("foo", fa),
			expectErr:  `missing envelope field "updated"`,
		},
		{
			name:       "envelope record name mismatch",
			local:      afterEnv("foo", fa),
			registered: `{"type":"record","name":"other_envelope","fields":[{"name":"after","type":["null",{"type":"record","name":"foo","fields":[` + fa + `]}]}]}`,
			expectErr:  "does not match",
		},

		// --- Registered schema validation ---
		{
			name:       "registered is not a record",
			local:      afterEnv("foo", fa),
			registered: `{"type":"enum","name":"e","symbols":["A"]}`,
			expectErr:  "not an avro record",
		},
		{
			name:       "registered is not valid avro",
			local:      afterEnv("foo", fa),
			registered: `{"not":"avro"}`,
			expectErr:  "not valid avro",
		},
		{
			name:  "registered references an unknown named type",
			local: afterEnv("foo", fa),
			registered: `{"type":"record","name":"foo_envelope","fields":[` +
				`{"name":"after","type":["null",{"type":"record","name":"foo","fields":[` +
				`{"name":"a","type":["null","missingType"]}]}]}]}`,
			expectErr: "not valid avro",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := CheckRegisteredEnvelopeCompat(tc.local, tc.registered)
			if tc.expectErr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.expectErr)
				return
			}
			require.NoError(t, err)
		})
	}
}
