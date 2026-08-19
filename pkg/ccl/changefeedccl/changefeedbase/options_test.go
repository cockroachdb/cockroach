// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedbase

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestOptionsValidations(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	require.NoError(t, MakeDefaultOptions().ValidateForCreateChangefeed(false),
		"Default options should be valid")
	require.NoError(t, MakeDefaultOptions().ValidateForCreateChangefeed(true),
		"Default options should be valid")

	tests := []struct {
		input     map[string]string
		isPred    bool
		expectErr string
	}{
		{map[string]string{"format": "txt"}, false, "unknown format"},
		{map[string]string{"initial_scan": "", "no_initial_scan": ""}, false, "cannot specify both"},
		{map[string]string{"format": "txt"}, true, "unknown format"},
		{map[string]string{"initial_scan": "", "no_initial_scan": ""}, true, "cannot specify both"},
		{map[string]string{"format": "parquet", "topic_in_value": ""}, false, "cannot specify both"},
		// Verify that the returned error uses the syntax initial_scan='yes' instead of initial_scan_only. See #97008.
		{map[string]string{"initial_scan_only": "", "resolved": ""}, true, "cannot specify both initial_scan='only'"},
		{map[string]string{"initial_scan_only": "", "resolved": ""}, true, "cannot specify both initial_scan='only'"},
		{map[string]string{"key_column": "b"}, false, "requires the unordered option"},
		{map[string]string{"format": "avro", "avro_mode": "read", "resolved": ""}, false,
			"resolved is not supported with avro_mode=read"},
		{map[string]string{"format": "avro", "avro_mode": "read", "resolved": "10s"}, false,
			"resolved is not supported with avro_mode=read"},
		{map[string]string{"format": "avro", "avro_mode": "write", "resolved": "10s"}, false, ""},
		{map[string]string{"format": "avro", "avro_mode": "read"}, false, ""},
		{map[string]string{"avro_mode": "read", "resolved": "10s"}, false, ""},
	}

	for _, test := range tests {
		o := MakeStatementOptions(test.input)
		err := o.ValidateForCreateChangefeed(test.isPred)
		if test.expectErr == "" {
			require.NoError(t, err)
		} else {
			require.Error(t, err, fmt.Sprintf("%v should not be valid", test.input))
			require.Contains(t, err.Error(), test.expectErr)
		}
	}
}

func TestEncodingOptionsValidations(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	cases := []struct {
		opts      EncodingOptions
		expectErr string
	}{
		{EncodingOptions{Envelope: OptEnvelopeRow, Format: OptFormatAvro}, "envelope=row is not supported with format=avro"},
		{EncodingOptions{Format: OptFormatAvro, EncodeJSONValueNullAsObject: true}, "is only usable with format=json"},
		{EncodingOptions{Format: OptFormatAvro, Envelope: OptEnvelopeBare, KeyInValue: true}, "is only usable with envelope=wrapped"},
		{EncodingOptions{Format: OptFormatAvro, Envelope: OptEnvelopeBare, TopicInValue: true}, "is only usable with envelope=wrapped"},
		{EncodingOptions{Format: OptFormatAvro, Envelope: OptEnvelopeBare, UpdatedTimestamps: true}, "is only usable with envelope=wrapped"},
		{EncodingOptions{Format: OptFormatAvro, Envelope: OptEnvelopeBare, MVCCTimestamps: true}, "is only usable with envelope=wrapped"},
		{EncodingOptions{Format: OptFormatAvro, Envelope: OptEnvelopeBare, Diff: true}, "is only usable with envelope=wrapped"},
		{EncodingOptions{Format: OptFormatAvro, AvroMode: OptAvroModeRead}, ""},
		{EncodingOptions{Format: OptFormatJSON, AvroMode: OptAvroModeRead}, "avro_mode is only usable with format=avro"},
		{EncodingOptions{Format: OptFormatAvro, AvroMode: OptAvroModeWrite}, ""},
		{EncodingOptions{Format: OptFormatJSON, AvroMode: OptAvroModeWrite}, "avro_mode is only usable with format=avro"},
		{EncodingOptions{Format: OptFormatJSON}, ""},
	}

	for _, c := range cases {
		err := c.opts.Validate()
		if c.expectErr == "" {
			require.NoError(t, err)
		} else {
			require.Error(t, err)
			require.Contains(t, err.Error(), c.expectErr)
		}
	}

}

func TestAvroSchemasOption(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	cases := []struct {
		name      string
		opts      map[string]string
		expectErr string
	}{
		{name: "rename and strategies in read mode",
			opts: map[string]string{"format": "avro", "avro_mode": "read",
				"avro_schemas": `{"foo-value": {"version": "latest_compatible"}, "foo-key": {"version": "pin(3)", "subject_name": "acme.foo.key"}}`}},
		{name: "key version without value entry in read mode",
			opts: map[string]string{"format": "avro", "avro_mode": "read",
				"avro_schemas": `{"foo-key": {"version": "pin(3)", "subject_name": "acme.foo.key"}}`}},
		{name: "version is not allowed in write mode",
			opts:      map[string]string{"format": "avro", "avro_schemas": `{"foo-value": {"version": "latest"}}`},
			expectErr: "version is only usable with avro_mode=read"},
		{name: "invalid JSON",
			opts:      map[string]string{"format": "avro", "avro_schemas": `{`},
			expectErr: "parsing avro_schemas"},
		{name: "not an object",
			opts:      map[string]string{"format": "avro", "avro_schemas": `[]`},
			expectErr: "must be a JSON object"},
		{name: "key without a subject suffix",
			opts:      map[string]string{"format": "avro", "avro_schemas": `{"foo": {}}`},
			expectErr: `ending in "-key" or "-value"`},
		{name: "entry not an object",
			opts:      map[string]string{"format": "avro", "avro_schemas": `{"foo-value": "acme.foo"}`},
			expectErr: `entry "foo-value" must be a JSON object`},
		{name: "unknown entry field",
			opts:      map[string]string{"format": "avro", "avro_schemas": `{"foo-value": {"subject": "x"}}`},
			expectErr: "unknown field"},
		{name: "empty subject_name",
			opts:      map[string]string{"format": "avro", "avro_schemas": `{"foo-value": {"subject_name": ""}}`},
			expectErr: "empty subject_name"},
		{name: "unknown version strategy",
			opts:      map[string]string{"format": "avro", "avro_mode": "read", "avro_schemas": `{"foo-value": {"version": "newest"}}`},
			expectErr: "invalid avro version strategy"},
		{name: "pin without a version",
			opts:      map[string]string{"format": "avro", "avro_mode": "read", "avro_schemas": `{"foo-value": {"version": "pin()"}}`},
			expectErr: "invalid pinned version"},
		{name: "pin with a non-numeric version",
			opts:      map[string]string{"format": "avro", "avro_mode": "read", "avro_schemas": `{"foo-value": {"version": "pin(abc)"}}`},
			expectErr: "invalid pinned version"},
		{name: "pin zero",
			opts:      map[string]string{"format": "avro", "avro_mode": "read", "avro_schemas": `{"foo-value": {"version": "pin(0)"}}`},
			expectErr: "invalid pinned version"},
		{name: "empty key entry",
			opts:      map[string]string{"format": "avro", "avro_schemas": `{"foo-key": {}}`},
			expectErr: `entry "foo-key" must specify at least one of "subject_name" or "version"`},
		{name: "empty value entry",
			opts:      map[string]string{"format": "avro", "avro_schemas": `{"foo-value": {}}`},
			expectErr: `entry "foo-value" must specify at least one of "subject_name" or "version"`},
		{name: "requires avro format",
			opts:      map[string]string{"avro_schemas": `{"foo-key": {"subject_name": "acme.foo.key"}}`},
			expectErr: "avro_schemas is only usable with format=avro"},
		{name: "empty object allowed with avro format",
			opts: map[string]string{"format": "avro", "avro_schemas": `{}`}},
		{name: "pin requires read mode",
			opts:      map[string]string{"format": "avro", "avro_schemas": `{"foo-value": {"version": "pin(2)"}}`},
			expectErr: "only usable with avro_mode=read"},
		{name: "latest_compatible requires read mode",
			opts:      map[string]string{"format": "avro", "avro_schemas": `{"foo-value": {"version": "latest_compatible"}}`},
			expectErr: "only usable with avro_mode=read"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := MakeStatementOptions(tc.opts).GetEncodingOptions()
			if tc.expectErr == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tc.expectErr)
			}
		})
	}
}
