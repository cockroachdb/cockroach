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
