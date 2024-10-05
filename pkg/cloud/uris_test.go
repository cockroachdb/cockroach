// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cloud_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/stretchr/testify/require"
)

func TestSanitizeExternalStorageURI(t *testing.T) {
	// Register a scheme to test scheme-specific redaction.
	cloud.RegisterExternalStorageProvider(0, nil, nil,
		cloud.RedactedParams("TEST_PARAM"),
		"test-scheme",
	)
	testCases := []struct {
		name             string
		inputURI         string
		inputExtraParams []string
		expected         string
	}{
		{
			name:     "redacts password",
			inputURI: "http://username:password@foo.com/something",
			expected: "http://username:redacted@foo.com/something",
		},
		{
			name:             "redacts given parameters",
			inputURI:         "http://foo.com/something?secret_key=uhoh",
			inputExtraParams: []string{"secret_key"},
			expected:         "http://foo.com/something?secret_key=redacted",
		},
		{
			name:     "redacts registered parameters",
			inputURI: "test-scheme://somehost/somepath?TEST_PARAM=uhoh",
			expected: "test-scheme://somehost/somepath?TEST_PARAM=redacted",
		},
		{
			name:     "preserves username",
			inputURI: "http://username@foo.com/something",
			expected: "http://username@foo.com/something",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actualOutput, err := cloud.SanitizeExternalStorageURI(tc.inputURI, tc.inputExtraParams)
			require.NoError(t, err)
			require.Equal(t, tc.expected, actualOutput)
		})
	}
}
