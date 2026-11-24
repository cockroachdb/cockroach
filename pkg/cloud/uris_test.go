// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cloud_test

import (
	"context"
	"net/url"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cloud/cloudpb"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"github.com/stretchr/testify/require"
)

func TestSanitizeExternalStorageURI(t *testing.T) {
	// Register a scheme to test scheme-specific redaction.
	cloud.RegisterExternalStorageProvider(0, cloud.RegisteredProvider{
		ParseFn: func(cloud.ExternalStorageURIContext, *url.URL) (cloudpb.ExternalStorage, error) {
			return cloudpb.ExternalStorage{}, errors.Newf("unimplemented")
		},
		ConstructFn: func(context.Context, cloud.ExternalStorageContext, cloudpb.ExternalStorage) (cloud.ExternalStorage, error) {
			return nil, errors.Newf("unimplemented")
		},
		RedactedParams: cloud.RedactedParams("TEST_PARAM"),
		Schemes:        []string{"test-scheme"},
	})
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

func TestMakeRedactableLocatorURI(t *testing.T) {
	unsafe := func(s string) string {
		return string(redact.Sprintf("%s", s))
	}
	redactedMarker := string(redact.RedactableString(redact.RedactedMarker()))

	for _, tc := range []struct {
		name     string
		inputURI string
		marked   string
		redacted string
	}{
		{
			name:     "no-query-params",
			inputURI: "nodelocal://1/backup",
			marked:   "nodelocal://1/backup",
			redacted: "nodelocal://1/backup",
		},
		{
			name:     "single-query-param",
			inputURI: "nodelocal://1/backup?AUTH=specified",
			marked:   "nodelocal://1/backup?" + unsafe("AUTH=specified"),
			redacted: "nodelocal://1/backup?" + redactedMarker,
		},
		{
			name:     "multiple-query-params",
			inputURI: "nodelocal://1/backup?COCKROACH_LOCALITY=east-1&SUPER_SECRET_PASSWORD=password",
			marked:   "nodelocal://1/backup?" + unsafe("COCKROACH_LOCALITY=east-1&SUPER_SECRET_PASSWORD=password"),
			redacted: "nodelocal://1/backup?" + redactedMarker,
		},
		{
			name:     "s3-uri-with-credentials",
			inputURI: "s3://my-bucket/path?AWS_ACCESS_KEY_ID=AKID&AWS_SECRET_ACCESS_KEY=secret",
			marked:   "s3://my-bucket/path?" + unsafe("AWS_ACCESS_KEY_ID=AKID&AWS_SECRET_ACCESS_KEY=secret"),
			redacted: "s3://my-bucket/path?" + redactedMarker,
		},
		{
			// url.Parse fails on this URI, so MakeRedactableLocatorURI falls back to treating the
			// entire string as unsafe.
			name:     "unparseable-uri-fully-redacted",
			inputURI: "%%invalid?SECRET=dont-leak-this",
			marked:   unsafe("%%invalid?SECRET=dont-leak-this"),
			redacted: redactedMarker,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			actualMarked := cloud.MakeRedactableLocatorURI(tc.inputURI)
			require.Equal(t, tc.marked, string(actualMarked))
			require.Equal(t, tc.redacted, string(actualMarked.Redact()))
			require.Equal(t, tc.inputURI, actualMarked.StripMarkers())
		})
	}
}
