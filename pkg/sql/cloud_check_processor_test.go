// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestResolveExternalDestination verifies how a URI is classified as an implicit
// or explicit-auth destination, which is what gates EXTERNALIOIMPLICITACCESS.
func TestResolveExternalDestination(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	for _, tc := range []struct {
		name string
		uri  string
		kms  bool
		// expectExplicitAuth is true when the destination does not require
		// EXTERNALIOIMPLICITACCESS.
		expectExplicitAuth bool
		expectKind         string
	}{
		{name: "aws-kms no auth", uri: "aws-kms:///key?REGION=r", kms: true, expectExplicitAuth: true, expectKind: "aws-kms"},
		{name: "aws-kms specified", uri: "aws-kms:///key?REGION=r&AUTH=specified", kms: true, expectExplicitAuth: true, expectKind: "aws-kms"},
		{name: "aws-kms implicit", uri: "aws-kms:///key?REGION=r&AUTH=implicit", kms: true, expectExplicitAuth: false, expectKind: "aws-kms"},
		{name: "gcp-kms no auth", uri: "gcp-kms:///key", kms: true, expectExplicitAuth: true, expectKind: "gcp-kms"},
		{name: "gcp-kms specified", uri: "gcp-kms:///key?AUTH=specified", kms: true, expectExplicitAuth: true, expectKind: "gcp-kms"},
		{name: "gcp-kms implicit", uri: "gcp-kms:///key?AUTH=implicit", kms: true, expectExplicitAuth: false, expectKind: "gcp-kms"},
		{name: "azure-kms no auth", uri: "azure-kms:///key?AZURE_TENANT_ID=t", kms: true, expectExplicitAuth: true, expectKind: "azure-kms"},
		{name: "azure-kms specified", uri: "azure-kms:///key?AZURE_TENANT_ID=t&AUTH=specified", kms: true, expectExplicitAuth: true, expectKind: "azure-kms"},
		{name: "azure-kms implicit", uri: "azure-kms:///key?AUTH=implicit", kms: true, expectExplicitAuth: false, expectKind: "azure-kms"},

		// The aws and gs schemes are both external storage and kms
		{name: "aws (kms alias) no auth", uri: "aws:///key?REGION=r", kms: true, expectExplicitAuth: true, expectKind: "aws"},
		{name: "gs (kms alias) implicit", uri: "gs:///key?AUTH=implicit", kms: true, expectExplicitAuth: false, expectKind: "gs"},

		{name: "external kms connection", uri: "external://kmskey", kms: true, expectExplicitAuth: true, expectKind: "external"},

		{name: "s3 specified", uri: "s3://bucket/key?AUTH=specified&AWS_ACCESS_KEY_ID=k&AWS_SECRET_ACCESS_KEY=s", kms: false, expectExplicitAuth: true, expectKind: "s3"},
		{name: "s3 implicit", uri: "s3://bucket/key?AUTH=implicit", kms: false, expectExplicitAuth: false, expectKind: "s3"},
		{name: "s3 custom endpoint", uri: "s3://bucket/key?AUTH=specified&AWS_ACCESS_KEY_ID=k&AWS_SECRET_ACCESS_KEY=s&AWS_ENDPOINT=http://evil", kms: false, expectExplicitAuth: false, expectKind: "s3"},
		{name: "nodelocal", uri: "nodelocal://1/path", kms: false, expectExplicitAuth: false, expectKind: "nodelocal"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dest, err := resolveExternalDestinationAsImplicitAuth(tc.uri, username.RootUserName(), tc.kms)
			require.NoError(t, err)
			require.Equal(t, tc.expectExplicitAuth, dest.AccessIsWithExplicitAuth())
			require.Equal(t, tc.expectKind, dest.Kind())
		})
	}
}
