// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package streamclient

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestParseConfigUri(t *testing.T) {
	defer leaktest.AfterTest(t)()
	type testCase struct {
		uri string
		err string
	}
	tests := []testCase{
		{uri: "external://foo", err: ""},
		{uri: "postgres://foo", err: ""},
		{uri: "postgresql://foo", err: ""},
		{uri: "randomgen://foo", err: ""},
		{uri: "ohhno://foo", err: "stream replication from scheme \"ohhno\" is unsupported"},
	}
	for _, test := range tests {
		uri, err := ParseConfigUri(test.uri)
		if test.err != "" {
			require.ErrorContains(t, err, test.err)
		} else {
			require.NoError(t, err)
			require.Equal(t, uri.Serialize(), test.uri)
		}
	}
}

func TestParseClusterUri(t *testing.T) {
	defer leaktest.AfterTest(t)()
	type testCase struct {
		uri string
		err string
	}
	tests := []testCase{
		{uri: "postgres://foo", err: ""},
		{uri: "postgresql://foo", err: ""},
		{uri: "postgresql://foo?crdb_route=node", err: ""},
		{uri: "postgresql://foo?crdb_route=gateway", err: ""},
		{uri: "postgresql://foo?crdb_route=ohhno", err: "unknown crdb_route value \"ohhno\""},
		{uri: "randomgen://foo", err: ""},
		{uri: "external://foo", err: "external uri \"external://foo\" must be resolved before constructing a cluster uri"},
		{uri: "ohhno://foo", err: "stream replication from scheme \"ohhno\" is unsupported"},
	}
	for _, test := range tests {
		uri, err := ParseClusterUri(test.uri)
		if test.err != "" {
			require.ErrorContains(t, err, test.err, "expected error parsing %s", test.uri)
		} else {
			require.NoError(t, err)
			require.Equal(t, uri.Serialize(), test.uri)
		}
	}
	// TODO(jeffswenson): add tests to ensure 'crdb_route' is parsed correctly.
}

func TestLookupClusterUri(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, sql, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	url, cleanup := s.PGUrl(t)
	defer cleanup()

	_, err := sql.Exec(fmt.Sprintf("CREATE EXTERNAL CONNECTION foobar AS '%s'", url.String()))
	require.NoError(t, err)

	clusterURI, err := LookupClusterUri(context.Background(), "external://foobar", s.InternalDB().(descs.DB))
	require.NoError(t, err)
	require.Equal(t, clusterURI.Serialize(), url.String())
}
