// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestMakeChangefeedDescription(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()

	for _, tc := range []struct {
		createStmt string
		sinkURI    string
		opts       map[string]string
		expected   string
	}{
		{
			createStmt: `CREATE CHANGEFEED FOR t`,
			expected:   `EXPERIMENTAL CHANGEFEED FOR TABLE t`,
		},
		{
			createStmt: `CREATE CHANGEFEED FOR t WITH diff`,
			opts:       map[string]string{"diff": ""},
			expected:   `EXPERIMENTAL CHANGEFEED FOR TABLE t WITH OPTIONS (diff)`,
		},
		{
			createStmt: `CREATE CHANGEFEED FOR t WITH diff, client_cert='a', client_key='b'`,
			opts:       map[string]string{"diff": "", "client_cert": "a", "client_key": "b"},
			expected:   `EXPERIMENTAL CHANGEFEED FOR TABLE t WITH OPTIONS (client_cert = 'a', client_key = 'redacted', diff)`,
		},
		{
			createStmt: `CREATE CHANGEFEED FOR t INTO 'null://'`,
			sinkURI:    `null://`,
			expected:   `CREATE CHANGEFEED FOR TABLE t INTO 'null:'`,
		},
		{
			createStmt: `CREATE CHANGEFEED FOR t INTO 'null://' WITH diff`,
			sinkURI:    `null://`,
			opts:       map[string]string{"diff": ""},
			expected:   `CREATE CHANGEFEED FOR TABLE t INTO 'null:' WITH OPTIONS (diff)`,
		},
		{
			createStmt: `CREATE CHANGEFEED FOR t INTO 'null://' WITH diff, client_cert='a', client_key='b'`,
			sinkURI:    `null://`,
			opts:       map[string]string{"diff": "", "client_cert": "a", "client_key": "b"},
			expected:   `CREATE CHANGEFEED FOR TABLE t INTO 'null:' WITH OPTIONS (client_cert = 'a', client_key = 'redacted', diff)`,
		},
		{
			createStmt: `CREATE CHANGEFEED FOR t INTO 'kafka://fake.kafka'`,
			sinkURI:    `kafka://fake.kafka`,
			expected:   `CREATE CHANGEFEED FOR TABLE t INTO 'kafka://fake.kafka'`,
		},
		{
			createStmt: `CREATE CHANGEFEED FOR t INTO 'kafka://fake.kafka' WITH diff`,
			sinkURI:    `kafka://fake.kafka`,
			opts:       map[string]string{"diff": ""},
			expected:   `CREATE CHANGEFEED FOR TABLE t INTO 'kafka://fake.kafka' WITH OPTIONS (diff)`,
		},
		{
			createStmt: `CREATE CHANGEFEED FOR t INTO 'kafka://fake.kafka' WITH diff, client_cert='a', client_key='b'`,
			sinkURI:    `kafka://fake.kafka`,
			opts:       map[string]string{"diff": "", "client_cert": "a", "client_key": "b"},
			expected:   `CREATE CHANGEFEED FOR TABLE t INTO 'kafka://fake.kafka' WITH OPTIONS (client_cert = 'a', client_key = 'redacted', diff)`,
		},
	} {
		parsed, err := parser.ParseOne(tc.createStmt)
		require.NoError(t, err)
		create := parsed.AST.(*tree.CreateChangefeed)

		opts := changefeedbase.MakeStatementOptions(tc.opts)

		desc, err := makeChangefeedDescription(ctx, create, tc.sinkURI, opts)
		require.NoError(t, err)
		require.Equal(t, tc.expected, desc)
	}
}
