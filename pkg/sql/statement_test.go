// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/prep"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlcommenter"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestMakeStatement_sqlcommenter(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	expectedTags := []sqlcommenter.QueryTag{
		{Key: "action", Value: "/param*d"},
		{Key: "controller", Value: "index"},
		{Key: "framework", Value: "spring"},
		{Key: "traceparent", Value: "00-5bd66ef5095369c7b0d1f8f4bd33716a-c532cb4098ac3dd2-01"},
		{Key: "tracestate", Value: "congo=t61rcWkgMzE,rojo=00f067aa0ba902b7"},
	}

	testcases := []struct {
		name         string
		queries      []string
		expectedTags []sqlcommenter.QueryTag
	}{
		{
			name: "valid sqlcommenter comment",
			queries: []string{
				// Single block comment where the comment is in the correct
				// sqlcommenter format.
				`SELECT 1 /*action='%2Fparam*d',controller='index',framework='spring',
		traceparent='00-5bd66ef5095369c7b0d1f8f4bd33716a-c532cb4098ac3dd2-01',
		tracestate='congo%3Dt61rcWkgMzE%2Crojo%3D00f067aa0ba902b7'*/`,
				// Single in-line comment where the comment is in the correct
				// sqlcommenter format.
				`SELECT 1 -- action='%2Fparam*d',controller='index',framework='spring', traceparent='00-5bd66ef5095369c7b0d1f8f4bd33716a-c532cb4098ac3dd2-01',tracestate='congo%3Dt61rcWkgMzE%2Crojo%3D00f067aa0ba902b7'`,
				// Multiple comments where the last comment is in the correct
				// sqlcommenter format.
				`-- This is a comment
SELECT 1 /*action='%2Fparam*d',controller='index',framework='spring',
		traceparent='00-5bd66ef5095369c7b0d1f8f4bd33716a-c532cb4098ac3dd2-01',
		tracestate='congo%3Dt61rcWkgMzE%2Crojo%3D00f067aa0ba902b7'*/`,
				// Multiple comments where both comments are in the correct
				// sqlcommenter format. Only the last is considered.
				`SELECT 1
-- key='value', key2='value2'
/*action='%2Fparam*d',controller='index',framework='spring',
		traceparent='00-5bd66ef5095369c7b0d1f8f4bd33716a-c532cb4098ac3dd2-01',
		tracestate='congo%3Dt61rcWkgMzE%2Crojo%3D00f067aa0ba902b7'*/`,
			},
			expectedTags: expectedTags,
		},
		{
			name: "invalid sqlcommenter comment",
			queries: []string{
				// Invalid sqlcommenter comment, last value isn't wrapped in single quotes
				`SELECT 1 /*action='%2Fparam*d',controller='index',framework=spring*/`,
				// sqlcommenter comment is not the last comment.
				`SELECT 1 /*action='%2Fparam*d',controller='index'*/ /*Some other comment*/`,
			},
			expectedTags: nil,
		},
	}

	var p parser.Parser
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {

			testutils.RunTrueAndFalse(t, "withComments", func(t *testing.T, withComments bool) {
				testutils.RunTrueAndFalse(t, "preparedStatement", func(t *testing.T, preparedStatement bool) {
					for _, query := range tc.queries {
						options := parser.DefaultParseOptions
						if withComments {
							options = options.RetainComments()
						}
						stmts, err := p.ParseWithOptions(query, options)
						require.NoError(t, err)
						require.Len(t, stmts, 1)
						var stmt Statement
						if preparedStatement {
							ps := &prep.Statement{
								Metadata: prep.Metadata{
									Statement: stmts[0],
								},
							}
							stmt = makeStatementFromPrepared(ps, clusterunique.ID{})
						} else {
							stmt = makeStatement(stmts[0], clusterunique.ID{}, tree.FmtSimple)
						}
						if withComments {
							require.Equal(t, tc.expectedTags, stmt.QueryTags)
						} else {
							require.Empty(t, stmt.Comments)
						}
					}
				})
			})
		})
	}
}
