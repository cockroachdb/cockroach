// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package execinfrapb

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestChangeAggregatorSpecGetSchemaTS(t *testing.T) {
	defer leaktest.AfterTest(t)()

	statementTime := hlc.Timestamp{WallTime: 1}
	initialHighWater := hlc.Timestamp{WallTime: 2}
	schemaTS := hlc.Timestamp{WallTime: 3}
	empty := hlc.Timestamp{}

	tests := []struct {
		name     string
		spec     ChangeAggregatorSpec
		expected hlc.Timestamp
	}{
		{
			name: "schema_ts set, initial_high_water set",
			spec: ChangeAggregatorSpec{
				Feed:             jobspb.ChangefeedDetails{StatementTime: statementTime},
				InitialHighWater: &initialHighWater,
				SchemaTS:         &schemaTS,
			},
			expected: schemaTS,
		},
		{
			name: "schema_ts nil, initial_high_water set",
			spec: ChangeAggregatorSpec{
				Feed:             jobspb.ChangefeedDetails{StatementTime: statementTime},
				InitialHighWater: &initialHighWater,
			},
			expected: initialHighWater,
		},
		{
			name: "schema_ts empty, initial_high_water set",
			spec: ChangeAggregatorSpec{
				Feed:             jobspb.ChangefeedDetails{StatementTime: statementTime},
				InitialHighWater: &initialHighWater,
				SchemaTS:         &empty,
			},
			expected: initialHighWater,
		},
		{
			name: "schema_ts nil, initial_high_water nil",
			spec: ChangeAggregatorSpec{
				Feed: jobspb.ChangefeedDetails{StatementTime: statementTime},
			},
			expected: statementTime,
		},
		{
			name: "schema_ts nil, initial_high_water empty",
			spec: ChangeAggregatorSpec{
				Feed:             jobspb.ChangefeedDetails{StatementTime: statementTime},
				InitialHighWater: &empty,
			},
			expected: statementTime,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, tc.spec.GetSchemaTS())
		})
	}
}
