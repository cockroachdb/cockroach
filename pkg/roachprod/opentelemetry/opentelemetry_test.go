// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package opentelemetry

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMakeDatadogTags(t *testing.T) {
	tt := []struct {
		name         string
		givenTags    []string
		expectedTags map[string]string
	}{
		{
			name:         "no tags",
			givenTags:    []string{},
			expectedTags: map[string]string{},
		},
		{
			name:      "all key-value pairs",
			givenTags: []string{"env:testing", "host:cockroachdb"},
			expectedTags: map[string]string{
				"env":  "testing",
				"host": "cockroachdb",
			},
		},
		{
			name:      "mixed key and key-value pairs",
			givenTags: []string{"env:testing", "host:cockroachdb", "source"},
			expectedTags: map[string]string{
				"env":    "testing",
				"host":   "cockroachdb",
				"source": "",
			},
		},
		{
			name:      "all key pairs",
			givenTags: []string{"env", "host", "source"},
			expectedTags: map[string]string{
				"env":    "",
				"host":   "",
				"source": "",
			},
		},
		{
			name:      "special characters",
			givenTags: []string{"env:testing", "host:cockroachdb@localhost", "source:otelcol:testing"},
			expectedTags: map[string]string{
				"env":    "testing",
				"host":   "cockroachdb@localhost",
				"source": "otelcol:testing",
			},
		},
	}

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			actualTags := makeDatadogTags(tc.givenTags)
			assert.Equal(t, tc.expectedTags, actualTags)
		})
	}
}
