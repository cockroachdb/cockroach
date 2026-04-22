// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package embedding

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseModelSpec(t *testing.T) {
	tests := []struct {
		name             string
		spec             string
		expectedProvider string
		expectedModel    string
	}{
		{
			name:             "local model",
			spec:             "all-MiniLM-L6-v2",
			expectedProvider: "",
			expectedModel:    "all-MiniLM-L6-v2",
		},
		{
			name:             "openai model",
			spec:             "openai/text-embedding-3-small",
			expectedProvider: "openai",
			expectedModel:    "text-embedding-3-small",
		},
		{
			name:             "provider with multiple slashes",
			spec:             "provider/model/variant",
			expectedProvider: "provider",
			expectedModel:    "model/variant",
		},
		{
			name:             "empty string",
			spec:             "",
			expectedProvider: "",
			expectedModel:    "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			provider, model := ParseModelSpec(tc.spec)
			require.Equal(t, tc.expectedProvider, provider)
			require.Equal(t, tc.expectedModel, model)
		})
	}
}

func TestLookupModel(t *testing.T) {
	tests := []struct {
		name         string
		spec         string
		expectedDims int
		expectedErr  string
	}{
		{
			name:         "local model",
			spec:         "all-MiniLM-L6-v2",
			expectedDims: 384,
		},
		{
			name:         "openai small",
			spec:         "openai/text-embedding-3-small",
			expectedDims: 1536,
		},
		{
			name:         "openai large",
			spec:         "openai/text-embedding-3-large",
			expectedDims: 3072,
		},
		{
			name:         "openai ada",
			spec:         "openai/text-embedding-ada-002",
			expectedDims: 1536,
		},
		{
			name:        "unknown model",
			spec:        "unknown/model",
			expectedErr: "unknown embedding model",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			info, err := LookupModel(tc.spec)
			if tc.expectedErr != "" {
				require.ErrorContains(t, err, tc.expectedErr)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expectedDims, info.Dims)
			}
		})
	}
}
