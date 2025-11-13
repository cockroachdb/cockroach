// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCheckKeyForFamilySuffix(t *testing.T) {
	testCases := []struct {
		name       string
		prettyKey  string
		maxAllowed int
		wantError  bool
	}{
		{
			name:       "valid single-column PK boundary",
			prettyKey:  "…/2",
			maxAllowed: 1,
			wantError:  false,
		},
		{
			name:       "valid multi-column PK boundary",
			prettyKey:  "…/100/5",
			maxAllowed: 2,
			wantError:  false,
		},
		{
			name:       "mid-row split with family 0 suffix (1 extra segment)",
			prettyKey:  "…/2/0",
			maxAllowed: 1,
			wantError:  true,
		},
		{
			name:       "mid-row split with family N>0 suffix (2 extra segments)",
			prettyKey:  "…/2/1/1",
			maxAllowed: 1,
			wantError:  true,
		},
		{
			name:       "valid secondary index boundary (indexed col only)",
			prettyKey:  "…/3",
			maxAllowed: 3, // indexed + PK cols
			wantError:  false,
		},
		{
			name:       "secondary index with family suffix",
			prettyKey:  "…/3/100/5/0",
			maxAllowed: 3,
			wantError:  true,
		},
		{
			name:       "before boundary marker",
			prettyKey:  "<before:/Table/105/1/2>",
			maxAllowed: 1,
			wantError:  false, // Special markers are skipped
		},
		{
			name:       "after boundary marker",
			prettyKey:  "<after:/Table/105/1/2>",
			maxAllowed: 1,
			wantError:  false,
		},
		{
			name:       "TableMin boundary",
			prettyKey:  "…/<TableMin>",
			maxAllowed: 1,
			wantError:  false,
		},
		{
			name:       "IndexMax boundary",
			prettyKey:  "…/<IndexMax>",
			maxAllowed: 1,
			wantError:  false,
		},
		{
			name:       "empty key",
			prettyKey:  "",
			maxAllowed: 1,
			wantError:  false,
		},
		{
			name:       "just ellipsis",
			prettyKey:  "…",
			maxAllowed: 1,
			wantError:  false,
		},
		{
			name:       "fewer segments than expected (partial key)",
			prettyKey:  "…/2",
			maxAllowed: 3,
			wantError:  false, // Valid - could be index boundary
		},
		{
			name:       "three extra segments (unexpected)",
			prettyKey:  "…/2/0/1/2",
			maxAllowed: 1,
			wantError:  false, // Not 1 or 2 extra, so not a family suffix pattern
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			violation := checkKeyForFamilySuffix(tc.prettyKey, tc.maxAllowed)
			if tc.wantError {
				require.NotEmpty(t, violation, "expected to detect column family suffix")
			} else {
				require.Empty(t, violation, "should not detect column family suffix")
			}
		})
	}
}
