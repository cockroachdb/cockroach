// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package artifacts

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseGSRef(t *testing.T) {
	bucket, objectName, err := parseGSRef("gs://bucket-name/artifacts/provisionings/abc/plan.json")
	require.NoError(t, err)
	require.Equal(t, "bucket-name", bucket)
	require.Equal(t, "artifacts/provisionings/abc/plan.json", objectName)
}

func TestParseGSRefRejectsInvalidRefs(t *testing.T) {
	_, _, err := parseGSRef("https://bucket/object")
	require.Error(t, err)

	_, _, err = parseGSRef("gs://bucket")
	require.Error(t, err)
}
