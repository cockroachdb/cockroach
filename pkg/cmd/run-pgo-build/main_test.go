// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"bytes"
	"os"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/google/pprof/profile"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestProcessProfiles feeds three copies of in.pb.gz to processProfiles and
// checks that the output matches `out.txt`.
func TestProcessProfiles(t *testing.T) {
	defer leaktest.AfterTest(t)()

	defer func(loc *time.Location) {
		time.Local = loc
	}(time.Local)
	time.Local = time.UTC

	// To regenerate the fixtures, set this to true.
	const rewrite = false

	ch := make(chan *profile.Profile, 10)
	b, err := os.ReadFile(datapathutils.TestDataPath(t, "in.pb.gz"))
	require.NoError(t, err)
	inProf, err := profile.ParseData(b)
	require.NoError(t, err)
	ch <- inProf
	ch <- inProf
	ch <- inProf
	close(ch)

	actProf, err := processProfiles(ch)
	require.NoError(t, err)
	var actBytes bytes.Buffer
	require.NoError(t, actProf.Write(&actBytes))

	if rewrite {
		require.NoError(t, os.WriteFile(datapathutils.TestDataPath(t, "out.pb.gz"), actBytes.Bytes(), 0644))
		require.NoError(t, os.WriteFile(datapathutils.TestDataPath(t, "out.txt"), []byte(actProf.String()), 0644))
		require.NoError(t, os.WriteFile(datapathutils.TestDataPath(t, "in.txt"), []byte(inProf.String()), 0644))
	}

	expText, err := os.ReadFile(datapathutils.TestDataPath(t, "out.txt"))
	require.NoError(t, err)
	actText := actProf.String()

	assert.Equal(t, string(expText), actText)

	if rewrite {
		t.Fatal("rewrote test files; please change `rewrite` back to false")
	}
}
