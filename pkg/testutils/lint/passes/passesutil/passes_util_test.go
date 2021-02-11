// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package passesutil_test

import (
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils/lint/passes/forbiddenmethod"
	"github.com/cockroachdb/cockroach/pkg/testutils/lint/passes/unconvert"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/stretchr/testify/require"
	"golang.org/x/tools/go/analysis/analysistest"
)

// Use tests from other packages to also test this package. This ensures
// that if that code changes, somebody will look here. Also it allows for
// coverage checking here.

func requireNotEmpty(t *testing.T, path string) {
	t.Helper()
	files, err := filepath.Glob(path)
	require.NoError(t, err)
	require.NotEmpty(t, files)
}

func TestDescriptorMarshal(t *testing.T) {
	skip.UnderStress(t)
	testdata, err := filepath.Abs(filepath.Join("..", "forbiddenmethod", "testdata"))
	require.NoError(t, err)
	requireNotEmpty(t, testdata)

	analysistest.Run(t, testdata, forbiddenmethod.DescriptorMarshalAnalyzer, "descmarshaltest")
}

func TestUnconvert(t *testing.T) {
	skip.UnderStress(t)
	testdata, err := filepath.Abs(filepath.Join("..", "unconvert", "testdata"))
	require.NoError(t, err)
	requireNotEmpty(t, testdata)
	analysistest.Run(t, testdata, unconvert.Analyzer, "a")
}
