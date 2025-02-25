// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sniffarg

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDo(t *testing.T) {
	defer leaktest.AfterTest(t)()

	args := []string{
		"-test.benchmem=5",
		"-test.outputdir", "banana",
		"something",
		"--somethingelse", "foo",
		"--boolflag",
	}
	var benchMem string
	var outputDir string
	var somethingElse string
	notFound := "hello"
	require.NoError(t, Do(args, "test.benchmem", &benchMem))
	require.NoError(t, Do(args, "test.outputdir", &outputDir))
	require.NoError(t, Do(args, "somethingelse", &somethingElse))
	require.NoError(t, Do(args, "notfound", &notFound))
	assert.Equal(t, "5", benchMem)
	assert.Equal(t, "banana", outputDir)
	assert.Equal(t, "foo", somethingElse)
	assert.Zero(t, notFound)
}
