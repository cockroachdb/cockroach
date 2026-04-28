// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package metric

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCallerSourceFile(t *testing.T) {
	// callerSourceFile(0) should return this test file's repo-relative path.
	got := callerSourceFile(0)
	require.True(t, strings.HasSuffix(got, "pkg/util/metric/metadata_test.go"),
		"expected path ending in pkg/util/metric/metadata_test.go, got %q", got)
}

func TestCallerSourceFileFromMethod(t *testing.T) {
	// Verify that callerSourceFile works correctly when called from a
	// method receiver, which produces a fully-qualified function name
	// like "pkg.(*Type).Method" containing multiple dots.
	h := &metadataTestHelper{}
	got := h.sourceFile()
	require.True(t, strings.HasSuffix(got, "pkg/util/metric/metadata_test.go"),
		"expected path ending in pkg/util/metric/metadata_test.go, got %q", got)
}

type metadataTestHelper struct{}

func (h *metadataTestHelper) sourceFile() string {
	return callerSourceFile(0)
}

func TestCallerSourceFileFromClosure(t *testing.T) {
	// Closures produce function names like "pkg.TestFoo.func1".
	var got string
	func() {
		got = callerSourceFile(0)
	}()
	require.True(t, strings.HasSuffix(got, "pkg/util/metric/metadata_test.go"),
		"expected path ending in pkg/util/metric/metadata_test.go, got %q", got)
}

func TestNewMetadata(t *testing.T) {
	m := NewMetadata("test.metric", "A test metric.", "Tests", Unit_COUNT)
	require.Equal(t, "test.metric", m.Name)
	require.Equal(t, "A test metric.", m.Help)
	require.Equal(t, "Tests", m.Measurement)
	require.Equal(t, Unit_COUNT, m.Unit)
	require.True(t, strings.HasSuffix(m.SourceFile, "pkg/util/metric/metadata_test.go"),
		"expected SourceFile ending in pkg/util/metric/metadata_test.go, got %q", m.SourceFile)
}

func TestInitMetadata(t *testing.T) {
	m := InitMetadata(Metadata{
		Name:        "test.metric",
		Help:        "A test metric.",
		Measurement: "Tests",
		Unit:        Unit_COUNT,
	})
	require.Equal(t, "test.metric", m.Name)
	require.True(t, strings.HasSuffix(m.SourceFile, "pkg/util/metric/metadata_test.go"),
		"expected SourceFile ending in pkg/util/metric/metadata_test.go, got %q", m.SourceFile)
}
