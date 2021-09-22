package errwrap_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/build/bazel"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/lint/passes/errwrap"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"golang.org/x/tools/go/analysis/analysistest"
)

func init() {
	if bazel.BuiltWithBazel() {
		bazel.SetGoEnv()
	}
}

func Test(t *testing.T) {
	skip.UnderStress(t, "Go cache files don't work under stress")
	testdata := testutils.TestDataPath(t)
	analysistest.TestData = func() string { return testdata }
	analysistest.Run(t, testdata, errwrap.Analyzer, "err")
}
