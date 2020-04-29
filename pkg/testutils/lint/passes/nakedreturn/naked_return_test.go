package nakedreturn_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/lint/passes/nakedreturn"
	"golang.org/x/tools/go/analysis/analysistest"
)

func Test(t *testing.T) {
	if testutils.NightlyStress() {
		t.Skip("Go cache files don't work under stress")
	}
	testdata := analysistest.TestData()
	analysistest.Run(t, testdata, nakedreturn.Analyzer, "a")
}
