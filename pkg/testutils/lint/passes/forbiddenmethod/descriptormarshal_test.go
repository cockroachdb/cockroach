// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package forbiddenmethod_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/lint/passes/forbiddenmethod"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"golang.org/x/tools/go/analysis/analysistest"
)

func TestForbiddenMethod(t *testing.T) {
	skip.UnderStress(t)
	testdata := datapathutils.TestDataPath(t)
	analysistest.TestData = func() string { return testdata }
	analysistest.Run(t, testdata, forbiddenmethod.DescriptorMarshalAnalyzer, "descmarshaltest")
	analysistest.Run(t, testdata, forbiddenmethod.GRPCClientConnCloseAnalyzer, "grpcconnclosetest")
	analysistest.Run(t, testdata, forbiddenmethod.GRPCStatusWithDetailsAnalyzer, "grpcstatuswithdetailstest")
}
