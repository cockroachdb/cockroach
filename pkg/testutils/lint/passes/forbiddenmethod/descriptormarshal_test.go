// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package forbiddenmethod_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/lint/passes/forbiddenmethod"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"golang.org/x/tools/go/analysis/analysistest"
)

func TestForbiddenMethod(t *testing.T) {
	skip.UnderStress(t)
	testdata := testutils.TestDataPath(t)
	analysistest.TestData = func() string { return testdata }
	analysistest.Run(t, testdata, forbiddenmethod.DescriptorMarshalAnalyzer, "descmarshaltest")
	analysistest.Run(t, testdata, forbiddenmethod.GRPCClientConnCloseAnalyzer, "grpcconnclosetest")
	analysistest.Run(t, testdata, forbiddenmethod.GRPCStatusWithDetailsAnalyzer, "grpcstatuswithdetailstest")
}
