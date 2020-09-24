// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package logictestccl

import (
	"testing"

	_ "github.com/cockroachdb/cockroach/pkg/ccl"
	"github.com/cockroachdb/cockroach/pkg/sql/logictest"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestCCLLogic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	logictest.RunLogicTest(t, logictest.TestServerArgs{}, "testdata/logic_test/[^.]*")
}
