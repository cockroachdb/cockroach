// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tenantcostmodel_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcostmodel"
)

func TestMockBatchRequest(t *testing.T) {
	test := func(isWrite bool, writeBytes int64) {
		t.Helper()
		ba := tenantcostmodel.MockBatchRequest(isWrite, writeBytes)
		info := tenantcostmodel.MakeRequestInfo(ba)
		exp := tenantcostmodel.TestingRequestInfo(isWrite, writeBytes)
		if info != exp {
			t.Errorf("got %v, expected %v\n", info, exp)
		}
	}
	test(true, 1234)
	test(false, 0)
}

func TestMockBatchResponse(t *testing.T) {
	test := func(readBytes int64) {
		t.Helper()
		br := tenantcostmodel.MockBatchResponse(readBytes)
		info := tenantcostmodel.MakeResponseInfo(br)
		exp := tenantcostmodel.TestingResponseInfo(readBytes)
		if info != exp {
			t.Errorf("got %v, expected %v\n", info, exp)
		}
	}
	test(0)
	test(1234)
}
