// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package a

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func foo(t *testing.T) {
	defer leaktest.AfterTest(t) // want `leaktest.AfterTest return not called`
}
