// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package a

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func foo(t *testing.T) {
	defer leaktest.AfterTest(t) // want `leaktest.AfterTest return not called`
}
