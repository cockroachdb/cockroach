// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package leaktest

import "testing"

func AfterTest(testing.TB) func() { return func() {} }
