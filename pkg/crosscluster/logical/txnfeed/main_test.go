// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnfeed

import _ "github.com/cockroachdb/cockroach/pkg/util/log" // for flags

//go:generate ../../../util/leaktest/add-leaktest.sh *_test.go
