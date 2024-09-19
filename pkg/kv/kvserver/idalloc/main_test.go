// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package idalloc_test

import (
	"github.com/cockroachdb/cockroach/pkg/security/securityassets"
	"github.com/cockroachdb/cockroach/pkg/security/securitytest"
)

//go:generate ../../../util/leaktest/add-leaktest.sh *_test.go

func init() {
	securityassets.SetLoader(securitytest.EmbeddedAssets)
}
