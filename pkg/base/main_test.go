// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package base_test

import (
	"github.com/cockroachdb/cockroach/pkg/security/securityassets"
	"github.com/cockroachdb/cockroach/pkg/security/securitytest"
	_ "github.com/cockroachdb/cockroach/pkg/util/log" // for flags
)

func init() {
	securityassets.SetLoader(securitytest.EmbeddedAssets)
}

//go:generate ../util/leaktest/add-leaktest.sh *_test.go
