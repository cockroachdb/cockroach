// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package application_api_test

import (
	"github.com/cockroachdb/cockroach/pkg/util/randident"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

func generateRandomName() string {
	rand, _ := randutil.NewTestRand()
	cfg := randident.DefaultNameGeneratorConfig()
	// REST api can not handle `/`. This is fixed in
	// the UI by using sql-over-http endpoint instead.
	cfg.Punctuate = -1
	cfg.Finalize()

	ng := randident.NewNameGenerator(
		&cfg,
		rand,
		"a b%s-c.d",
	)
	return ng.GenerateOne("42")
}
