// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
	return ng.GenerateOne(42)
}
