// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"fmt"
	"os"

	"github.com/cockroachdb/cockroach/pkg/internal/sqlsmith"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

func main() {
	rng, seed := randutil.NewTestRand()
	fmt.Print("-- seed: ", seed, "\n")

	// TODO(michae2): Set smither options from command-line options.
	smithOpts := []sqlsmith.SmitherOption{}
	smither, err := sqlsmith.NewSmither(nil, rng, smithOpts...)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer smither.Close()

	for {
		fmt.Print("\n", smither.Generate(), ";\n")
	}
}
