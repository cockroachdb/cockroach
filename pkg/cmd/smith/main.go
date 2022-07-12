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
	"flag"
	"fmt"
	"os"

	"github.com/cockroachdb/cockroach/pkg/internal/sqlsmith"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/errors"
)

// smith is a command-line wrapper around sqlsmith-go, useful for examining
// sqlsmith output when making changes to the random query generator.

const description = `%[1]s prints randomly-generated statements to stdout.

Usage:
  %[1]s | less
  %[1]s | head -n100
  %[1]s | awk 'BEGIN {RS=""} /CREATE TABLE/ {print $0 "\n"}' | less
  %[1]s -num 20
  COCKROACH_RANDOM_SEED=3931604535443227367 %[1]s | less

Options:
`

var (
	flags = flag.NewFlagSet(os.Args[0], flag.ContinueOnError)
	num   = flags.Int("num", 0, "number of statements to generate, 0 for infinity (default)")
)

func usage() {
	fmt.Fprintf(flags.Output(), description, os.Args[0])
	flags.PrintDefaults()
}

func init() {
	flags.Usage = usage
}

func main() {
	if err := flags.Parse(os.Args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			os.Exit(0)
		} else {
			os.Exit(1)
		}
	}
	rng, seed := randutil.NewPseudoRand()
	fmt.Print("-- COCKROACH_RANDOM_SEED=", seed, "\n")

	// TODO(michae2): Set smither options from command-line options.
	smithOpts := []sqlsmith.SmitherOption{}
	smither, err := sqlsmith.NewSmither(nil, rng, smithOpts...)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer smither.Close()

	for i := 0; *num == 0 || i < *num; i++ {
		fmt.Print("\n", smither.Generate(), ";\n")
	}
}
