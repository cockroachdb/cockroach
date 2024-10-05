// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	gosql "database/sql"
	"flag"
	"fmt"
	"os"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/internal/sqlsmith"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/errors"
)

// smith is a command-line wrapper around sqlsmith-go, useful for examining
// sqlsmith output when making changes to the random query generator.

const description = `
%[1]s prints random SQL statements to stdout, using a random SQL statement
generator inspired by SQLsmith. See https://github.com/anse1/sqlsmith for more
about the SQLsmith project.

Statements span multiple lines, with blank lines between each statement. One way
to search whole multi-line statements with regular expressions is to use awk
with RS="", for example:

  smith -num 10000 | awk 'BEGIN {RS="";ORS="\n\n"} /regex/' | less

Usage:
  [COCKROACH_RANDOM_SEED=1234] %[1]s [options] [sqlsmith-go options]

Options:
`

var (
	flags         = flag.NewFlagSet(os.Args[0], flag.ContinueOnError)
	expr          = flags.Bool("expr", false, "generate expressions instead of statements")
	num           = flags.Int("num", 1, "number of statements / expressions to generate")
	url           = flags.String("url", "", "database to fetch schema from")
	smitherOptMap = map[string]sqlsmith.SmitherOption{
		"AvoidConsts":                             sqlsmith.AvoidConsts(),
		"CompareMode":                             sqlsmith.CompareMode(),
		"DisableAggregateFuncs":                   sqlsmith.DisableAggregateFuncs(),
		"DisableCRDBFns":                          sqlsmith.DisableCRDBFns(),
		"DisableCrossJoins":                       sqlsmith.DisableCrossJoins(),
		"DisableDDLs":                             sqlsmith.DisableDDLs(),
		"DisableDecimals":                         sqlsmith.DisableDecimals(),
		"DisableDivision":                         sqlsmith.DisableDivision(),
		"DisableEverything":                       sqlsmith.DisableEverything(),
		"DisableIndexHints":                       sqlsmith.DisableIndexHints(),
		"DisableInsertSelect":                     sqlsmith.DisableInsertSelect(),
		"DisableJoins":                            sqlsmith.DisableJoins(),
		"DisableLimits":                           sqlsmith.DisableLimits(),
		"DisableMutations":                        sqlsmith.DisableMutations(),
		"DisableNondeterministicFns":              sqlsmith.DisableNondeterministicFns(),
		"DisableNondeterministicLimits":           sqlsmith.DisableNondeterministicLimits(),
		"DisableWindowFuncs":                      sqlsmith.DisableWindowFuncs(),
		"DisableWith":                             sqlsmith.DisableWith(),
		"EnableAlters":                            sqlsmith.EnableAlters(),
		"FavorCommonData":                         sqlsmith.FavorCommonData(),
		"InsUpdOnly":                              sqlsmith.InsUpdOnly(),
		"LowProbabilityWhereClauseWithJoinTables": sqlsmith.LowProbabilityWhereClauseWithJoinTables(),
		"MultiRegionDDLs":                         sqlsmith.MultiRegionDDLs(),
		"MutatingMode":                            sqlsmith.MutatingMode(),
		"MutationsOnly":                           sqlsmith.MutationsOnly(),
		"OnlyNoDropDDLs":                          sqlsmith.OnlyNoDropDDLs(),
		"OnlySingleDMLs":                          sqlsmith.OnlySingleDMLs(),
		"OutputSort":                              sqlsmith.OutputSort(),
		"PostgresMode":                            sqlsmith.PostgresMode(),
		"SimpleDatums":                            sqlsmith.SimpleDatums(),
		"SimpleNames":                             sqlsmith.SimpleNames(),
		"UnlikelyConstantPredicate":               sqlsmith.UnlikelyConstantPredicate(),
		"UnlikelyRandomNulls":                     sqlsmith.UnlikelyRandomNulls(),
	}
	smitherOpts []string
)

func usage() {
	fmt.Fprintf(flags.Output(), description, os.Args[0])
	flags.PrintDefaults()
	fmt.Fprint(flags.Output(), "\nSqlsmith-go options:\n")
	for _, opt := range smitherOpts {
		fmt.Fprintln(flags.Output(), "  ", opt)
	}
}

func init() {
	smitherOpts = make([]string, 0, len(smitherOptMap))
	for opt := range smitherOptMap {
		smitherOpts = append(smitherOpts, opt)
	}
	sort.Strings(smitherOpts)
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

	// Gather our sqlsmith options from command-line arguments.
	var smitherOpts []sqlsmith.SmitherOption
	for _, arg := range flags.Args() {
		if opt, ok := smitherOptMap[arg]; ok {
			fmt.Print("-- ", arg, ": ", opt, "\n")
			smitherOpts = append(smitherOpts, opt)
		} else {
			fmt.Fprintf(flags.Output(), "unrecognized sqlsmith-go option: %v\n", arg)
			usage()
			os.Exit(2)
		}
	}

	// Connect to an external database for schema information.
	var db *gosql.DB
	if *url != "" {
		var err error
		db, err = gosql.Open("postgres", *url)
		if err != nil {
			fmt.Fprintf(flags.Output(), "could not connect to database\n")
			os.Exit(3)
		}
		defer db.Close()
		if err := db.Ping(); err != nil {
			fmt.Fprintf(flags.Output(), "could not ping database\n")
			os.Exit(4)
		}
		fmt.Println("-- connected to", *url)
	}

	// Create our smither.
	smither, err := sqlsmith.NewSmither(db, rng, smitherOpts...)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer smither.Close()

	// Finally, generate num statements (or expressions).
	fmt.Println("-- num", *num)
	if *expr {
		fmt.Println("-- expr")
		for i := 0; i < *num; i++ {
			fmt.Print("\n", smither.GenerateExpr(), "\n")
		}
	} else {
		for i := 0; i < *num; i++ {
			fmt.Print("\n", smither.Generate(), ";\n")
		}
	}
}
