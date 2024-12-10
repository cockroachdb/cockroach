// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"context"
	gosql "database/sql"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/internal/sqlsmith"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/bootstrap"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/importer"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
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
	flags      = flag.NewFlagSet(os.Args[0], flag.ContinueOnError)
	expr       = flags.Bool("expr", false, "generate expressions instead of statements")
	udfs       = flags.Bool("udfs", false, "generate only CREATE FUNCTION statements")
	num        = flags.Int("num", 1, "number of statements / expressions to generate")
	url        = flags.String("url", "", "database to fetch schema from")
	execStmts  = flags.Bool("exec-stmts", false, "execute each generated statement against the db specified by url")
	schemaPath = flags.String("schema", "", "path containing schema definitions")
	prefix     = flags.String("prefix", "", "prefix each statement or expression")

	smitherOptMap = map[string]sqlsmith.SmitherOption{
		"AvoidConsts":                sqlsmith.AvoidConsts(),
		"CompareMode":                sqlsmith.CompareMode(),
		"DisableAggregateFuncs":      sqlsmith.DisableAggregateFuncs(),
		"DisableCRDBFns":             sqlsmith.DisableCRDBFns(),
		"DisableCrossJoins":          sqlsmith.DisableCrossJoins(),
		"DisableDDLs":                sqlsmith.DisableDDLs(),
		"DisableDecimals":            sqlsmith.DisableDecimals(),
		"DisableEverything":          sqlsmith.DisableEverything(),
		"DisableIndexHints":          sqlsmith.DisableIndexHints(),
		"DisableInsertSelect":        sqlsmith.DisableInsertSelect(),
		"DisableJoins":               sqlsmith.DisableJoins(),
		"DisableLimits":              sqlsmith.DisableLimits(),
		"DisableMutations":           sqlsmith.DisableMutations(),
		"DisableNondeterministicFns": sqlsmith.DisableNondeterministicFns(),
		"DisableWindowFuncs":         sqlsmith.DisableWindowFuncs(),
		"DisableWith":                sqlsmith.DisableWith(),
		"DisableUDFs":                sqlsmith.DisableUDFs(),
		"EnableAlters":               sqlsmith.EnableAlters(),
		"EnableLimits":               sqlsmith.EnableLimits(),
		"EnableWith":                 sqlsmith.EnableWith(),
		"FavorCommonData":            sqlsmith.FavorCommonData(),
		"IgnoreFNs":                  strArgOpt(sqlsmith.IgnoreFNs),
		"InsUpdOnly":                 sqlsmith.InsUpdOnly(),
		"MaybeSortOutput":            sqlsmith.MaybeSortOutput(),
		"MultiRegionDDLs":            sqlsmith.MultiRegionDDLs(),
		"MutatingMode":               sqlsmith.MutatingMode(),
		"MutationsOnly":              sqlsmith.MutationsOnly(),
		"OnlyNoDropDDLs":             sqlsmith.OnlyNoDropDDLs(),
		"OnlySingleDMLs":             sqlsmith.OnlySingleDMLs(),
		"OutputSort":                 sqlsmith.OutputSort(),
		"PostgresMode":               sqlsmith.PostgresMode(),
		"SimpleDatums":               sqlsmith.SimpleDatums(),
		"SimpleScalarTypes":          sqlsmith.SimpleScalarTypes(),
		"SimpleNames":                sqlsmith.SimpleNames(),
		"UnlikelyConstantPredicate":  sqlsmith.UnlikelyConstantPredicate(),
		"UnlikelyRandomNulls":        sqlsmith.UnlikelyRandomNulls(),

		"DisableNondeterministicLimits":           sqlsmith.DisableNondeterministicLimits(),
		"LowProbabilityWhereClauseWithJoinTables": sqlsmith.LowProbabilityWhereClauseWithJoinTables(),
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
		argKV := strings.SplitN(arg, "=", 2)
		if opt, ok := smitherOptMap[argKV[0]]; ok {
			fmt.Print("-- ", arg, ": ", opt, "\n")
			if len(argKV) == 2 {
				smitherOpts = append(smitherOpts, opt.(strArgOpt)(argKV[1]))
			} else {
				smitherOpts = append(smitherOpts, opt)
			}
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

	if *schemaPath != "" {
		opts, err := parseSchemaDefinition(*schemaPath)
		if err != nil {
			fmt.Fprintf(flags.Output(), "could not parse schema file %s: %s", *schemaPath, err)
			os.Exit(2)
		}
		smitherOpts = append(smitherOpts, opts...)
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
	sep := "\n"
	if *prefix != "" {
		sep = fmt.Sprintf("\n%s\n", *prefix)
	}

	if *expr {
		fmt.Println("-- expr")
		for i := 0; i < *num; i++ {
			fmt.Print(sep, smither.GenerateExpr(), "\n")
		}
	} else if *udfs {
		for i := 0; i < *num; i++ {
			fmt.Print(sep, smither.GenerateUDF(), ";\n")
		}
	} else {
		for i := 0; i < *num; i++ {
			stmt := smither.Generate()
			fmt.Print(sep, stmt, ";\n")
			if db != nil && *execStmts {
				_, _ = db.Exec(stmt)
			}
		}
	}
}

func parseSchemaDefinition(schemaPath string) (opts []sqlsmith.SmitherOption, _ error) {
	f, err := os.Open(schemaPath)
	if err != nil {
		return nil, errors.Wrapf(err, "could not open schema file %s for reading", schemaPath)
	}
	schema, err := io.ReadAll(f)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read schema definition data")
	}
	stmts, err := parser.Parse(string(schema))
	if err != nil {
		return nil, errors.Wrap(err, "could not parse schema definition")
	}
	semaCtx := tree.MakeSemaContext(nil /* resolver */)
	st := cluster.MakeTestingClusterSettings()
	wall := timeutil.Now().UnixNano()
	parentID := descpb.ID(bootstrap.TestingUserDescID(0))
	for i, s := range stmts {
		switch t := s.AST.(type) {
		default:
			return nil, errors.AssertionFailedf("only CreateTable statements supported, found %T", t)
		case *tree.CreateTable:
			tableID := descpb.ID(int(parentID) + i + 1)
			desc, err := importer.MakeTestingSimpleTableDescriptor(
				context.Background(), &semaCtx, st, t, parentID, keys.PublicSchemaID, tableID, importer.NoFKs, wall)
			if err != nil {
				return nil, errors.Wrapf(err, "failed to create table descriptor for statement %s", t)
			}
			opts = append(opts, sqlsmith.WithTableDescriptor(t.Table, desc.TableDescriptor))
		}
	}
	return opts, nil
}

type strArgOpt func(v string) sqlsmith.SmitherOption

func (o strArgOpt) Apply(s *sqlsmith.Smither) {}
func (o strArgOpt) String() string            { return "" }
