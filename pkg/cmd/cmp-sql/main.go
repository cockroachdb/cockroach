// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// cmp-sql connects to postgres and cockroach servers and compares the
// results of SQL statements. Statements support both random generation
// and random placeholder values. It can thus be used to do correctness or
// compatibility testing.
//
// To use, start a cockroach and postgres server with SSL disabled. cmp-sql
// will connect to both, generate some random SQL, and print an error when
// difference results are returned. Currently it tests LIKE, binary operators,
// and unary operators. cmp-sql runs a loop: 1) choose an Input, 2) generate a
// random SQL string from from the Input, 3) generate random placeholders, 4)
// execute the SQL + placeholders and compare results.
//
// The Inputs slice determines what SQL is generated. cmp-sql will repeatedly
// generate new kinds of input SQL. The `sql` property of an Input is a
// function that returns a SQL string with possible placeholders. The `args`
// func slice generates the placeholder arguments.
package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"regexp"
	"strings"

	"github.com/cockroachdb/apd/v2"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/jackc/pgx"
	"github.com/jackc/pgx/pgtype"
)

var (
	pgAddr = flag.String("pg", "localhost:5432", "postgres address")
	pgUser = flag.String("pg-user", "postgres", "postgres user")
	crAddr = flag.String("cr", "localhost:26257", "cockroach address")
	crUser = flag.String("cr-user", "root", "cockroach user")
	rng, _ = randutil.NewPseudoRand()
)

func main() {
	flag.Parse()
	ctx := context.Background()

	// Save the confs so we can print out the used ports later.
	var confs []pgx.ConnConfig
	for addr, user := range map[string]string{
		*pgAddr: *pgUser,
		*crAddr: *crUser,
	} {
		conf, err := pgx.ParseURI(fmt.Sprintf("postgresql://%s@%s?sslmode=disable", user, addr))
		if err != nil {
			panic(err)
		}
		confs = append(confs, conf)
	}
	dbs := make([]*pgx.Conn, len(confs))
	for i, conf := range confs {
		db, err := pgx.Connect(conf)
		if err != nil {
			panic(err)
		}
		dbs[i] = db
	}

	// Record unique errors and mismatches and only show them once.
	seen := make(map[string]bool)
	sawErr := func(err error) string {
		s := reduceErr(err)
		// Ignore error strings after the first semicolon.
		res := fmt.Sprintf("ERR: %s", s)
		if !seen[res] {
			fmt.Print(err, "\n\n")
			seen[res] = true
		}
		return res
	}
	for {
	Loop:
		for _, input := range Inputs {
			results := map[int]string{}
			sql, args, repro := input.Generate()
			for i, db := range dbs {
				var res, full string
				if rows, err := db.Query(sql, args...); err != nil {
					res = sawErr(err)
					full = err.Error()
				} else {
					if rows.Next() {
						vals, err := rows.Values()
						if err != nil {
							panic(err)
						} else if len(vals) != 1 {
							panic(fmt.Errorf("expected 1 val, got %v", vals))
						} else {
							switch v := vals[0].(type) {
							case *pgtype.Numeric:
								b, err := v.EncodeText(nil, nil)
								if err != nil {
									panic(err)
								}
								// Use a decimal so we can Reduce away the extra zeros. Needed because
								// pg and cr return equivalent but not identical decimal results.
								var d apd.Decimal
								if _, _, err := d.SetString(string(b)); err != nil {
									panic(err)
								}
								d.Reduce(&d)
								res = d.String()
							default:
								res = fmt.Sprint(v)
							}
							full = res
						}
					}
					rows.Close()
					if err := rows.Err(); err != nil {
						res = sawErr(err)
						full = err.Error()
					}
					if res == "" {
						panic("empty")
					}
				}
				// Ping to see if the previous query panic'd the server.
				if err := db.Ping(ctx); err != nil {
					fmt.Print("CRASHER:\n", repro)
					panic(fmt.Errorf("%v is down", confs[i].Port))
				}
				// Check the current result against all previous results. Make sure they are the same.
				for vi, v := range results {
					if verr, reserr := strings.HasPrefix(v, "ERR"), strings.HasPrefix(res, "ERR"); verr && reserr {
						continue
					} else if input.ignoreIfEitherError && (verr || reserr) {
						continue
					}
					if v != res {
						mismatch := fmt.Sprintf("%v: got %s\n%v: saw %s\n",
							confs[i].Port,
							full,
							confs[vi].Port,
							v,
						)
						if !seen[mismatch] {
							seen[mismatch] = true
							fmt.Print("MISMATCH:\n", mismatch)
							fmt.Println(repro)
						}
						continue Loop
					}
				}
				results[i] = res
			}
		}
	}
}

var reduceErrRE = regexp.MustCompile(` *(ERROR)?[ :]*([A-Za-z ]+?) +`)

// reduceErr removes any "ERROR:" prefix and returns the first words of an
// error message. This is usually enough to uniquely identify it and remove
// any non-unique (i.e., random string or numeric) values.
func reduceErr(err error) string {
	match := reduceErrRE.FindStringSubmatch(err.Error())
	if match == nil {
		return err.Error()
	}
	return match[2]
}

// Input defines an SQL statement generator.
type Input struct {
	sql  func() string
	args []func() interface{}
	// ignoreIfEitherError, if true, will only do mismatch comparison if both
	// crdb and pg return non-error results.
	ignoreIfEitherError bool
}

// Generate returns an instance of input's SQL and arguments, as well as a
// repro string that can be copy-pasted into a SQL console.
func (i Input) Generate() (sql string, args []interface{}, repro string) {
	sql = i.sql()
	args = make([]interface{}, len(i.args))
	for i, fn := range i.args {
		args[i] = fn()
	}
	var b strings.Builder
	fmt.Fprintf(&b, "PREPARE a AS %s;\n", sql)
	b.WriteString("EXECUTE a (")
	for i, fn := range i.args {
		if i > 0 {
			b.WriteString(", ")
		}
		arg := fn()
		switch arg := arg.(type) {
		case int, int64, float64:
			fmt.Fprint(&b, arg)
		case string:
			s := fmt.Sprintf("%q", arg)
			fmt.Fprintf(&b, "e'%s'", s[1:len(s)-1])
		default:
			panic(fmt.Errorf("unknown type: %T", arg))
		}
	}
	b.WriteString(");\n")
	return sql, args, b.String()
}

// Inputs is the collection of generators that are compared.
var Inputs = []Input{
	{
		sql:  pass("SELECT $1 LIKE $2"),
		args: twoLike,
	},
	{
		sql:  pass("SELECT $1 LIKE $2 ESCAPE $3"),
		args: threeLike,
	},
	{
		sql: fromSlices(
			"SELECT $1::%s %s $2::%s %s $3::%s",
			numTyps,
			binaryNumOps,
			numTyps,
			binaryNumOps,
			numTyps,
		),
		args:                threeNum,
		ignoreIfEitherError: true,
	},
	{
		sql: fromSlices(
			"SELECT %s($1::%s)",
			unaryNumOps,
			numTyps,
		),
		args: oneNum,
	},
}

var (
	twoLike   = []func() interface{}{likeArg(5), likeArg(5)}
	threeLike = []func() interface{}{likeArg(5), likeArg(5), likeArg(3)}
	oneNum    = []func() interface{}{num}
	threeNum  = []func() interface{}{num, num, num}

	binaryNumOps = []string{
		"-",
		"+",
		"^",
		"*",
		"/",
		"//",
		"%",
		"<<",
		">>",
		"&",
		"#",
		"|",
	}
	unaryNumOps = []string{
		"-",
		"~",
	}
	numTyps = []string{
		"int8",
		"float8",
		"decimal",
	}
)

func pass(s string) func() string {
	return func() string {
		return s
	}
}

// fromSlice generates arguments for and executes fmt.Sprintf by randomly
// selecting elements of args.
func fromSlices(s string, args ...[]string) func() string {
	return func() string {
		gen := make([]interface{}, len(args))
		for i, arg := range args {
			gen[i] = arg[rand.Intn(len(arg))]
		}
		return fmt.Sprintf(s, gen...)
	}
}

// num generates a random number (int or float64).
func num() interface{} {
	switch rand.Intn(6) {
	case 1:
		return 1
	case 2:
		return 2
	case 3:
		return -1
	case 4:
		return rand.Int() / (rand.Intn(10) + 1)
	case 5:
		return rand.NormFloat64()
	default:
		return 0
	}
}

func likeArg(n int) func() interface{} {
	return func() interface{} {
		p := make([]byte, rng.Intn(n))
		for i := range p {
			switch rand.Intn(4) {
			case 0:
				p[i] = '_'
			case 1:
				p[i] = '%'
			default:
				p[i] = byte(1 + rng.Intn(127))
			}
		}
		return string(p)
	}
}
