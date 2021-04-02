// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package reducesql_test

import (
	"context"
	"flag"
	"net/url"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/testutils/reduce"
	"github.com/cockroachdb/cockroach/pkg/testutils/reduce/reducesql"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/jackc/pgx"
)

var printUnknown = flag.Bool("unknown", false, "print unknown types during walk")

func TestReduceSQL(t *testing.T) {
	// These take a bit too long to need to run every time.
	skip.IgnoreLint(t, "unnecessary")
	reducesql.LogUnknown = *printUnknown

	reduce.Walk(t, "testdata", reducesql.Pretty, isInterestingSQL, reduce.ModeInteresting, reducesql.SQLPasses)
}

func isInterestingSQL(contains string) reduce.InterestingFn {
	return func(ctx context.Context, f reduce.File) bool {
		args := base.TestServerArgs{
			Insecure: true,
		}
		ts, err := server.TestServerFactory.New(args)
		if err != nil {
			panic(err)
		}
		serv := ts.(*server.TestServer)
		defer serv.Stopper().Stop(ctx)
		if err := serv.Start(context.Background()); err != nil {
			panic(err)
		}

		options := url.Values{}
		options.Add("sslmode", "disable")
		url := url.URL{
			Scheme:   "postgres",
			User:     url.User(security.RootUser),
			Host:     serv.ServingSQLAddr(),
			RawQuery: options.Encode(),
		}

		conf, err := pgx.ParseURI(url.String())
		if err != nil {
			panic(err)
		}
		db, err := pgx.Connect(conf)
		if err != nil {
			panic(err)
		}
		_, err = db.ExecEx(ctx, string(f), nil)
		if err == nil {
			return false
		}
		return strings.Contains(err.Error(), contains)
	}
}
