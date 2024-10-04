// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package reducesql_test

import (
	"context"
	"flag"
	"net/url"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cmd/reduce/reduce"
	"github.com/cockroachdb/cockroach/pkg/cmd/reduce/reduce/reducesql"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/jackc/pgx/v4"
)

var printUnknown = flag.Bool("unknown", false, "print unknown types during walk")

func TestReduceSQL(t *testing.T) {
	// These take a bit too long to need to run every time.
	skip.IgnoreLint(t, "unnecessary")
	reducesql.LogUnknown = *printUnknown

	reduce.Walk(t, "testdata", reducesql.Pretty, isInterestingSQL, reduce.ModeInteresting,
		nil /* chunkReducer */, reducesql.SQLPasses)
}

func isInterestingSQL(contains string) reduce.InterestingFn {
	return func(ctx context.Context, f string) (bool, func()) {
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
			User:     url.User(username.RootUser),
			Host:     serv.ServingSQLAddr(),
			RawQuery: options.Encode(),
		}

		db, err := pgx.Connect(ctx, url.String())
		if err != nil {
			panic(err)
		}
		_, err = db.Exec(ctx, f)
		if err == nil {
			return false, nil
		}
		return strings.Contains(err.Error(), contains), nil
	}
}
