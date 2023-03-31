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
	"github.com/cockroachdb/cockroach/pkg/cmd/reduce/reduce"
	"github.com/cockroachdb/cockroach/pkg/cmd/reduce/reduce/reducesql"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/jackc/pgx/v4"
	"github.com/stretchr/testify/require"
)

var printUnknown = flag.Bool("unknown", false, "print unknown types during walk")

func TestReduceSQL(t *testing.T) {
	// These take a bit too long to need to run every time.
	skip.IgnoreLint(t, "unnecessary")
	reducesql.LogUnknown = *printUnknown

	isInterestingSQLWrapper := func(contains string) reduce.InterestingFn {
		return isInterestingSQL(t, contains)
	}

	reduce.Walk(t, "testdata", reducesql.Pretty, isInterestingSQLWrapper, reduce.ModeInteresting,
		nil /* chunkReducer */, reducesql.SQLPasses)
}

func isInterestingSQL(t *testing.T, contains string) reduce.InterestingFn {
	return func(ctx context.Context, f string) (bool, func()) {
		args := base.TestServerArgs{
			Insecure: true,
		}

		serv, err := serverutils.StartServerRaw(t, args)
		require.NoError(t, err)
		defer serv.Stopper().Stop(ctx)

		options := url.Values{}
		options.Add("sslmode", "disable")
		url := url.URL{
			Scheme:   "postgres",
			User:     url.User(username.RootUser),
			Host:     serv.ServingSQLAddr(),
			RawQuery: options.Encode(),
		}

		db, err := pgx.Connect(ctx, url.String())
		require.NoError(t, err)
		_, err = db.Exec(ctx, f)
		if err == nil {
			return false, nil
		}
		return strings.Contains(err.Error(), contains), nil
	}
}
