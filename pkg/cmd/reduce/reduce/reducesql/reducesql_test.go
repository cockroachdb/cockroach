// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package reducesql_test

import (
	"context"
	"flag"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cmd/reduce/reduce"
	"github.com/cockroachdb/cockroach/pkg/cmd/reduce/reduce/reducesql"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
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

		serv := serverutils.StartServerOnly(t, args)
		defer serv.Stopper().Stop(ctx)

		db := serv.ApplicationLayer().SQLConn(t)

		_, err := db.Exec(f)
		if err == nil {
			return false, nil
		}
		return strings.Contains(err.Error(), contains), nil
	}
}
