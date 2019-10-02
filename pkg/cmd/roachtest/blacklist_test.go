// Copyright 2019 The Cockroach Authors.
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
	"context"
	"fmt"
	"sort"
	"strconv"
	"testing"

	"github.com/google/go-github/github"
)

var issueTitleMap = map[int]string{
	243:   "roadmap: Blob storage",
	3781:  "sql: Add Data Type Formatting Functions",
	4035:  "sql/pgwire: missing support for row count limits in pgwire",
	5807:  "sql: Add support for TEMP tables",
	6130:  "sql: add support for key watches with notifications of changes",
	6583:  "sql: explicit lock syntax (SELECT FOR {SHARE,UPDATE} {skip locked,nowait})",
	9682:  "sql: implement computed indexes",
	10028: "sql: Support view queries with star expansions",
	10735: "sql: support SQL savepoints",
	12123: "sql: Can't drop and replace a table within a transaction",
	17511: "sql: support stored procedures",
	18846: "sql: Support CIDR column type",
	21085: "sql: WITH RECURSIVE (recursive common table expressions)",
	21286: "sql: Add support for geometric types",
	22329: "Support XA distributed transactions in CockroachDB",
	23299: "sql: support coercing string literals to arrays",
	23468: "sql: support sql arrays of JSONB",
	24062: "sql: 32 bit SERIAL type",
	24897: "sql: CREATE OR REPLACE VIEW",
	26097: "sql: make TIMETZ more pg-compatible",
	26443: "sql: support user-defined schemas between database and table",
	26508: "sql: restricted DDL / DML inside transactions",
	26725: "sql: support postgres' API to handle blob storage (incl lo_creat, lo_from_bytea)",
	26732: "sql: support the binary operator: <int> / <float>",
	26925: "sql: make the CockroachDB integer types more compatible with postgres",
	27791: "sql: support RANGE types",
	27793: "sql: support custom/user-defined base scalar (primitive) types",
	27796: "sql: support user-defined DOMAIN types",
	30352: "roadmap:when CockroachDB  will support cursor?",
	31632: "sql: FK options (deferrable, etc)",
	31708: "sql: support current_time",
	32552: "multi-dim arrays",
	32562: "sql: support SET LOCAL and txn-scoped session variable changes",
	32565: "sql: support optional TIME precision",
	32610: "sql: can't insert self reference",
	35807: "sql: INTERVAL output doesn't match PG",
	35879: "sql: `default_transaction_read_only` should also accept 'on' and 'off'",
	35882: "sql: support other character sets",
	35897: "sql: unknown function: pg_terminate_backend()",
	35902: "sql: large object support",
	36115: "sql: psychopg: investigate if datetimetz is being returned instead of datetime",
	36116: "sql: psychopg: investigate how `'infinity'::timestamp` is presented",
	36118: "sql: Cannot parse '24:00' as type time",
	36179: "sql: implicitly convert date to timestamp",
	36215: "sql: enable setting standard_conforming_strings to off",
	40195: "pgwire: multiple active result sets (portals) not supported",
	40205: "sql: add non-trivial implementations of FOR UPDATE, FOR NO KEY UPDATE, FOR SHARE, FOR NO KEY SHARE",
	40474: "sql: support `SELECT ... FOR UPDATE OF` syntax",
	40476: "sql: support `FOR {UPDATE,SHARE} {SKIP LOCKED,NOWAIT}`",
	40854: "sql: set application_name from connection string",
}

func TestBlacklists(t *testing.T) {
	blacklists := []blacklist{
		hibernateBlackList19_2, pgjdbcBlackList19_2, psycopgBlackList19_2,
	}
	var failureMap = make(map[string]int)
	for _, bl := range blacklists {
		for _, reason := range bl {
			failureMap[reason]++
		}
	}

	type reasonCount struct {
		reason string
		count  int
	}

	counts := make([]reasonCount, 0, len(failureMap))

	for reason, count := range failureMap {
		counts = append(counts, reasonCount{reason: reason, count: count})
	}

	ctx := context.Background()
	// If you run out of non-authed requests, set ts to your personal access token.
	// ts := oauth2.StaticTokenSource(
	// &oauth2.Token{AccessToken: "github personal access token"},
	// )
	// tc := oauth2.NewClient(ctx, ts)
	// client := github.NewClient(tc)
	client := github.NewClient(nil)

	sort.Slice(counts, func(i, j int) bool {
		return counts[i].count > counts[j].count
	})
	var editedIssueTitleMap bool
	for i := range counts {
		issueTitle := "unknown"
		reason := counts[i].reason
		var issueNum int
		var err error
		if issueNum, err = strconv.Atoi(counts[i].reason); err == nil {
			if issueTitleMap[issueNum] == "" {
				// Found an issue without a title in issueTitleMap. Fetch it from GitHub.
				t.Log("Fetching", issueNum)
				if issue, _, err := client.Issues.Get(ctx, "cockroachdb", "cockroach", issueNum); err == nil {
					editedIssueTitleMap = true
					issueTitleMap[issueNum] = issue.GetTitle()
				}
			}
			reason = fmt.Sprintf("https://github.com/cockroachdb/cockroach/issues/%d", issueNum)
			issueTitle = issueTitleMap[issueNum]
		}
		fmt.Printf("%4d: %-54s (%s)\n", counts[i].count, reason, issueTitle)
	}

	if editedIssueTitleMap {
		// Print out a new issueTitleMap so the user can edit the one in this file.
		issueNums := make([]int, 0, len(issueTitleMap))
		for k := range issueTitleMap {
			issueNums = append(issueNums, k)
		}
		sort.Ints(issueNums)
		fmt.Println("var issueTitleMap = map[int]string{")
		for _, n := range issueNums {
			fmt.Printf("%7d: %q,\n", n, issueTitleMap[n])
		}
		fmt.Println("}")
	}
}
