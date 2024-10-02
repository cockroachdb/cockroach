// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sqlccl

import (
	"context"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/sqlsmith"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// TestShowCreateRedactableValues tests that ShowCreateTable and ShowCreateView
// do not leak PII when called with RedactableValues set to true.
func TestShowCreateRedactableValues(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	rng, seed := randutil.NewTestRand()
	t.Log("seed:", seed)

	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	conn, err := sqlDB.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// To check for PII leaks, we inject a single unlikely string into some of the
	// query constants produced by SQLSmith, and then search the redacted SHOW
	// CREATE statement for this string.
	pii := "allosaurus"
	containsPII := func(create, createRedactable string) error {
		createRedacted := string(redact.RedactableString(createRedactable).Redact())
		lowerCreateRedacted := strings.ToLower(createRedacted)
		if strings.Contains(lowerCreateRedacted, pii) {
			return errors.Newf(
				"SHOW CREATE output contained PII (%q):\noriginal:\n%s\nredactable:\n%s\nredacted:\n%s\n",
				pii, create, createRedactable, createRedacted,
			)
		}
		return nil
	}

	// Check all redactable SHOW CREATE statements at once by using
	// crdb_internal.create_statements.
	checkAllShowCreateRedactable := func() {
		rows, err := conn.QueryContext(
			ctx, "SELECT create_statement, create_redactable FROM crdb_internal.create_statements",
		)
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			var create, createRedactable string
			if err := rows.Scan(&create, &createRedactable); err != nil {
				t.Fatal(err)
			}
			if err := containsPII(create, createRedactable); err != nil {
				t.Error(err)
				continue
			}
		}
	}

	// Perform a few random initial CREATE TABLEs and check for PII leaks.
	setup := sqlsmith.RandTablesPrefixStringConsts(rng, pii)
	setup = append(setup, "SET statement_timeout = '30s';")
	for _, stmt := range setup {
		t.Log(stmt)
		if _, err := conn.ExecContext(ctx, stmt); err != nil {
			// Ignore errors.
			t.Log("-- ignoring error:", err)
			continue
		}
	}
	checkAllShowCreateRedactable()

	// Perform a few random ALTERs (and additional CREATE TABLEs) and check for
	// PII leaks.
	alterSmith, err := sqlsmith.NewSmither(sqlDB, rng,
		sqlsmith.PrefixStringConsts(pii),
		sqlsmith.DisableEverything(),
		sqlsmith.EnableAlters(),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer alterSmith.Close()
	for i := 0; i < 5; i++ {
		alter := alterSmith.Generate()
		t.Log(alter)
		if _, err := conn.ExecContext(ctx, alter); err != nil {
			// Ignore errors.
			t.Log("-- ignoring error:", err)
			continue
		}
	}
	checkAllShowCreateRedactable()

	// Perform a few random CREATE VIEWs and check for PII leaks.
	smith, err := sqlsmith.NewSmither(sqlDB, rng,
		sqlsmith.PrefixStringConsts(pii),
		sqlsmith.DisableMutations(),
		sqlsmith.DisableWith(),
		sqlsmith.DisableUDFs(),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer smith.Close()
	for i := 0; i < 5; i++ {
		view := "CREATE VIEW v" + strconv.Itoa(i) + " AS " + smith.Generate()
		t.Log(view)
		if _, err := conn.ExecContext(ctx, view); err != nil {
			// Ignore errors.
			t.Log("-- ignoring error:", err)
			continue
		}
	}
	checkAllShowCreateRedactable()
}
