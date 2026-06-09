// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package schemachanger_test

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/stretchr/testify/require"
)

// TestConcurrentDMLDuringExpressionIndexBackfill is a regression test for a bug
// where concurrent INSERT/UPSERT/UPDATE on a table failed with "Access to
// crdb_internal and system is restricted" while an expression index was being
// created on it.
//
// Creating an expression index synthesizes a virtual computed column, for which
// the declarative schema changer adds a transient validation CHECK constraint
// that calls crdb_internal.assignment_cast(<expr>, NULL::<type>). That check is
// enforced for the duration of the backfill, so every concurrent mutation
// rebuilds it. Because assignment_cast resolves to the crdb_internal schema, the
// unsafe-internals gate rejected the mutation until assignment_cast was added to
// the allowlist of system-injected builtins.
//
// The test enforces the unsafe-internals gate (test servers bypass it by
// default), confirms the gate is active with a positive control, then injects
// DML at every schema-change stage and asserts that none of it is rejected.
func TestConcurrentDMLDuringExpressionIndexBackfill(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderDuress(t, "spins up a server and drives a backfill with timing-sensitive hooks")

	ctx := context.Background()
	const restricted = "Access to crdb_internal and system is restricted"

	type failure struct{ stage, op, err string }
	var (
		sqlDB      *sqlutils.SQLRunner
		setupDone  atomic.Bool
		hookFired  atomic.Bool
		failuresMu syncutil.Mutex
		failures   []failure
	)

	s := serverutils.StartServerOnly(t, base.TestServerArgs{
		// Connect to the system tenant directly so the UnsafeOverride knob below
		// governs enforcement.
		DefaultTestTenant: base.TestControlsTenantsExplicitly,
		Knobs: base.TestingKnobs{
			// Test servers default UnsafeOverride to always-allow, which disables
			// the unsafe-internals gate. Returning nil restores real enforcement
			// so this test actually exercises it.
			SQLEvalContext: &eval.TestingKnobs{
				UnsafeOverride: func() *bool { return nil },
			},
			SQLDeclarativeSchemaChanger: &scexec.TestingKnobs{
				BeforeStage: func(p scplan.Plan, stageIdx int) error {
					if !setupDone.Load() {
						return nil
					}
					if len(p.TargetState.Statements) == 0 ||
						!strings.Contains(p.TargetState.Statements[0].Statement, "CREATE INDEX") {
						return nil
					}
					hookFired.Store(true)
					st := p.Stages[stageIdx]
					stageDesc := fmt.Sprintf("%s:%d", st.Phase, st.Ordinal)
					record := func(op string, err error) {
						if err != nil {
							failuresMu.Lock()
							defer failuresMu.Unlock()
							failures = append(failures, failure{stageDesc, op, err.Error()})
						}
					}
					// Mimic an external client (e.g. the MOLT replicator) writing
					// to the table during the backfill.
					_, err := sqlDB.DB.ExecContext(ctx,
						`UPSERT INTO jobs (id, params) VALUES (100, '{"hash": "x"}')`)
					record("upsert", err)
					_, err = sqlDB.DB.ExecContext(ctx,
						`UPDATE jobs SET params = params WHERE id = 1`)
					record("update", err)
					return nil
				},
			},
		},
	})
	defer s.Stopper().Stop(ctx)

	sqlDB = sqlutils.MakeSQLRunner(s.SQLConn(t))
	sqlDB.Exec(t, `CREATE TABLE jobs (id INT PRIMARY KEY, params JSONB)`)
	sqlDB.Exec(t, `INSERT INTO jobs VALUES (1, '{"hash": "seed"}')`)

	// Positive control: confirm the unsafe-internals gate is actually enforced,
	// otherwise the assertions below would pass vacuously.
	_, ctlErr := sqlDB.DB.ExecContext(ctx, `SELECT count(*) FROM system.users`)
	require.ErrorContains(t, ctlErr, restricted,
		"unsafe-internals gate is not enforced; the rest of this test would be vacuous")

	setupDone.Store(true)
	_, err := sqlDB.DB.ExecContext(ctx, `CREATE INDEX jobs_hash_idx ON jobs ((params->>'hash'))`)
	require.NoError(t, err)

	// The hook must have run, otherwise the test exercises nothing.
	require.True(t, hookFired.Load(), "BeforeStage hook never fired for the CREATE INDEX")

	failuresMu.Lock()
	defer failuresMu.Unlock()
	require.Empty(t, failures,
		"concurrent DML during the expression-index backfill must not be rejected")
}
