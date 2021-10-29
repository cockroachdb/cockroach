// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package schemachanger_test

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scbuild"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scdeps/sctestdeps"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scdeps/sctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scrun"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

// TestEndToEndCatalog is a data-driven end-to-end test of the declarative
// schema changer which checks the state of the catalog after executing a DDL
// statement. The statement execution is done in different ways, with identical
// outcomes:
// - using a test cluster with the legacy schema changer,
// - using a test cluster with the declarative schema changer,
// - using the declarative schema changer with test dependencies.
func TestEndToEndCatalog(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	for _, mode := range []struct {
		name string
		exec execSchemaChangeFn
	}{
		{
			// We run these first as a sanity check.
			name: "sanity_check",
			exec: execInCluster,
		},
		{
			name: "in_memory",
			exec: execNewSchemaChangerInMemory,
		},
		{
			name: "in_cluster",
			exec: execNewSchemaChangerInCluster,
		},
	} {
		t.Run(mode.name, func(t *testing.T) {
			runDataDrivenSchemaChangeTest(ctx, t, "catalog_state", mode.exec, marshalCatalog)
		})
	}
}

// execSchemaChangeFn is for functions which actually execute the DDL statement
// to be tested.
type execSchemaChangeFn func(
	ctx context.Context,
	t *testing.T,
	tdb *sqlutils.SQLRunner,
	stmt parser.Statement,
) *sctestdeps.TestState

// marshallTestStateFn is for functions which map the final test state to the
// expected text output in the data-driven test data.
type marshallTestStateFn func(
	ctx context.Context,
	t *testing.T,
	s *sctestdeps.TestState,
) string

// runDataDrivenSchemaChangeTest runs data-driven end-to-end schema changer
// tests according to a common spec format.
func runDataDrivenSchemaChangeTest(
	ctx context.Context,
	t *testing.T,
	file string,
	exec execSchemaChangeFn,
	marshal marshallTestStateFn,
) {
	datadriven.Walk(t, filepath.Join("testdata", file), func(t *testing.T, p string) {
		s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
		defer s.Stopper().Stop(ctx)

		tdb := sqlutils.MakeSQLRunner(sqlDB)

		datadriven.RunTest(t, p, func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "setup":
				tdb.Exec(t, "SET experimental_use_new_schema_changer = 'off'")
				stmts, err := parser.Parse(d.Input)
				require.NoError(t, err)
				require.NotEmpty(t, stmts)
				for _, stmt := range stmts {
					tdb.Exec(t, stmt.SQL)
				}
				waitForSchemaChangeJobsCompletion(t, tdb)
				return ""

			case "test":
				stmts, err := parser.Parse(d.Input)
				require.NoError(t, err)
				require.Len(t, stmts, 1)
				stmt := stmts[0]
				// Run the schema change and return the resulting state.
				testState := exec(ctx, t, tdb, stmt)
				waitForSchemaChangeJobsCompletion(t, tdb)
				// Return the serialized catalog from the test state.
				// This should be the same regardless of the testing mode.
				return marshal(ctx, t, testState)

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
	})
}

// execNewSchemaChangerInMemory executes the DDL statement using the new schema
// changer with in-memory testing dependencies.
func execNewSchemaChangerInMemory(
	ctx context.Context, t *testing.T, tdb *sqlutils.SQLRunner, stmt parser.Statement,
) *sctestdeps.TestState {
	// Initialize test state with current cluster state.
	deps := sctestdeps.NewTestDependencies(ctx, t, tdb, nil /* testingKnobs */, []string{stmt.SQL})

	{ // Run new schema changer with in-memory testing dependencies.
		state, err := scbuild.Build(ctx, deps, nil /* initial */, stmt.AST)
		require.NoError(t, err, "error in builder")
		// Run statement phase.
		deps.WithTxn(scop.StatementPhase, func(s *sctestdeps.TestState) {
			state, err = scrun.RunSchemaChangesInTxn(ctx, s, state)
			require.NoError(t, err, "error in %s", s.Phase())
		})

		// Run pre-commit phase.
		var jobID jobspb.JobID
		deps.WithTxn(scop.PreCommitPhase, func(s *sctestdeps.TestState) {
			state, err = scrun.RunSchemaChangesInTxn(ctx, s, state)
			require.NoError(t, err, "error in %s", s.Phase())
			jobID, err = scrun.CreateSchemaChangeJob(ctx, s, state)
			require.NoError(t, err, "error in mock schema change job creation")
		})

		if job := deps.JobRecord(jobID); job != nil {
			// Mock running the schema change job.
			details := job.Details.(jobspb.NewSchemaChangeDetails)
			progress := job.Progress.(jobspb.NewSchemaChangeProgress)
			err = scrun.RunSchemaChangesInJob(ctx, deps, jobID, job.DescriptorIDs, details, progress)
			require.NoError(t, err, "error in mock schema change job execution")
		}
	}

	// Running the new schema changer using the test dependencies doesn't have
	// any side effects outside the test state. We therefore need to execute
	// the statement on the cluster using the old schema changer to move the
	// cluster state forward, assuming of course that the old schema changer
	// behaves correctly
	tdb.Exec(t, stmt.SQL)

	return deps
}

// execInCluster executes the DDL statement in the cluster.
// Unless specified otherwise, the legacy schema changer will be used.
func execInCluster(
	ctx context.Context, t *testing.T, tdb *sqlutils.SQLRunner, stmt parser.Statement,
) *sctestdeps.TestState {
	tdb.Exec(t, stmt.SQL)
	// Return a test state built _after_ running the DDL statement.
	return sctestdeps.NewTestDependencies(ctx, t, tdb, nil /* testingKnobs */, []string{stmt.SQL})
}

// execNewSchemaChangerInCluster executes the DDL statement in the cluster
// using the new schema changer.
func execNewSchemaChangerInCluster(
	ctx context.Context, t *testing.T, tdb *sqlutils.SQLRunner, stmt parser.Statement,
) *sctestdeps.TestState {
	tdb.Exec(t, "SET experimental_use_new_schema_changer = 'unsafe_always'")
	return execInCluster(ctx, t, tdb, stmt)
}

// waitForSchemaChangeJobsCompletion waits for all old schema changer jobs to
// complete.
func waitForSchemaChangeJobsCompletion(t *testing.T, tdb *sqlutils.SQLRunner) {
	q := fmt.Sprintf(`
		SELECT count(*)
		FROM [SHOW JOBS]
		WHERE job_type IN ('%s', '%s', '%s')
			AND status <> 'succeeded'`,
		jobspb.TypeSchemaChange,
		jobspb.TypeTypeSchemaChange,
		jobspb.TypeNewSchemaChange,
	)
	tdb.CheckQueryResultsRetry(t, q, [][]string{{"0"}})
}

// marshalCatalog returns a text representation of the state of the catalog
// (descriptors and namespace entries) in the test state.
func marshalCatalog(ctx context.Context, t *testing.T, s *sctestdeps.TestState) string {
	result := strings.Builder{}
	writeln := func(str string) {
		result.WriteString(str)
		if !strings.HasSuffix(str, "\n") {
			result.WriteRune('\n')
		}
	}

	var nonDropped []catalog.Descriptor
	_ = s.ForEachDescriptor(func(desc catalog.Descriptor) error {
		if desc.Dropped() || desc.Offline() {
			return nil
		}
		nonDropped = append(nonDropped, desc.NewBuilder().BuildCreatedMutable())
		return nil
	})
	ve := catalog.Validate(ctx, s, catalog.NoValidationTelemetry, catalog.ValidationLevelAllPreTxnCommit, nonDropped...)
	if len(ve.Errors()) > 0 {
		for _, err := range ve.Errors() {
			writeln(err.Error())
		}
	}
	for _, desc := range nonDropped {
		switch t := desc.(type) {
		// Redact time-dependent and other non-interesting fields.
		case *dbdesc.Mutable:
			t.ModificationTime = hlc.Timestamp{}
			t.DefaultPrivileges = nil
			t.Privileges = nil
		case *schemadesc.Mutable:
			t.ModificationTime = hlc.Timestamp{}
			t.Privileges = nil
		case *tabledesc.Mutable:
			t.TableDescriptor.ModificationTime = hlc.Timestamp{}
			t.TableDescriptor.CreateAsOfTime = hlc.Timestamp{}
			t.TableDescriptor.DropTime = 0
			t.Privileges = nil
		case *typedesc.Mutable:
			t.TypeDescriptor.ModificationTime = hlc.Timestamp{}
			t.Privileges = nil
		}

		yaml, err := sctestutils.ProtoToYAML(desc.DescriptorProto())
		require.NoError(t, err)
		writeln(yaml)
	}
	return result.String()
}

// TestEndToEndSideEffects is a data-driven end-to-end test of the declarative
// schema changer which checks the side effects modifying the cluster state
// during the execution of a DDL statement by the declarative schema changer
// using its test dependencies.
func TestEndToEndSideEffects(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	runDataDrivenSchemaChangeTest(
		ctx,
		t,
		"side_effects",
		execNewSchemaChangerInMemory,
		func(ctx context.Context, t *testing.T, s *sctestdeps.TestState) string {
			return s.SideEffectLog()
		},
	)
}
