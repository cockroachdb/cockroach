// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdctest"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/security/externalcreds"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

// pgErrCode returns the pgcode of err as reported by the SQL driver, or the
// empty string if err is not a *pq.Error.
func pgErrCode(err error) string {
	var pqErr *pq.Error
	if !errors.As(err, &pqErr) {
		return ""
	}
	return string(pqErr.Code)
}

// fileBasedClientAssertionSinkURI builds a PROPRIETARY_OAUTH file-mode sink
// URI. jwtRelPath is interpreted relative to the node's
// --external-credentials-dir.
func fileBasedClientAssertionSinkURI(t *testing.T, jwtRelPath string) string {
	t.Helper()
	return fmt.Sprintf(
		"kafka://127.0.0.1:1/?tls_enabled=true&sasl_enabled=true"+
			"&sasl_mechanism=PROPRIETARY_OAUTH"+
			"&sasl_client_id=c"+
			"&sasl_token_url=https://idp/token"+
			"&sasl_proprietary_resource=r"+
			"&sasl_proprietary_client_assertion_type=at"+
			"&sasl_proprietary_client_assertion_location=%s",
		url.QueryEscape(jwtRelPath),
	)
}

func TestChangefeedFileBasedCredentialPrivilege(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	secretDir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(secretDir, "jwt"), []byte("dummy"), 0o600))

	ecDir, err := externalcreds.NewDir(secretDir)
	require.NoError(t, err)
	srv := serverutils.StartServerOnly(t, base.TestServerArgs{
		ExternalCredentialsDir: ecDir,
	})
	defer srv.Stopper().Stop(ctx)

	sqlutils.MakeSQLRunner(srv.SystemLayer().SQLConn(t)).Exec(
		t, `SET CLUSTER SETTING kv.rangefeed.enabled = true`)
	app := srv.ApplicationLayer()
	rootDB := sqlutils.MakeSQLRunner(app.SQLConn(t))
	rootDB.Exec(t, `CREATE TABLE t (x INT PRIMARY KEY)`)
	rootDB.Exec(t, `CREATE USER testuser`)
	rootDB.Exec(t, `GRANT CHANGEFEED ON TABLE t TO testuser`)
	// The EXTERNALIOIMPLICITACCESS requirement ships off on this release
	// branch, so the test opts in.
	rootDB.Exec(t,
		`SET CLUSTER SETTING changefeed.permissions.require_external_io_implicit_access.enabled = true`)

	testuser := app.SQLConn(t, serverutils.User("testuser"))
	sinkURI := fileBasedClientAssertionSinkURI(t, "jwt")

	rootDB.Exec(t, fmt.Sprintf(`CREATE EXTERNAL CONNECTION ec AS '%s'`, sinkURI))
	rootDB.Exec(t, `GRANT USAGE ON EXTERNAL CONNECTION ec TO testuser`)
	const ecURI = `external://ec`

	_, err = testuser.ExecContext(ctx, `CREATE CHANGEFEED FOR t INTO $1`, sinkURI)
	require.Equal(t, pgcode.InsufficientPrivilege.String(), pgErrCode(err),
		"expected privilege error on direct URI, got: %v", err)
	require.ErrorContains(t, err, `EXTERNALIOIMPLICITACCESS`)

	_, err = testuser.ExecContext(ctx, `CREATE CHANGEFEED FOR t INTO $1`, ecURI)
	require.Equal(t, pgcode.InsufficientPrivilege.String(), pgErrCode(err),
		"expected privilege error on external connection, got: %v", err)
	require.ErrorContains(t, err, changefeedbase.SinkParamSASLProprietaryClientAssertionLocation)

	// After the grant, the sink dials 127.0.0.1:1 so an error is still
	// expected; what matters is that the privilege gate is no longer the
	// failure reason on either path.
	rootDB.Exec(t, `GRANT SYSTEM EXTERNALIOIMPLICITACCESS TO testuser`)
	for _, uri := range []string{sinkURI, ecURI} {
		_, err = testuser.ExecContext(ctx, `CREATE CHANGEFEED FOR t INTO $1`, uri)
		require.Error(t, err)
		require.NotEqual(t, pgcode.InsufficientPrivilege.String(), pgErrCode(err),
			"unexpected privilege error after grant for %q: %v", uri, err)
	}
}

// TestChangefeedRequiresUsageOnExternalConnection asserts that a changefeed
// requires USAGE on every external connection it names, whether or not
// changefeed.permissions.require_external_connection_sink.enabled is set.
func TestChangefeedRequiresUsageOnExternalConnection(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
			DistSQL: &execinfra.TestingKnobs{
				Changefeed: &TestingKnobs{
					// The kafka:// URIs point at nothing; this test is about planning.
					WrapSink: func(s Sink, _ jobspb.JobID) Sink {
						if _, ok := s.(*externalConnectionKafkaSink); ok {
							return s
						}
						return &externalConnectionKafkaSink{sink: s, ignoreDialError: true}
					},
				},
			},
		},
	})
	defer srv.Stopper().Stop(ctx)

	sqlutils.MakeSQLRunner(srv.SystemLayer().SQLConn(t)).Exec(t,
		`SET CLUSTER SETTING kv.rangefeed.enabled = true`)

	rootDB := sqlutils.MakeSQLRunner(db)
	rootDB.ExecMultiple(t,
		`CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`,
		`INSERT INTO foo VALUES (0, 'dog')`,
		`CREATE USER testuser`,
		`GRANT CHANGEFEED ON TABLE foo TO testuser`,
	)

	userDB := sqlutils.MakeSQLRunner(srv.SQLConn(t, serverutils.User(username.TestUser)))

	// The USAGE requirement ships off on this release branch (the cluster setting
	// defaults to false), so a subtest that relies on it enforces the opt-in
	// itself and restores the default when it ends. Keeping the opt-in
	// subtest-local avoids depending on the order subtests run in or on another
	// subtest's cleanup.
	requireUsagePrivilege := func(t *testing.T) {
		rootDB.Exec(t,
			`SET CLUSTER SETTING sql.auth.external_connection.require_usage_privilege.enabled = true`)
		t.Cleanup(func() {
			rootDB.Exec(t,
				`RESET CLUSTER SETTING sql.auth.external_connection.require_usage_privilege.enabled`)
		})
	}

	usageErr := func(connection string) string {
		return fmt.Sprintf(
			"user testuser does not have USAGE privilege on external_connection %s", connection)
	}

	const onlyExternalConnectionErr = "the CHANGEFEED privilege on all tables " +
		"can only be used with external connection sinks"

	t.Run("create-sink", func(t *testing.T) {
		requireUsagePrivilege(t)
		rootDB.Exec(t, `CREATE EXTERNAL CONNECTION create_sink AS 'kafka://nope'`)

		userDB.ExpectErr(t, usageErr("create_sink"),
			`CREATE CHANGEFEED FOR foo INTO 'external://create_sink'`)

		rootDB.Exec(t, `GRANT USAGE ON EXTERNAL CONNECTION create_sink TO testuser`)
		var jobID jobspb.JobID
		userDB.QueryRow(t,
			`CREATE CHANGEFEED FOR foo INTO 'external://create_sink'`).Scan(&jobID)
		userDB.Exec(t, `CANCEL JOB $1`, jobID)
	})

	t.Run("schema-registry", func(t *testing.T) {
		requireUsagePrivilege(t)
		schemaReg := cdctest.StartTestSchemaRegistry()
		defer schemaReg.Close()

		rootDB.Exec(t, `CREATE EXTERNAL CONNECTION registry_sink AS 'kafka://nope'`)
		rootDB.Exec(t, `GRANT USAGE ON EXTERNAL CONNECTION registry_sink TO testuser`)
		rootDB.Exec(t, fmt.Sprintf(
			`CREATE EXTERNAL CONNECTION registry AS '%s'`, schemaReg.URL()))

		const create = `CREATE CHANGEFEED FOR foo INTO 'external://registry_sink' ` +
			`WITH format = avro, confluent_schema_registry = 'external://registry'`
		userDB.ExpectErr(t, usageErr("registry"), create)

		rootDB.Exec(t, `GRANT USAGE ON EXTERNAL CONNECTION registry TO testuser`)
		var jobID jobspb.JobID
		userDB.QueryRow(t, create).Scan(&jobID)

		// ALTER has to repeat the check, since it can name a new connection.
		userDB.Exec(t, `PAUSE JOB $1`, jobID)
		waitForJobState(userDB, t, jobID, `paused`)
		rootDB.Exec(t, fmt.Sprintf(
			`CREATE EXTERNAL CONNECTION registry2 AS '%s'`, schemaReg.URL()))
		alter := fmt.Sprintf(
			`ALTER CHANGEFEED %d SET confluent_schema_registry = 'external://registry2'`, jobID)
		userDB.ExpectErr(t, usageErr("registry2"), alter)

		rootDB.Exec(t, `GRANT USAGE ON EXTERNAL CONNECTION registry2 TO testuser`)
		userDB.Exec(t, alter)
		userDB.Exec(t, `CANCEL JOB $1`, jobID)
	})

	// ALTER holds the registry to the same external connection requirement as
	// CREATE, and turning the USAGE requirement off must not make it stricter
	// than it was without one.
	t.Run("alter-registry-must-be-external-connection", func(t *testing.T) {
		requireUsagePrivilege(t)
		schemaReg := cdctest.StartTestSchemaRegistry()
		defer schemaReg.Close()

		rootDB.Exec(t, `CREATE EXTERNAL CONNECTION alter_reg_sink AS 'kafka://nope'`)
		rootDB.Exec(t, `GRANT USAGE ON EXTERNAL CONNECTION alter_reg_sink TO testuser`)

		rootDB.Exec(t, `GRANT SYSTEM EXTERNALIOIMPLICITACCESS TO testuser`)
		defer rootDB.Exec(t, `REVOKE SYSTEM EXTERNALIOIMPLICITACCESS FROM testuser`)

		var jobID jobspb.JobID
		userDB.QueryRow(t, fmt.Sprintf(
			`CREATE CHANGEFEED FOR foo INTO 'external://alter_reg_sink' `+
				`WITH format = avro, confluent_schema_registry = '%s'`, schemaReg.URL())).Scan(&jobID)
		userDB.Exec(t, `PAUSE JOB $1`, jobID)
		waitForJobState(userDB, t, jobID, `paused`)

		rootDB.Exec(t,
			`SET CLUSTER SETTING changefeed.permissions.require_external_connection_sink.enabled = true`)
		defer rootDB.Exec(t,
			`RESET CLUSTER SETTING changefeed.permissions.require_external_connection_sink.enabled`)

		alter := fmt.Sprintf(`ALTER CHANGEFEED %d SET resolved = '10s'`, jobID)
		userDB.ExpectErr(t, onlyExternalConnectionErr,
			alter)

		rootDB.Exec(t,
			`SET CLUSTER SETTING sql.auth.external_connection.require_usage_privilege.enabled = false`)
		userDB.Exec(t, alter)
		userDB.Exec(t, `CANCEL JOB $1`, jobID)
	})

	t.Run("alter-registry-requires-implicit-access", func(t *testing.T) {
		rootDB.Exec(t,
			`SET CLUSTER SETTING changefeed.permissions.require_external_io_implicit_access.enabled = true`)
		defer rootDB.Exec(t,
			`RESET CLUSTER SETTING changefeed.permissions.require_external_io_implicit_access.enabled`)

		schemaReg := cdctest.StartTestSchemaRegistry()
		defer schemaReg.Close()

		rootDB.Exec(t, `CREATE EXTERNAL CONNECTION impl_reg_sink AS 'kafka://nope'`)
		rootDB.Exec(t, `GRANT USAGE ON EXTERNAL CONNECTION impl_reg_sink TO testuser`)
		rootDB.Exec(t, fmt.Sprintf(
			`CREATE EXTERNAL CONNECTION impl_reg AS '%s'`, schemaReg.URL()))
		rootDB.Exec(t, `GRANT USAGE ON EXTERNAL CONNECTION impl_reg TO testuser`)

		var jobID jobspb.JobID
		userDB.QueryRow(t, `CREATE CHANGEFEED FOR foo INTO 'external://impl_reg_sink' `+
			`WITH format = avro, confluent_schema_registry = 'external://impl_reg'`).Scan(&jobID)
		userDB.Exec(t, `PAUSE JOB $1`, jobID)
		waitForJobState(userDB, t, jobID, `paused`)

		rootDB.Exec(t,
			`SET CLUSTER SETTING sql.auth.external_connection.require_usage_privilege.enabled = false`)
		defer rootDB.Exec(t,
			`RESET CLUSTER SETTING sql.auth.external_connection.require_usage_privilege.enabled`)

		alter := fmt.Sprintf(
			`ALTER CHANGEFEED %d SET confluent_schema_registry = '%s'`, jobID, schemaReg.URL())
		userDB.ExpectErr(t, "EXTERNALIOIMPLICITACCESS", alter)

		rootDB.Exec(t, `GRANT SYSTEM EXTERNALIOIMPLICITACCESS TO testuser`)
		defer rootDB.Exec(t, `REVOKE SYSTEM EXTERNALIOIMPLICITACCESS FROM testuser`)
		userDB.Exec(t, alter)
		userDB.Exec(t, `CANCEL JOB $1`, jobID)
	})

	// CONTROLCHANGEFEED lets a user substitute SELECT for the CHANGEFEED
	// privilege on the target tables. It says nothing about the credentials a
	// connection holds, so USAGE still applies.
	t.Run("controlchangefeed-role-option", func(t *testing.T) {
		requireUsagePrivilege(t)
		rootDB.ExecMultiple(t,
			`CREATE USER ccuser WITH PASSWORD 'pw' CONTROLCHANGEFEED`,
			`GRANT SELECT ON TABLE foo TO ccuser`,
			`CREATE EXTERNAL CONNECTION cc_sink AS 'kafka://nope'`,
		)
		ccDB := sqlutils.MakeSQLRunner(srv.SQLConn(t,
			serverutils.UserPassword("ccuser", "pw"), serverutils.ClientCerts(false)))

		ccDB.ExpectErr(t,
			"user ccuser does not have USAGE privilege on external_connection cc_sink",
			`CREATE CHANGEFEED FOR foo INTO 'external://cc_sink'`)

		rootDB.Exec(t, `GRANT USAGE ON EXTERNAL CONNECTION cc_sink TO ccuser`)
		var jobID jobspb.JobID
		ccDB.QueryRow(t, `CREATE CHANGEFEED FOR foo INTO 'external://cc_sink'`).Scan(&jobID)
		ccDB.Exec(t, `CANCEL JOB $1`, jobID)

		// The role option still permits any sink, so the restriction
		// require_external_connection_sink imposes does not reach it.
		rootDB.Exec(t, `GRANT SYSTEM EXTERNALIOIMPLICITACCESS TO ccuser`)
		rootDB.Exec(t,
			`SET CLUSTER SETTING changefeed.permissions.require_external_connection_sink.enabled = true`)
		defer rootDB.Exec(t,
			`RESET CLUSTER SETTING changefeed.permissions.require_external_connection_sink.enabled`)
		ccDB.QueryRow(t, `CREATE CHANGEFEED FOR foo INTO 'kafka://nope'`).Scan(&jobID)
		ccDB.Exec(t, `CANCEL JOB $1`, jobID)

		rootDB.Exec(t,
			`SET CLUSTER SETTING sql.auth.external_connection.require_usage_privilege.enabled = false`)
		rootDB.Exec(t, `REVOKE USAGE ON EXTERNAL CONNECTION cc_sink FROM ccuser`)
		ccDB.QueryRow(t, `CREATE CHANGEFEED FOR foo INTO 'external://cc_sink'`).Scan(&jobID)
		ccDB.Exec(t, `CANCEL JOB $1`, jobID)
	})

	// The escape hatch exists for feeds that predate the requirement, whose
	// owners may not hold USAGE on the connections those feeds use. ALTER
	// re-authorizes the whole feed, so it is rejected even when the statement
	// leaves the sink alone.
	t.Run("existing-feed-without-usage", func(t *testing.T) {
		requireUsagePrivilege(t)
		rootDB.Exec(t, `CREATE EXTERNAL CONNECTION legacy_sink AS 'kafka://nope'`)
		rootDB.Exec(t, `GRANT USAGE ON EXTERNAL CONNECTION legacy_sink TO testuser`)

		var jobID jobspb.JobID
		userDB.QueryRow(t,
			`CREATE CHANGEFEED FOR foo INTO 'external://legacy_sink'`).Scan(&jobID)
		userDB.Exec(t, `PAUSE JOB $1`, jobID)
		waitForJobState(userDB, t, jobID, `paused`)
		rootDB.Exec(t, `REVOKE USAGE ON EXTERNAL CONNECTION legacy_sink FROM testuser`)

		alter := fmt.Sprintf(`ALTER CHANGEFEED %d SET resolved = '10s'`, jobID)
		userDB.ExpectErr(t, usageErr("legacy_sink"), alter)

		rootDB.Exec(t,
			`SET CLUSTER SETTING sql.auth.external_connection.require_usage_privilege.enabled = false`)
		userDB.Exec(t, alter)
		userDB.Exec(t, `CANCEL JOB $1`, jobID)
	})

	// Turning the requirement off must leave exactly the checks that predate it,
	// and no fewer.
	t.Run("usage-requirement-disabled", func(t *testing.T) {
		rootDB.Exec(t,
			`SET CLUSTER SETTING sql.auth.external_connection.require_usage_privilege.enabled = false`)
		defer rootDB.Exec(t,
			`RESET CLUSTER SETTING sql.auth.external_connection.require_usage_privilege.enabled`)

		rootDB.Exec(t, `CREATE EXTERNAL CONNECTION ungranted_sink AS 'kafka://nope'`)
		var jobID jobspb.JobID
		userDB.QueryRow(t,
			`CREATE CHANGEFEED FOR foo INTO 'external://ungranted_sink'`).Scan(&jobID)
		userDB.Exec(t, `CANCEL JOB $1`, jobID)

		// require_external_connection_sink still demands both that the sink be an
		// external connection and that the user hold USAGE on it.
		rootDB.Exec(t,
			`SET CLUSTER SETTING changefeed.permissions.require_external_connection_sink.enabled = true`)
		defer rootDB.Exec(t,
			`RESET CLUSTER SETTING changefeed.permissions.require_external_connection_sink.enabled`)

		userDB.ExpectErr(t, usageErr("ungranted_sink"),
			`CREATE CHANGEFEED FOR foo INTO 'external://ungranted_sink'`)
		userDB.ExpectErr(t, onlyExternalConnectionErr,
			`CREATE CHANGEFEED FOR foo INTO 'kafka://nope'`)

		// Unlike ALTER, CREATE applies the external connection requirement to the
		// registry too, whether or not USAGE is required.
		schemaReg := cdctest.StartTestSchemaRegistry()
		defer schemaReg.Close()
		rootDB.Exec(t, `GRANT USAGE ON EXTERNAL CONNECTION ungranted_sink TO testuser`)
		userDB.ExpectErr(t, onlyExternalConnectionErr, fmt.Sprintf(
			`CREATE CHANGEFEED FOR foo INTO 'external://ungranted_sink' `+
				`WITH format = avro, confluent_schema_registry = '%s'`, schemaReg.URL()))
	})
}
