// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdctest"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

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
		// The requirement ships off on this branch; the tests opt in.
		`SET CLUSTER SETTING sql.auth.external_connection.require_usage_privilege.enabled = true`,
	)

	userDB := sqlutils.MakeSQLRunner(srv.SQLConn(t, serverutils.User(username.TestUser)))

	usageErr := func(connection string) string {
		return fmt.Sprintf(
			"user testuser does not have USAGE privilege on external_connection %s", connection)
	}

	const onlyExternalConnectionErr = "the CHANGEFEED privilege on all target tables " +
		"can only be used with external connection sinks"

	t.Run("create-sink", func(t *testing.T) {
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
		schemaReg := cdctest.StartTestSchemaRegistry()
		defer schemaReg.Close()

		rootDB.Exec(t, `CREATE EXTERNAL CONNECTION alter_reg_sink AS 'kafka://nope'`)
		rootDB.Exec(t, `GRANT USAGE ON EXTERNAL CONNECTION alter_reg_sink TO testuser`)

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
		defer rootDB.Exec(t,
			`SET CLUSTER SETTING sql.auth.external_connection.require_usage_privilege.enabled = true`)
		userDB.Exec(t, alter)
		userDB.Exec(t, `CANCEL JOB $1`, jobID)
	})

	// CONTROLCHANGEFEED lets a user substitute SELECT for the CHANGEFEED
	// privilege on the target tables. It says nothing about the credentials a
	// connection holds, so USAGE still applies.
	t.Run("controlchangefeed-role-option", func(t *testing.T) {
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
		rootDB.Exec(t,
			`SET CLUSTER SETTING changefeed.permissions.require_external_connection_sink.enabled = true`)
		defer rootDB.Exec(t,
			`RESET CLUSTER SETTING changefeed.permissions.require_external_connection_sink.enabled`)
		ccDB.QueryRow(t, `CREATE CHANGEFEED FOR foo INTO 'kafka://nope'`).Scan(&jobID)
		ccDB.Exec(t, `CANCEL JOB $1`, jobID)

		rootDB.Exec(t,
			`SET CLUSTER SETTING sql.auth.external_connection.require_usage_privilege.enabled = false`)
		defer rootDB.Exec(t,
			`SET CLUSTER SETTING sql.auth.external_connection.require_usage_privilege.enabled = true`)
		rootDB.Exec(t, `REVOKE USAGE ON EXTERNAL CONNECTION cc_sink FROM ccuser`)
		ccDB.QueryRow(t, `CREATE CHANGEFEED FOR foo INTO 'external://cc_sink'`).Scan(&jobID)
		ccDB.Exec(t, `CANCEL JOB $1`, jobID)
	})

	// The escape hatch exists for feeds that predate the requirement, whose
	// owners may not hold USAGE on the connections those feeds use. ALTER
	// re-authorizes the whole feed, so it is rejected even when the statement
	// leaves the sink alone.
	t.Run("existing-feed-without-usage", func(t *testing.T) {
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
		defer rootDB.Exec(t,
			`SET CLUSTER SETTING sql.auth.external_connection.require_usage_privilege.enabled = true`)
		userDB.Exec(t, alter)
		userDB.Exec(t, `CANCEL JOB $1`, jobID)
	})

	// Turning the requirement off must leave exactly the checks that predate it,
	// and no fewer.
	t.Run("usage-requirement-disabled", func(t *testing.T) {
		rootDB.Exec(t,
			`SET CLUSTER SETTING sql.auth.external_connection.require_usage_privilege.enabled = false`)
		defer rootDB.Exec(t,
			`SET CLUSTER SETTING sql.auth.external_connection.require_usage_privilege.enabled = true`)

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
