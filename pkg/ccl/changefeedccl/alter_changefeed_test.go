// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"cmp"
	"context"
	gosql "database/sql"
	"fmt"
	"maps"
	"math/rand"
	"net/url"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdceval"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdctest"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobfrontier"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/interval"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestAlterChangefeedAddTargetPrivileges tests permissions for
// users creating new changefeeds while altering them.
func TestAlterChangefeedAddTargetPrivileges(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		DefaultTestTenant: base.TestDoesNotWorkWithSecondaryTenantsButWeDontKnowWhyYet(142799),
		Knobs: base.TestingKnobs{
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
			DistSQL: &execinfra.TestingKnobs{
				Changefeed: &TestingKnobs{
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
	defer s.Stopper().Stop(ctx)

	rootDB := sqlutils.MakeSQLRunner(db)
	rootDB.ExecMultiple(
		t,
		`CREATE TYPE type_a as enum ('a')`,
		`CREATE TABLE table_a (id int, type type_a)`,
		`CREATE TABLE table_b (id int, type type_a)`,
		`CREATE TABLE table_c (id int, type type_a)`,
		`CREATE USER feedCreator`,
		`CREATE ROLE feedowner`,
		`GRANT feedowner TO feedCreator`,
		`GRANT SELECT ON table_a TO feedCreator`,
		`GRANT CHANGEFEED ON table_a TO feedCreator`,
		`CREATE EXTERNAL CONNECTION "first" AS 'kafka://nope'`,
		`GRANT USAGE ON EXTERNAL CONNECTION first TO feedCreator`,
		`INSERT INTO table_a(id) values (0)`,
	)

	rootDB.Exec(t, `SET CLUSTER SETTING kv.rangefeed.enabled = true`)
	enableEnterprise := utilccl.TestingDisableEnterprise()
	enableEnterprise()

	withUser := func(t *testing.T, user string, fn func(*sqlutils.SQLRunner)) {
		password := `password`
		rootDB.Exec(t, fmt.Sprintf(`ALTER USER %s WITH PASSWORD '%s'`, user, password))

		pgURL := url.URL{
			Scheme: "postgres",
			User:   url.UserPassword(user, password),
			Host:   s.SQLAddr(),
		}
		db2, err := gosql.Open("postgres", pgURL.String())
		if err != nil {
			t.Fatal(err)
		}
		defer db2.Close()
		userDB := sqlutils.MakeSQLRunner(db2)

		fn(userDB)
	}

	t.Run("using-changefeed-grant", func(t *testing.T) {
		rootDB.Exec(t, `CREATE EXTERNAL CONNECTION "second" AS 'kafka://nope'`)
		rootDB.Exec(t, `CREATE USER user1`)
		rootDB.Exec(t, `GRANT CHANGEFEED ON table_a TO user1`)

		var jobID int
		withUser(t, "feedCreator", func(userDB *sqlutils.SQLRunner) {
			row := userDB.QueryRow(t, "CREATE CHANGEFEED for table_a INTO 'external://first'")
			row.Scan(&jobID)
			userDB.Exec(t, `PAUSE JOB $1`, jobID)
			waitForJobState(userDB, t, catpb.JobID(jobID), `paused`)
			userDB.Exec(t, `ALTER JOB $1 OWNER TO feedowner`, jobID)
		})

		// user1 is missing the CHANGEFEED privilege on table_b and table_c.
		withUser(t, "user1", func(userDB *sqlutils.SQLRunner) {
			userDB.ExpectErr(t,
				"user user1 does not have privileges for job",
				fmt.Sprintf("ALTER CHANGEFEED %d ADD table_b, table_c set sink='external://second'", jobID),
			)
		})
		rootDB.Exec(t, `GRANT CHANGEFEED ON table_b TO user1`)
		withUser(t, "user1", func(userDB *sqlutils.SQLRunner) {
			userDB.ExpectErr(t,
				"user user1 does not have privileges for job",
				fmt.Sprintf("ALTER CHANGEFEED %d ADD table_b, table_c set sink='external://second'", jobID),
			)
		})
		rootDB.Exec(t, `GRANT feedowner TO user1`)
		rootDB.Exec(t, `GRANT CHANGEFEED ON table_c TO user1`)
		withUser(t, "user1", func(userDB *sqlutils.SQLRunner) {
			userDB.Exec(t,
				fmt.Sprintf("ALTER CHANGEFEED %d ADD table_b, table_c set sink='external://second'", jobID),
			)
		})

		// With require_external_connection_sink enabled, the user requires
		// USAGE on the external connection. Drop and re-add tables since
		// they were already added above.
		rootDB.Exec(t, "SET CLUSTER SETTING changefeed.permissions.require_external_connection_sink.enabled = true")
		withUser(t, "user1", func(userDB *sqlutils.SQLRunner) {
			userDB.ExpectErr(t,
				"user user1 does not have USAGE privilege on external_connection second",
				fmt.Sprintf("ALTER CHANGEFEED %d DROP table_b, table_c ADD table_b, table_c set sink='external://second'", jobID),
			)
		})
		rootDB.Exec(t, `GRANT USAGE ON EXTERNAL CONNECTION second TO user1`)
		withUser(t, "user1", func(userDB *sqlutils.SQLRunner) {
			userDB.Exec(t,
				fmt.Sprintf("ALTER CHANGEFEED %d DROP table_b, table_c ADD table_b, table_c set sink='external://second'", jobID),
			)
		})
		rootDB.Exec(t, "SET CLUSTER SETTING changefeed.permissions.require_external_connection_sink.enabled = false")
	})

	// TODO(#94757): remove CONTROLCHANGEFEED entirely
	t.Run("using-controlchangefeed-roleoption", func(t *testing.T) {
		rootDB.Exec(t, `CREATE USER user2 WITH CONTROLCHANGEFEED`)
		rootDB.Exec(t, `GRANT CHANGEFEED ON table_a TO user2`)
		rootDB.Exec(t, `GRANT SELECT ON table_a TO user2`)

		var jobID int
		withUser(t, "feedCreator", func(userDB *sqlutils.SQLRunner) {
			row := userDB.QueryRow(t, "CREATE CHANGEFEED for table_a INTO 'kafka://foo'")
			row.Scan(&jobID)
			userDB.Exec(t, `PAUSE JOB $1`, jobID)
			waitForJobState(userDB, t, catpb.JobID(jobID), `paused`)
			userDB.Exec(t, `ALTER JOB $1 OWNER TO feedowner`, jobID)
		})

		// user2 is missing the SELECT privilege on table_b and table_c.
		withUser(t, "user2", func(userDB *sqlutils.SQLRunner) {
			userDB.ExpectErr(t,
				"pq: user user2 does not have privileges for job",
				fmt.Sprintf("ALTER CHANGEFEED %d ADD table_b, table_c set sink='kafka://bar'", jobID),
			)
		})
		rootDB.Exec(t, `GRANT SELECT ON table_b TO user2`)
		withUser(t, "user2", func(userDB *sqlutils.SQLRunner) {
			userDB.ExpectErr(t,
				"pq: user user2 does not have privileges for job",
				fmt.Sprintf("ALTER CHANGEFEED %d ADD table_b, table_c set sink='kafka://bar'", jobID),
			)
		})
		rootDB.Exec(t, `GRANT feedowner TO user2`)
		withUser(t, "user2", func(userDB *sqlutils.SQLRunner) {
			userDB.ExpectErr(t,
				"requires the SELECT privilege on all target tables",
				fmt.Sprintf("ALTER CHANGEFEED %d ADD table_b, table_c set sink='kafka://bar'", jobID),
			)
		})
		rootDB.Exec(t, `GRANT SELECT ON table_c TO user2`)
		withUser(t, "user2", func(userDB *sqlutils.SQLRunner) {
			userDB.Exec(t,
				fmt.Sprintf("ALTER CHANGEFEED %d ADD table_b, table_c set sink='kafka://bar'", jobID),
			)
		})
	})
}

func TestAlterChangefeedAddTarget(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY)`)
		sqlDB.Exec(t, `CREATE TABLE bar (a INT PRIMARY KEY)`)

		testFeed := feed(t, f, `CREATE CHANGEFEED FOR foo`, optOutOfMetamorphicDBLevelChangefeed{
			reason: "db level changefeeds don't support ADD/DROP TARGETS in ALTER CHANGEFEEDs",
		})
		defer closeFeed(t, testFeed)

		feed, ok := testFeed.(cdctest.EnterpriseTestFeed)
		require.True(t, ok)

		sqlDB.Exec(t, `PAUSE JOB $1`, feed.JobID())
		waitForJobState(sqlDB, t, feed.JobID(), `paused`)

		sqlDB.Exec(t, fmt.Sprintf(`ALTER CHANGEFEED %d ADD bar`, feed.JobID()))

		sqlDB.Exec(t, fmt.Sprintf(`RESUME JOB %d`, feed.JobID()))
		waitForJobState(sqlDB, t, feed.JobID(), `running`)

		sqlDB.Exec(t, `INSERT INTO foo VALUES(1)`)
		assertPayloads(t, testFeed, []string{
			`foo: [1]->{"after": {"a": 1}}`,
		})

		sqlDB.Exec(t, `INSERT INTO bar VALUES(2)`)
		assertPayloads(t, testFeed, []string{
			`bar: [2]->{"after": {"a": 2}}`,
		})
	}

	cdcTest(t, testFn, feedTestEnterpriseSinks, feedTestNoExternalConnection)
}

// TestAlterChangefeedAddTargetAfterInitialScan tests adding a new target
// after the changefeed has already completed its initial scan.
func TestAlterChangefeedAddTargetAfterInitialScan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testutils.RunValues(t, "initial_scan", []string{"yes", "no", "only"}, func(t *testing.T, initialScan string) {
		testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
			sqlDB := sqlutils.MakeSQLRunner(s.DB)
			sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY)`)
			sqlDB.Exec(t, `CREATE TABLE bar (a INT PRIMARY KEY, b INT)`)

			testFeed := feed(t, f, `CREATE CHANGEFEED FOR foo`, optOutOfMetamorphicDBLevelChangefeed{
				reason: "db level changefeeds don't support ADD/DROP TARGETS in ALTER CHANGEFEEDs",
			})
			defer closeFeed(t, testFeed)

			feed, ok := testFeed.(cdctest.EnterpriseTestFeed)
			require.True(t, ok)

			checkHighwaterAdvance := func(ts hlc.Timestamp) func() error {
				return func() error {
					hw, err := feed.HighWaterMark()
					if err != nil {
						return err
					}
					if hw.After(ts) {
						return nil
					}
					return errors.Newf("waiting for highwater to advance past %s", ts)
				}
			}

			// Insert and update row into new table after changefeed was already created.
			sqlDB.Exec(t, `INSERT INTO bar VALUES(2, 2)`)
			sqlDB.Exec(t, `UPDATE bar SET b = 9 WHERE a = 2`)

			var tsStr string
			sqlDB.QueryRow(t, `INSERT INTO foo VALUES(1) RETURNING cluster_logical_timestamp()`).Scan(&tsStr)
			assertPayloads(t, testFeed, []string{
				`foo: [1]->{"after": {"a": 1}}`,
			})
			ts := parseTimeToHLC(t, tsStr)
			testutils.SucceedsSoon(t, checkHighwaterAdvance(ts))

			sqlDB.Exec(t, `PAUSE JOB $1`, feed.JobID())
			waitForJobState(sqlDB, t, feed.JobID(), `paused`)

			switch initialScan {
			case "only":
				// initial_scan = 'only' is not supported when adding targets
				// to a non-scan-only changefeed.
				sqlDB.ExpectErr(t,
					`cannot use initial_scan = 'only' when adding targets to a non-scan-only changefeed`,
					fmt.Sprintf(`ALTER CHANGEFEED %d ADD bar WITH initial_scan = 'only'`, feed.JobID()),
				)
				return
			default:
				sqlDB.Exec(t, fmt.Sprintf(`ALTER CHANGEFEED %d ADD bar WITH initial_scan = '%s'`, feed.JobID(), initialScan))
			}

			sqlDB.Exec(t, `RESUME JOB $1`, feed.JobID())
			waitForJobState(sqlDB, t, feed.JobID(), `running`)

			// Updates for the new table only start at the changefeed's current
			// highwater at the time of the ALTER CHANGEFEED.
			switch initialScan {
			case "yes":
				assertPayloads(t, testFeed, []string{
					// There is no `bar: [2]->{"after": {"a": 2, "b": 2}}` message
					// because it was inserted before the highwater.
					`bar: [2]->{"after": {"a": 2, "b": 9}}`,
				})
			case "no":
			default:
				t.Fatalf("unknown initial scan type %q", initialScan)
			}

			sqlDB.Exec(t, `INSERT INTO foo VALUES(2)`)
			assertPayloads(t, testFeed, []string{
				`foo: [2]->{"after": {"a": 2}}`,
			})

			sqlDB.Exec(t, `UPDATE bar SET b = 25 WHERE a = 2`)
			assertPayloads(t, testFeed, []string{
				`bar: [2]->{"after": {"a": 2, "b": 25}}`,
			})
		}

		cdcTest(t, testFn, feedTestEnterpriseSinks, feedTestNoExternalConnection)
	})
}

func TestAlterChangefeedAddTargetFamily(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testutils.SetVModule(t, "helpers_test=1")

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING, FAMILY onlya (a), FAMILY onlyb (b))`)

		testFeed := feed(t, f, `CREATE CHANGEFEED FOR foo FAMILY onlya`)
		defer closeFeed(t, testFeed)

		feed, ok := testFeed.(cdctest.EnterpriseTestFeed)
		require.True(t, ok)

		tsr := sqlDB.QueryRow(t, `INSERT INTO foo VALUES(42, 'hello') RETURNING cluster_logical_timestamp()`)
		var insertTsDecStr string
		tsr.Scan(&insertTsDecStr)
		insertTs := parseTimeToHLC(t, insertTsDecStr)
		assertPayloads(t, testFeed, []string{
			`foo.onlya: [42]->{"after": {"a": 42}}`,
		})

		// Wait for the high water mark (aka resolved ts) to advance past the row we inserted's
		// mvcc ts. Otherwise, we'd see [42] again due to a catch up scan, and it
		// would muddy the waters.
		testutils.SucceedsSoon(t, func() error {
			registry := s.Server.JobRegistry().(*jobs.Registry)
			job, err := registry.LoadJob(context.Background(), feed.JobID())
			require.NoError(t, err)
			prog := job.Progress()
			if p := prog.GetHighWater(); p != nil && !p.IsEmpty() && insertTs.Less(*p) {
				return nil
			}
			return errors.New("waiting for highwater")
		})

		sqlDB.Exec(t, `PAUSE JOB $1`, feed.JobID())
		waitForJobState(sqlDB, t, feed.JobID(), `paused`)

		sqlDB.Exec(t, fmt.Sprintf(`ALTER CHANGEFEED %d ADD foo FAMILY onlyb`, feed.JobID()))

		sqlDB.Exec(t, fmt.Sprintf(`RESUME JOB %d`, feed.JobID()))
		waitForJobState(sqlDB, t, feed.JobID(), `running`)
		sqlDB.Exec(t, `INSERT INTO foo VALUES(37, 'goodbye')`)
		assertPayloads(t, testFeed, []string{
			// Note that we don't see foo.onlyb.[42] here, because we're not
			// doing a catchup scan and we've already processed that tuple.
			`foo.onlya: [37]->{"after": {"a": 37}}`,
			`foo.onlyb: [37]->{"after": {"b": "goodbye"}}`,
		})
	}

	// TODO: Figure out why this freezes on other sinks (ex: webhook)
	cdcTest(t, testFn, feedTestForceSink("kafka"), feedTestNoExternalConnection)
}

func TestAlterChangefeedSwitchFamily(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testutils.SetVModule(t, "helpers_test=1")

	testutils.RunTrueAndFalse(t, "drop first", func(t *testing.T, dropFirst bool) {
		testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
			sqlDB := sqlutils.MakeSQLRunner(s.DB)
			sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING, FAMILY onlya (a), FAMILY onlyb (b))`)

			testFeed := feed(t, f, `CREATE CHANGEFEED FOR foo FAMILY onlya`)
			defer closeFeed(t, testFeed)

			tsr := sqlDB.QueryRow(t, `INSERT INTO foo VALUES(1, 'hello') RETURNING cluster_logical_timestamp()`)
			var insertTsDecStr string
			tsr.Scan(&insertTsDecStr)
			insertTs := parseTimeToHLC(t, insertTsDecStr)

			assertPayloads(t, testFeed, []string{
				`foo.onlya: [1]->{"after": {"a": 1}}`,
			})

			feed, ok := testFeed.(cdctest.EnterpriseTestFeed)
			require.True(t, ok)

			// Wait for the high water mark (aka resolved ts) to advance past the row we inserted's
			// mvcc ts. Otherwise, we'd see [1] again due to a catch up scan, and it
			// would muddy the waters.
			testutils.SucceedsSoon(t, func() error {
				registry := s.Server.JobRegistry().(*jobs.Registry)
				job, err := registry.LoadJob(context.Background(), feed.JobID())
				require.NoError(t, err)
				prog := job.Progress()
				if p := prog.GetHighWater(); p != nil && !p.IsEmpty() && insertTs.Less(*p) {
					return nil
				}
				return errors.New("waiting for highwater")
			})

			sqlDB.Exec(t, `PAUSE JOB $1`, feed.JobID())
			waitForJobState(sqlDB, t, feed.JobID(), `paused`)

			alterCmd := `ADD foo FAMILY onlyb DROP foo FAMILY onlya`
			if dropFirst {
				alterCmd = `DROP foo FAMILY onlya ADD foo FAMILY onlyb`
			}
			sqlDB.Exec(t, fmt.Sprintf(`ALTER CHANGEFEED %d %s`, feed.JobID(), alterCmd))

			sqlDB.Exec(t, fmt.Sprintf(`RESUME JOB %d`, feed.JobID()))
			waitForJobState(sqlDB, t, feed.JobID(), `running`)

			sqlDB.Exec(t, `INSERT INTO foo VALUES(2, 'goodbye')`)
			assertPayloads(t, testFeed, []string{
				// Note that we don't see foo.onlyb.[1] here, because we're not
				// doing a catchup scan and we've already processed that tuple.
				`foo.onlyb: [2]->{"after": {"b": "goodbye"}}`,
			})
		}

		// TODO: Figure out why this freezes on other sinks (ex: cloudstorage)
		cdcTest(t, testFn, feedTestForceSink("kafka"), feedTestNoExternalConnection)
	})
}

func TestAlterChangefeedDropTarget(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY)`)
		sqlDB.Exec(t, `CREATE TABLE bar (a INT PRIMARY KEY)`)

		testFeed := feed(t, f, `CREATE CHANGEFEED FOR foo, bar`, optOutOfMetamorphicDBLevelChangefeed{
			reason: "db level changefeeds don't support ADD/DROP TARGETS in ALTER CHANGEFEEDs",
		})
		defer closeFeed(t, testFeed)

		feed, ok := testFeed.(cdctest.EnterpriseTestFeed)
		require.True(t, ok)

		sqlDB.Exec(t, `PAUSE JOB $1`, feed.JobID())
		waitForJobState(sqlDB, t, feed.JobID(), `paused`)

		sqlDB.Exec(t, fmt.Sprintf(`ALTER CHANGEFEED %d DROP bar`, feed.JobID()))

		sqlDB.Exec(t, fmt.Sprintf(`RESUME JOB %d`, feed.JobID()))
		waitForJobState(sqlDB, t, feed.JobID(), `running`)

		sqlDB.Exec(t, `INSERT INTO foo VALUES(1)`)
		assertPayloads(t, testFeed, []string{
			`foo: [1]->{"after": {"a": 1}}`,
		})

		sqlDB.Exec(t, `INSERT INTO bar VALUES(2)`)
		assertPayloads(t, testFeed, nil)
	}

	cdcTest(t, testFn, feedTestEnterpriseSinks, feedTestNoExternalConnection)
}

func TestAlterChangefeedDropTargetAfterTableDrop(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY)`)
		sqlDB.Exec(t, `CREATE TABLE bar (a INT PRIMARY KEY)`)

		testFeed := feed(t, f, `CREATE CHANGEFEED FOR foo, bar WITH on_error='pause'`,
			optOutOfMetamorphicDBLevelChangefeed{
				reason: "db level changefeeds don't support ADD/DROP TARGETS in ALTER CHANGEFEEDs",
			},
		)
		defer closeFeed(t, testFeed)

		feed, ok := testFeed.(cdctest.EnterpriseTestFeed)
		require.True(t, ok)

		// Drop bar table.  This should cause the job to be paused.
		sqlDB.Exec(t, `DROP TABLE bar`)
		waitForJobState(sqlDB, t, feed.JobID(), `paused`)

		sqlDB.Exec(t, fmt.Sprintf(`ALTER CHANGEFEED %d DROP bar`, feed.JobID()))

		sqlDB.Exec(t, fmt.Sprintf(`RESUME JOB %d`, feed.JobID()))
		waitForJobState(sqlDB, t, feed.JobID(), `running`)

		sqlDB.Exec(t, `INSERT INTO foo VALUES(1)`)
		assertPayloads(t, testFeed, []string{
			`foo: [1]->{"after": {"a": 1}}`,
		})
	}

	cdcTest(t, testFn, feedTestEnterpriseSinks, feedTestNoExternalConnection, withAllowChangefeedErr("error is expected when dropping"))
}

func TestAlterChangefeedDropTargetFamily(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING, FAMILY onlya (a), FAMILY onlyb (b))`)

		var args []any
		if _, ok := f.(*webhookFeedFactory); ok {
			args = append(args, optOutOfMetamorphicEnrichedEnvelope{reason: "metamorphic enriched envelope does not support column families for webhook sinks"})
		}
		testFeed := feed(t, f, `CREATE CHANGEFEED FOR foo FAMILY onlya, foo FAMILY onlyb`, args...)
		defer closeFeed(t, testFeed)

		feed, ok := testFeed.(cdctest.EnterpriseTestFeed)
		require.True(t, ok)

		sqlDB.Exec(t, `PAUSE JOB $1`, feed.JobID())
		waitForJobState(sqlDB, t, feed.JobID(), `paused`)

		sqlDB.Exec(t, fmt.Sprintf(`ALTER CHANGEFEED %d DROP foo FAMILY onlyb`, feed.JobID()))

		sqlDB.Exec(t, fmt.Sprintf(`RESUME JOB %d`, feed.JobID()))
		waitForJobState(sqlDB, t, feed.JobID(), `running`)

		sqlDB.Exec(t, `INSERT INTO foo VALUES(1, 'hello')`)
		sqlDB.Exec(t, `INSERT INTO foo VALUES(2, 'goodbye')`)
		assertPayloads(t, testFeed, []string{
			`foo.onlya: [1]->{"after": {"a": 1}}`,
			`foo.onlya: [2]->{"after": {"a": 2}}`,
		})

	}

	cdcTest(t, testFn, feedTestEnterpriseSinks, feedTestNoExternalConnection)
}

func TestAlterChangefeedSetDiffOption(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)

		testFeed := feed(t, f, `CREATE CHANGEFEED FOR foo with format='json'`)
		defer closeFeed(t, testFeed)

		feed, ok := testFeed.(cdctest.EnterpriseTestFeed)
		require.True(t, ok)

		sqlDB.Exec(t, `PAUSE JOB $1`, feed.JobID())
		waitForJobState(sqlDB, t, feed.JobID(), `paused`)

		sqlDB.Exec(t, fmt.Sprintf(`ALTER CHANGEFEED %d SET diff`, feed.JobID()))

		sqlDB.Exec(t, fmt.Sprintf(`RESUME JOB %d`, feed.JobID()))
		waitForJobState(sqlDB, t, feed.JobID(), `running`)

		sqlDB.Exec(t, `INSERT INTO foo VALUES (0, 'initial')`)
		assertPayloads(t, testFeed, []string{
			`foo: [0]->{"after": {"a": 0, "b": "initial"}, "before": null}`,
		})
	}

	cdcTest(t, testFn, feedTestEnterpriseSinks, feedTestNoExternalConnection)
}

func TestAlterChangefeedRespectsCDCQuery(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)

		const cdcQuery = "SELECT * FROM foo WHERE a % 2 = 0"
		testFeed := feed(t, f, "CREATE CHANGEFEED WITH format='json', envelope='wrapped' AS "+cdcQuery)
		defer closeFeed(t, testFeed)

		feed, ok := testFeed.(cdctest.EnterpriseTestFeed)
		require.True(t, ok)

		sqlDB.Exec(t, `PAUSE JOB $1`, feed.JobID())
		waitForJobState(sqlDB, t, feed.JobID(), `paused`)

		sqlDB.Exec(t, fmt.Sprintf(`ALTER CHANGEFEED %d SET diff`, feed.JobID()))
		registry := s.Server.JobRegistry().(*jobs.Registry)
		job, err := registry.LoadJob(context.Background(), feed.JobID())
		require.NoError(t, err)
		details, ok := job.Details().(jobspb.ChangefeedDetails)
		require.True(t, ok)

		// Verify the query still intact.
		sc, err := cdceval.ParseChangefeedExpression(cdcQuery)
		require.NoError(t, err)
		require.Equal(t, cdceval.AsStringUnredacted(sc), details.Select)

		// Addition/Removal of the tables is not supported yet.
		t.Run("cannot add or drop", func(t *testing.T) {
			sqlDB.ExpectErr(t, "cannot modify targets when using CDC query changefeed",
				fmt.Sprintf(`ALTER CHANGEFEED %d ADD blah SET DIFF`, feed.JobID()))
			sqlDB.ExpectErr(t, "cannot modify targets when using CDC query changefeed",
				fmt.Sprintf(`ALTER CHANGEFEED %d DROP blah SET DIFF`, feed.JobID()))
		})
	}

	cdcTest(t, testFn, feedTestEnterpriseSinks, feedTestNoExternalConnection)
}

func TestAlterChangefeedUnsetDiffOption(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)

		testFeed := feed(t, f, `CREATE CHANGEFEED FOR foo WITH diff`)
		defer closeFeed(t, testFeed)

		feed, ok := testFeed.(cdctest.EnterpriseTestFeed)
		require.True(t, ok)

		sqlDB.Exec(t, `PAUSE JOB $1`, feed.JobID())
		waitForJobState(sqlDB, t, feed.JobID(), `paused`)

		sqlDB.Exec(t, fmt.Sprintf(`ALTER CHANGEFEED %d UNSET diff`, feed.JobID()))

		sqlDB.Exec(t, fmt.Sprintf(`RESUME JOB %d`, feed.JobID()))
		waitForJobState(sqlDB, t, feed.JobID(), `running`)

		sqlDB.Exec(t, `INSERT INTO foo VALUES (0, 'initial')`)
		assertPayloads(t, testFeed, []string{
			`foo: [0]->{"after": {"a": 0, "b": "initial"}}`,
		})
	}

	// TODO: Figure out why this fails on other sinks
	cdcTest(t, testFn, feedTestForceSink("kafka"), feedTestNoExternalConnection)
}

func TestAlterChangefeedErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY)`)
		sqlDB.Exec(t, `CREATE TABLE bar (a INT PRIMARY KEY)`)
		testFeed := feed(t, f, `CREATE CHANGEFEED FOR foo`, optOutOfMetamorphicDBLevelChangefeed{
			reason: "db level changefeeds don't support ADD/DROP TARGETS in ALTER CHANGEFEEDs",
		})
		defer closeFeed(t, testFeed)

		feed, ok := testFeed.(cdctest.EnterpriseTestFeed)
		require.True(t, ok)

		sqlDB.ExpectErr(t,
			`could not load job with job id -1`,
			`ALTER CHANGEFEED -1 ADD bar`,
		)

		sqlDB.Exec(t, `ALTER TABLE bar ADD COLUMN b INT`)
		var alterTableJobID jobspb.JobID
		sqlDB.QueryRow(t, `SELECT job_id FROM [SHOW JOBS] WHERE job_type = 'NEW SCHEMA CHANGE'`).Scan(&alterTableJobID)
		sqlDB.ExpectErr(t,
			fmt.Sprintf(`job %d is not changefeed job`, alterTableJobID),
			fmt.Sprintf(`ALTER CHANGEFEED %d ADD bar`, alterTableJobID),
		)

		sqlDB.ExpectErr(t,
			fmt.Sprintf(`job %d is not paused`, feed.JobID()),
			fmt.Sprintf(`ALTER CHANGEFEED %d ADD bar`, feed.JobID()),
		)

		sqlDB.Exec(t, `PAUSE JOB $1`, feed.JobID())
		waitForJobState(sqlDB, t, feed.JobID(), `paused`)

		sqlDB.ExpectErr(t,
			`pq: target "TABLE baz" does not exist`,
			fmt.Sprintf(`ALTER CHANGEFEED %d ADD baz`, feed.JobID()),
		)
		sqlDB.ExpectErr(t,
			`pq: target "TABLE baz" does not exist`,
			fmt.Sprintf(`ALTER CHANGEFEED %d DROP baz`, feed.JobID()),
		)
		sqlDB.ExpectErr(t,
			`pq: target "TABLE bar" already not watched by changefeed`,
			fmt.Sprintf(`ALTER CHANGEFEED %d DROP bar`, feed.JobID()),
		)
		sqlDB.ExpectErr(t,
			`pq: invalid option "qux"`,
			fmt.Sprintf(`ALTER CHANGEFEED %d SET qux`, feed.JobID()),
		)
		sqlDB.ExpectErr(t,
			`pq: cannot alter option "initial_scan"`,
			fmt.Sprintf(`ALTER CHANGEFEED %d SET initial_scan`, feed.JobID()),
		)
		sqlDB.ExpectErr(t,
			`pq: invalid option "qux"`,
			fmt.Sprintf(`ALTER CHANGEFEED %d UNSET qux`, feed.JobID()),
		)
		sqlDB.ExpectErr(t,
			`pq: cannot alter option "initial_scan"`,
			fmt.Sprintf(`ALTER CHANGEFEED %d UNSET initial_scan`, feed.JobID()),
		)
		sqlDB.ExpectErr(t,
			`pq: cannot alter option "initial_scan_only"`,
			fmt.Sprintf(`ALTER CHANGEFEED %d UNSET initial_scan_only`, feed.JobID()),
		)
		sqlDB.ExpectErr(t,
			`pq: cannot alter option "end_time"`,
			fmt.Sprintf(`ALTER CHANGEFEED %d UNSET end_time`, feed.JobID()),
		)

		sqlDB.ExpectErr(t,
			`cannot unset option "sink"`,
			fmt.Sprintf(`ALTER CHANGEFEED %d UNSET sink`, feed.JobID()),
		)

		sqlDB.ExpectErr(t,
			`pq: invalid option "diff"`,
			fmt.Sprintf(`ALTER CHANGEFEED %d ADD bar WITH diff`, feed.JobID()),
		)

		sqlDB.ExpectErr(t,
			`pq: cannot specify both "initial_scan" and "no_initial_scan"`,
			fmt.Sprintf(`ALTER CHANGEFEED %d ADD bar WITH initial_scan, no_initial_scan`, feed.JobID()),
		)

		sqlDB.ExpectErr(t, "pq: changefeed ID must be an INT value: subqueries are not allowed in cdc",
			"ALTER CHANGEFEED (SELECT 1) ADD bar")
		sqlDB.ExpectErr(t, "pq: changefeed ID must be an INT value: could not parse \"two\" as type int",
			"ALTER CHANGEFEED 'two' ADD bar")
	}

	cdcTest(t, testFn, feedTestEnterpriseSinks, feedTestNoExternalConnection)
}

func TestAlterChangefeedDropAllTargetsError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY)`)
		sqlDB.Exec(t, `CREATE TABLE bar (a INT PRIMARY KEY)`)

		testFeed := feed(t, f, `CREATE CHANGEFEED FOR foo, bar`, optOutOfMetamorphicDBLevelChangefeed{
			reason: "db level changefeeds don't support ADD/DROP TARGETS in ALTER CHANGEFEEDs",
		})
		defer closeFeed(t, testFeed)

		feed, ok := testFeed.(cdctest.EnterpriseTestFeed)
		require.True(t, ok)

		sqlDB.Exec(t, `PAUSE JOB $1`, feed.JobID())
		waitForJobState(sqlDB, t, feed.JobID(), `paused`)

		sqlDB.ExpectErr(t,
			`cannot drop all targets`,
			fmt.Sprintf(`ALTER CHANGEFEED %d DROP foo, bar`, feed.JobID()),
		)
	}

	cdcTest(t, testFn, feedTestEnterpriseSinks, feedTestNoExternalConnection)
}

func TestAlterChangefeedTelemetry(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY)`)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (1)`)
		sqlDB.Exec(t, `CREATE TABLE bar (a INT PRIMARY KEY)`)
		sqlDB.Exec(t, `INSERT INTO bar VALUES (1)`)
		sqlDB.Exec(t, `CREATE TABLE baz (a INT PRIMARY KEY)`)
		sqlDB.Exec(t, `INSERT INTO baz VALUES (1)`)

		// Reset the counts.
		_ = telemetry.GetFeatureCounts(telemetry.Raw, telemetry.ResetCounts)

		testFeed := feed(t, f, `CREATE CHANGEFEED FOR foo, bar WITH diff`, optOutOfMetamorphicDBLevelChangefeed{
			reason: "db level changefeeds don't support ADD/DROP TARGETS in ALTER CHANGEFEEDs",
		})
		defer closeFeed(t, testFeed)
		feed := testFeed.(cdctest.EnterpriseTestFeed)

		require.NoError(t, feed.Pause())

		// The job system clears the lease asyncronously after
		// cancellation. This lease clearing transaction can
		// cause a restart in the alter changefeed
		// transaction, which will lead to different feature
		// counter counts. Thus, we want to wait for the lease
		// clear. However, the lease clear isn't guaranteed to
		// happen, so we only wait a few seconds for it.
		waitForNoLease := func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			for {
				if ctx.Err() != nil {
					return
				}
				var sessionID []byte
				sqlDB.QueryRow(t, `SELECT claim_session_id FROM system.jobs WHERE id = $1`, feed.JobID()).Scan(&sessionID)
				if sessionID == nil {
					return
				}
				time.Sleep(250 * time.Millisecond)
			}
		}

		waitForNoLease()
		sqlDB.Exec(t, fmt.Sprintf(`ALTER CHANGEFEED %d DROP bar, foo ADD baz UNSET diff SET resolved, format=json`, feed.JobID()))

		counts := telemetry.GetFeatureCounts(telemetry.Raw, telemetry.ResetCounts)
		require.Equal(t, int32(1), counts[`changefeed.alter`])
		require.Equal(t, int32(1), counts[`changefeed.alter.dropped_targets.2`])
		require.Equal(t, int32(1), counts[`changefeed.alter.added_targets.1`])
		require.Equal(t, int32(1), counts[`changefeed.alter.set_options.2`])
		require.Equal(t, int32(1), counts[`changefeed.alter.unset_options.1`])
	}

	cdcTest(t, testFn, feedTestEnterpriseSinks, feedTestNoExternalConnection)
}

// The purpose of this test is to ensure that the ALTER CHANGEFEED statement
// does not accidentally redact secret keys in the changefeed details
func TestAlterChangefeedPersistSinkURI(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const unredactedSinkURI = "null://blah?AWS_ACCESS_KEY_ID=the_secret"

	ctx := context.Background()
	srv, rawSQLDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
		},
	})
	defer srv.Stopper().Stop(ctx)

	s := srv.ApplicationLayer()
	sqlDB := sqlutils.MakeSQLRunner(rawSQLDB)
	registry := s.JobRegistry().(*jobs.Registry)

	for _, l := range []serverutils.ApplicationLayerInterface{s, srv.SystemLayer()} {
		kvserver.RangefeedEnabled.Override(ctx, &l.ClusterSettings().SV, true)
	}

	query := `CREATE TABLE foo (a string)`
	sqlDB.Exec(t, query)

	query = `CREATE TABLE bar (b string)`
	sqlDB.Exec(t, query)

	var changefeedID jobspb.JobID

	doneCh := make(chan struct{})
	defer close(doneCh)
	registry.TestingWrapResumerConstructor(jobspb.TypeChangefeed,
		func(raw jobs.Resumer) jobs.Resumer {
			r := fakeResumer{
				done: doneCh,
			}
			return &r
		})

	sqlDB.QueryRow(t, `CREATE CHANGEFEED FOR TABLE foo, bar INTO $1`, unredactedSinkURI).Scan(&changefeedID)

	sqlDB.Exec(t, `PAUSE JOB $1`, changefeedID)
	waitForJobState(sqlDB, t, changefeedID, `paused`)

	sqlDB.Exec(t, fmt.Sprintf(`ALTER CHANGEFEED %d SET diff`, changefeedID))

	sqlDB.Exec(t, fmt.Sprintf(`RESUME JOB %d`, changefeedID))
	waitForJobState(sqlDB, t, changefeedID, `running`)

	job, err := registry.LoadJob(ctx, changefeedID)
	require.NoError(t, err)
	details, ok := job.Details().(jobspb.ChangefeedDetails)
	require.True(t, ok)

	require.Equal(t, unredactedSinkURI, details.SinkURI)
}

func TestAlterChangefeedChangeSinkTypeError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)

		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)

		testFeed := feed(t, f, `CREATE CHANGEFEED FOR foo`)
		defer closeFeed(t, testFeed)

		feed, ok := testFeed.(cdctest.EnterpriseTestFeed)
		require.True(t, ok)

		sqlDB.Exec(t, `PAUSE JOB $1`, feed.JobID())
		waitForJobState(sqlDB, t, feed.JobID(), `paused`)

		sqlDB.ExpectErr(t,
			`pq: New sink type "null" does not match original sink type "kafka". Altering the sink type of a changefeed is disallowed, consider creating a new changefeed instead.`,
			fmt.Sprintf(`ALTER CHANGEFEED %d SET sink = 'null://'`, feed.JobID()),
		)
	}

	cdcTest(t, testFn, feedTestForceSink("kafka"), feedTestNoExternalConnection)
}

func TestAlterChangefeedChangeSinkURI(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		registry := s.Server.JobRegistry().(*jobs.Registry)
		ctx := context.Background()

		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)

		testFeed := feed(t, f, `CREATE CHANGEFEED FOR foo`)
		defer closeFeed(t, testFeed)

		feed, ok := testFeed.(cdctest.EnterpriseTestFeed)
		require.True(t, ok)

		sqlDB.Exec(t, `PAUSE JOB $1`, feed.JobID())
		waitForJobState(sqlDB, t, feed.JobID(), `paused`)

		newSinkURI := `kafka://new_kafka_uri`

		sqlDB.Exec(t, fmt.Sprintf(`ALTER CHANGEFEED %d SET sink = '%s'`, feed.JobID(), newSinkURI))

		sqlDB.Exec(t, fmt.Sprintf(`RESUME JOB %d`, feed.JobID()))
		waitForJobState(sqlDB, t, feed.JobID(), `running`)

		job, err := registry.LoadJob(ctx, feed.JobID())
		require.NoError(t, err)
		details, ok := job.Details().(jobspb.ChangefeedDetails)
		require.True(t, ok)

		require.Equal(t, newSinkURI, details.SinkURI)
	}

	// TODO (zinger): Decide how this functionality should interact with external connections
	// and add a test for it.
	cdcTest(t, testFn, feedTestForceSink("kafka"), feedTestNoExternalConnection)
}

func TestAlterChangefeedAddTargetErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY)`)
		sqlDB.Exec(t, `INSERT INTO foo (a) SELECT * FROM generate_series(1, 1000)`)

		knobs := s.TestingKnobs.
			DistSQL.(*execinfra.TestingKnobs).
			Changefeed.(*TestingKnobs)

		// Ensure Scan Requests are always small enough that we receive multiple
		// resolved events during a backfill
		knobs.FeedKnobs.BeforeScanRequest = func(b *kv.Batch) error {
			b.Header.MaxSpanRequestKeys = 10
			return nil
		}

		// ensure that we do not emit a resolved timestamp
		knobs.FilterSpanWithMutation = func(r *jobspb.ResolvedSpan) (bool, error) {
			return true, nil
		}

		testFeed := feed(t, f, `CREATE CHANGEFEED FOR foo WITH resolved = '100ms'`,
			optOutOfMetamorphicDBLevelChangefeed{
				reason: "db level changefeeds don't support ADD/DROP TARGETS in ALTER CHANGEFEEDs",
			})

		// Kafka feeds are not buffered, so we have to consume messages.
		g := ctxgroup.WithContext(context.Background())
		g.Go(func() error {
			for {
				_, err := testFeed.Next()
				if err != nil {
					return err
				}
			}
		})
		defer func() {
			closeFeed(t, testFeed)
			_ = g.Wait()
		}()

		feed, ok := testFeed.(cdctest.EnterpriseTestFeed)
		require.True(t, ok)

		require.NoError(t, feed.Pause())
		waitForJobState(sqlDB, t, feed.JobID(), `paused`)

		sqlDB.Exec(t, `CREATE TABLE bar (a INT PRIMARY KEY)`)
		sqlDB.Exec(t, `INSERT INTO bar VALUES (1), (2), (3)`)
		sqlDB.ExpectErr(t,
			`pq: target "bar" cannot be resolved as of the resume time .+\. `+
				`Please wait until the high water mark progresses past the creation `+
				`time of this target in order to add it to the changefeed.`,
			fmt.Sprintf(`ALTER CHANGEFEED %d ADD bar`, feed.JobID()),
		)

		// allow the changefeed to emit resolved events now
		knobs.FilterSpanWithMutation = func(r *jobspb.ResolvedSpan) (bool, error) {
			return false, nil
		}

		require.NoError(t, feed.Resume())

		// Wait for the high water mark to be non-zero.
		testutils.SucceedsSoon(t, func() error {
			registry := s.Server.JobRegistry().(*jobs.Registry)
			job, err := registry.LoadJob(context.Background(), feed.JobID())
			require.NoError(t, err)
			prog := job.Progress()
			if p := prog.GetHighWater(); p != nil && !p.IsEmpty() {
				return nil
			}
			return errors.New("waiting for highwater")
		})

		require.NoError(t, feed.Pause())
		waitForJobState(sqlDB, t, feed.JobID(), `paused`)

		sqlDB.Exec(t, `CREATE TABLE baz (a INT PRIMARY KEY)`)
		sqlDB.Exec(t, `INSERT INTO baz VALUES (1), (2), (3)`)

		sqlDB.ExpectErr(t,
			`pq: target "baz" cannot be resolved as of the resume time .+\. `+
				`Please wait until the high water mark progresses past the creation `+
				`time of this target in order to add it to the changefeed.`,
			fmt.Sprintf(`ALTER CHANGEFEED %d ADD baz`, feed.JobID()),
		)
	}

	cdcTest(t, testFn, feedTestEnterpriseSinks, feedTestNoExternalConnection)
}

func TestAlterChangefeedDatabaseQualifiedNames(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE d.drivers (id INT PRIMARY KEY, name STRING)`)
		sqlDB.Exec(t, `CREATE TABLE d.users (id INT PRIMARY KEY, name STRING)`)
		sqlDB.Exec(t, `INSERT INTO d.drivers VALUES (1, 'Alice')`)
		sqlDB.Exec(t, `INSERT INTO d.users VALUES (1, 'Bob')`)
		testFeed := feed(t, f, `CREATE CHANGEFEED FOR d.drivers WITH resolved = '100ms', diff`,
			optOutOfMetamorphicDBLevelChangefeed{
				reason: "db level changefeeds don't support ADD/DROP TARGETS in ALTER CHANGEFEEDs",
			})
		defer closeFeed(t, testFeed)

		assertPayloads(t, testFeed, []string{
			`drivers: [1]->{"after": {"id": 1, "name": "Alice"}, "before": null}`,
		})

		expectResolvedTimestamp(t, testFeed)

		feed, ok := testFeed.(cdctest.EnterpriseTestFeed)
		require.True(t, ok)

		require.NoError(t, feed.Pause())

		sqlDB.Exec(t, fmt.Sprintf(`ALTER CHANGEFEED %d ADD d.users WITH initial_scan UNSET diff`, feed.JobID()))

		require.NoError(t, feed.Resume())

		assertPayloads(t, testFeed, []string{
			`users: [1]->{"after": {"id": 1, "name": "Bob"}}`,
		})

		sqlDB.Exec(t, `INSERT INTO d.drivers VALUES (3, 'Carol')`)

		assertPayloads(t, testFeed, []string{
			`drivers: [3]->{"after": {"id": 3, "name": "Carol"}}`,
		})
	}

	cdcTest(t, testFn, feedTestEnterpriseSinks, feedTestNoExternalConnection)
}

func TestAlterChangefeedDatabaseScope(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE DATABASE movr`)
		sqlDB.Exec(t, `CREATE DATABASE new_movr`)

		sqlDB.Exec(t, `CREATE TABLE movr.drivers (id INT PRIMARY KEY, name STRING)`)
		sqlDB.Exec(t, `CREATE TABLE new_movr.drivers (id INT PRIMARY KEY, name STRING)`)

		sqlDB.Exec(t,
			`INSERT INTO movr.drivers VALUES (1, 'Alice')`,
		)
		sqlDB.Exec(t,
			`INSERT INTO new_movr.drivers VALUES (1, 'Bob')`,
		)

		testFeed := feed(t, f, `CREATE CHANGEFEED FOR movr.drivers WITH diff`,
			optOutOfMetamorphicDBLevelChangefeed{
				reason: "changefeed watches tables not in the default database",
			},
		)
		defer closeFeed(t, testFeed)

		assertPayloads(t, testFeed, []string{
			`drivers: [1]->{"after": {"id": 1, "name": "Alice"}, "before": null}`,
		})

		feed, ok := testFeed.(cdctest.EnterpriseTestFeed)
		require.True(t, ok)

		require.NoError(t, feed.Pause())

		sqlDB.Exec(t, `USE new_movr`)

		sqlDB.Exec(t, fmt.Sprintf(`ALTER CHANGEFEED %d DROP movr.drivers ADD drivers WITH initial_scan UNSET diff`, feed.JobID()))

		require.NoError(t, feed.Resume())

		assertPayloads(t, testFeed, []string{
			`drivers: [1]->{"after": {"id": 1, "name": "Bob"}}`,
		})
	}

	cdcTest(t, testFn, feedTestEnterpriseSinks, feedTestNoExternalConnection, feedTestUseRootUserConnection)
}

// TestAlterChangefeedDatabaseLevelChangefeedFilters tests that we can alter a
// database level changefeed to set/unset include/exclude filters. We do not
// create or drop any tables in this test once we create the changefeed so that
// it doesn't depend on the table watcher functionality.
func TestAlterChangefeedDatabaseLevelChangefeedFilters(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// The cases in this test will either a) create a changefeed with no filter,
	// and then remove the filter with an ALTER CHANGEFEED, or b) create a
	// changefeed with a filter and then remove the filter with an `ALTER
	// CHANGEFEED UNSET (INCLUDE|EXCLUDE)` statement.
	// expectActiveFilter is true if we expect there to be an active filter at
	// the end of the test (case a) or false if we expect there to be no active
	// filter (case b) at the end of the test when we make out assertions.
	type filterTestCase struct {
		name               string
		createStmt         string
		alterStmt          string
		expectActiveFilter bool
	}

	testCases := []filterTestCase{
		{
			name:               "set_exclude",
			createStmt:         "CREATE CHANGEFEED FOR DATABASE d",
			alterStmt:          "SET EXCLUDE TABLES foo",
			expectActiveFilter: true,
		},
		{
			name:               "set_include",
			createStmt:         "CREATE CHANGEFEED FOR DATABASE d",
			alterStmt:          "SET INCLUDE TABLES bar",
			expectActiveFilter: true,
		},
		{
			name:               "unset_include",
			createStmt:         "CREATE CHANGEFEED FOR DATABASE d INCLUDE TABLES bar",
			alterStmt:          "UNSET INCLUDE TABLES",
			expectActiveFilter: false,
		},
		{
			name:               "unset_exclude",
			createStmt:         "CREATE CHANGEFEED FOR DATABASE d EXCLUDE TABLES foo",
			alterStmt:          "UNSET EXCLUDE TABLES",
			expectActiveFilter: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
				sqlDB := sqlutils.MakeSQLRunner(s.DB)
				sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
				sqlDB.Exec(t, `CREATE TABLE bar (a INT PRIMARY KEY, b STRING)`)

				foo := feed(t, f, tc.createStmt)
				defer closeFeed(t, foo)

				jobID := foo.(cdctest.EnterpriseTestFeed).JobID()

				sqlDB.Exec(t, `PAUSE JOB $1`, jobID)
				waitForJobState(sqlDB, t, jobID, `paused`)

				sqlDB.Exec(t, fmt.Sprintf(`ALTER CHANGEFEED %d %s`, jobID, tc.alterStmt))

				sqlDB.Exec(t, `RESUME JOB $1`, jobID)
				waitForJobState(sqlDB, t, jobID, `running`)

				// This event will be filtered out in the case where our ALTER
				// statement SET a filter, but will be emitted in the case
				// where our ALTER statement UNSET a filter.
				sqlDB.Exec(t, `INSERT INTO foo VALUES (0, 'initial')`)

				// These events should always be emitted.
				sqlDB.Exec(t, `INSERT INTO bar VALUES (0, 'initial')`)
				sqlDB.Exec(t, `UPSERT INTO bar VALUES (0, 'updated')`)

				expectedPayloads := []string{
					`bar: [0]->{"after": {"a": 0, "b": "initial"}}`,
					`bar: [0]->{"after": {"a": 0, "b": "updated"}}`,
				}
				if !tc.expectActiveFilter {
					expectedPayloads = append([]string{`foo: [0]->{"after": {"a": 0, "b": "initial"}}`}, expectedPayloads...)
				}

				assertPayloads(t, foo, expectedPayloads)
			}
			cdcTest(t, testFn, feedTestEnterpriseSinks)
		})
	}
}

// TestAlterChangefeedDatabaseLevelChangefeedDoesntRemoveFilter tests that we don't
// remove the filter when we do an ALTER CHANGEFEED that doesn't explicitly
// set or unset a filter.
func TestAlterChangefeedDatabaseLevelChangefeedDoesntRemoveFilter(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
		sqlDB.Exec(t, `CREATE TABLE bar (a INT PRIMARY KEY, b STRING)`)

		foo := feed(t, f, `CREATE CHANGEFEED FOR DATABASE d EXCLUDE TABLES foo`)
		defer closeFeed(t, foo)

		jobID := foo.(cdctest.EnterpriseTestFeed).JobID()

		sqlDB.Exec(t, `PAUSE JOB $1`, jobID)
		waitForJobState(sqlDB, t, jobID, `paused`)

		sqlDB.Exec(t, fmt.Sprintf(`ALTER CHANGEFEED %d SET DIFF`, jobID))

		sqlDB.Exec(t, `RESUME JOB $1`, jobID)
		waitForJobState(sqlDB, t, jobID, `running`)

		// This event should still be excluded.
		sqlDB.Exec(t, `INSERT INTO foo VALUES (0, 'initial')`)

		// These events should be emitted with diff.
		sqlDB.Exec(t, `INSERT INTO bar VALUES (0, 'initial')`)
		sqlDB.Exec(t, `UPSERT INTO bar VALUES (0, 'updated')`)

		assertPayloads(t, foo, []string{
			`bar: [0]->{"after": {"a": 0, "b": "initial"}, "before": null}`,
			`bar: [0]->{"after": {"a": 0, "b": "updated"}, "before": {"a": 0, "b": "initial"}}`,
		})
	}

	cdcTest(t, testFn, feedTestEnterpriseSinks)
}

// TestAlterChangefeedDatabaseLevelChangefeedRemainsDatabaseLevel tests that we
// don't silently convert a database level changefeed to a table level changefeed
// when we do an unrelated alter.
func TestAlterChangefeedDatabaseLevelChangefeedRemainsDatabaseLevel(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
		sqlDB.Exec(t, `CREATE TABLE bar (a INT PRIMARY KEY, b STRING)`)

		foo := feed(t, f, `CREATE CHANGEFEED FOR DATABASE d EXCLUDE TABLES foo`)
		defer closeFeed(t, foo)

		jobID := foo.(cdctest.EnterpriseTestFeed).JobID()

		sqlDB.Exec(t, `PAUSE JOB $1`, jobID)
		waitForJobState(sqlDB, t, jobID, `paused`)

		sqlDB.Exec(t, fmt.Sprintf(`ALTER CHANGEFEED %d SET DIFF`, jobID))
		// This should not fail, which it would if the changefeeed were
		// silently converted to a table-level changefeed.
		sqlDB.Exec(t, fmt.Sprintf(`ALTER CHANGEFEED %d UNSET EXCLUDE TABLES`, jobID))

		sqlDB.Exec(t, `RESUME JOB $1`, jobID)
		waitForJobState(sqlDB, t, jobID, `running`)

		sqlDB.Exec(t, `INSERT INTO foo VALUES (0, 'initial')`)
		sqlDB.Exec(t, `INSERT INTO bar VALUES (0, 'initial')`)
		sqlDB.Exec(t, `UPSERT INTO bar VALUES (0, 'updated')`)

		assertPayloads(t, foo, []string{
			`foo: [0]->{"after": {"a": 0, "b": "initial"}, "before": null}`,
			`bar: [0]->{"after": {"a": 0, "b": "initial"}, "before": null}`,
			`bar: [0]->{"after": {"a": 0, "b": "updated"}, "before": {"a": 0, "b": "initial"}}`,
		})
	}

	cdcTest(t, testFn, feedTestEnterpriseSinks)
}

// TestAlterChangefeedDatabaseLevelChangefeedDiff tests that we can alter a
// database level changefeed to turn on the diff option. We do not create or
// drop any tables in this test once we create the changefeed so that it doesn't
// depend on the table watcher functionality.
func TestAlterChangefeedDatabaseLevelChangefeedDiff(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
		sqlDB.Exec(t, `CREATE TABLE bar (a INT PRIMARY KEY, b STRING)`)

		foo := feed(t, f, `CREATE CHANGEFEED FOR DATABASE d WITH initial_scan='no'`)
		defer closeFeed(t, foo)

		jobID := foo.(cdctest.EnterpriseTestFeed).JobID()

		sqlDB.Exec(t, `PAUSE JOB $1`, jobID)
		waitForJobState(sqlDB, t, jobID, `paused`)

		sqlDB.Exec(t, fmt.Sprintf(`ALTER CHANGEFEED %d SET DIFF`, jobID))

		sqlDB.Exec(t, `RESUME JOB $1`, jobID)
		waitForJobState(sqlDB, t, jobID, `running`)

		sqlDB.Exec(t, `INSERT INTO foo VALUES (0, 'initial')`)
		sqlDB.Exec(t, `INSERT INTO bar VALUES (0, 'initial')`)
		sqlDB.Exec(t, `UPSERT INTO bar VALUES (0, 'updated')`)

		assertPayloads(t, foo, []string{
			`foo: [0]->{"after": {"a": 0, "b": "initial"}, "before": null}`,
			`bar: [0]->{"after": {"a": 0, "b": "initial"}, "before": null}`,
			`bar: [0]->{"after": {"a": 0, "b": "updated"}, "before": {"a": 0, "b": "initial"}}`,
		})
	}

	cdcTest(t, testFn, feedTestEnterpriseSinks)
}

// TestAlterChangefeedDatabaseLevelChangefeedFailsOnTargetAlter tests that
// ADD and DROP operations fail on database level changefeeds.
func TestAlterChangefeedDatabaseLevelChangefeedFailsOnTargetAlter(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	for _, op := range []string{"ADD", "DROP"} {
		testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
			sqlDB := sqlutils.MakeSQLRunner(s.DB)
			sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
			sqlDB.Exec(t, `CREATE TABLE bar (a INT PRIMARY KEY, b STRING)`)

			foo := feed(t, f, `CREATE CHANGEFEED FOR DATABASE d`)
			defer closeFeed(t, foo)

			jobID := foo.(cdctest.EnterpriseTestFeed).JobID()

			sqlDB.Exec(t, `PAUSE JOB $1`, jobID)
			waitForJobState(sqlDB, t, jobID, `paused`)

			sqlDB.ExpectErr(
				t,
				"pq: cannot alter targets for a database level changefeed",
				fmt.Sprintf(`ALTER CHANGEFEED %d %s foo`, jobID, op),
			)
		}

		cdcTest(t, testFn, feedTestEnterpriseSinks)
	}
}

// TestAlterChangefeedDatabaseLevelChangefeedMultipleSetClauses tests that
// a single ALTER statement can include multiple SET clauses and all changes
// are respected.
func TestAlterChangefeedDatabaseLevelChangefeedMultipleSetClauses(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
		sqlDB.Exec(t, `CREATE TABLE bar (a INT PRIMARY KEY, b STRING)`)

		foo := feed(t, f, `CREATE CHANGEFEED FOR DATABASE d`)
		defer closeFeed(t, foo)

		jobID := foo.(cdctest.EnterpriseTestFeed).JobID()

		sqlDB.Exec(t, `PAUSE JOB $1`, jobID)
		waitForJobState(sqlDB, t, jobID, `paused`)

		// Alter with both SET diff and SET INCLUDE TABLES in a single statement
		sqlDB.Exec(t, fmt.Sprintf(`ALTER CHANGEFEED %d SET diff SET INCLUDE TABLES foo`, jobID))

		sqlDB.Exec(t, `RESUME JOB $1`, jobID)
		waitForJobState(sqlDB, t, jobID, `running`)

		// Insert into both tables
		sqlDB.Exec(t, `INSERT INTO foo VALUES (0, 'initial')`)
		sqlDB.Exec(t, `INSERT INTO bar VALUES (0, 'initial')`)
		sqlDB.Exec(t, `UPSERT INTO foo VALUES (0, 'updated')`)

		assertPayloads(t, foo, []string{
			`foo: [0]->{"after": {"a": 0, "b": "initial"}, "before": null}`,
			`foo: [0]->{"after": {"a": 0, "b": "updated"}, "before": {"a": 0, "b": "initial"}}`,
		})
	}

	cdcTest(t, testFn, feedTestEnterpriseSinks)
}

// TestAlterChangefeedDatabaseLevelChangefeedFilterSemantics tests various
// filter alteration behaviors including replacement and switching between
// INCLUDE/EXCLUDE modes.
func TestAlterChangefeedDatabaseLevelChangefeedFilterSemantics(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	type alterSpec struct {
		stmt string
		// When expectedErr is non-empty, this alter is expected to fail with this error.
		expectedErr string
	}

	type filterTestCase struct {
		name       string
		createStmt string
		alters     []alterSpec
		// expectPayloads maps a table name to whether payloads are expected after
		// the alter statement(s).
		expectPayloads map[string]bool
	}

	testCases := []filterTestCase{
		{
			name:       "exclude_replaces_exclude_single",
			createStmt: "CREATE CHANGEFEED FOR DATABASE d EXCLUDE TABLES foo",
			alters:     []alterSpec{{stmt: "SET EXCLUDE TABLES bar"}},
			// Altering the changefeed to exclude bar means that bar is the only
			// excluded table. foo is no longer excluded.
			expectPayloads: map[string]bool{"foo": true, "bar": false, "baz": true},
		},
		{
			name:           "exclude_replaces_with_multiple",
			createStmt:     "CREATE CHANGEFEED FOR DATABASE d EXCLUDE TABLES foo",
			alters:         []alterSpec{{stmt: "SET EXCLUDE TABLES foo, bar"}},
			expectPayloads: map[string]bool{"foo": false, "bar": false, "baz": true},
		},
		{
			name:       "include_replaces_include",
			createStmt: "CREATE CHANGEFEED FOR DATABASE d INCLUDE TABLES foo",
			alters:     []alterSpec{{stmt: "SET INCLUDE TABLES bar"}},
			// Altering the changefeed to include bar means that bar is the only
			// included table. foo is no longer included.
			expectPayloads: map[string]bool{"foo": false, "bar": true, "baz": false},
		},
		{
			// This test is the same as the previous one, except that SET INCLUDE
			// foo and SET INCLUDE bar are in a single ALTER CHANGEFEED statement.
			name:           "include_replaces_include_single_alter",
			createStmt:     "CREATE CHANGEFEED FOR DATABASE d",
			alters:         []alterSpec{{stmt: "SET INCLUDE TABLES foo SET INCLUDE TABLES bar"}},
			expectPayloads: map[string]bool{"foo": false, "bar": true, "baz": false},
		},
		{
			name:           "include_replaces_with_multiple",
			createStmt:     "CREATE CHANGEFEED FOR DATABASE d INCLUDE TABLES foo",
			alters:         []alterSpec{{stmt: "SET INCLUDE TABLES foo, bar"}},
			expectPayloads: map[string]bool{"foo": true, "bar": true, "baz": false},
		},
		{
			name:       "include_then_exclude_single_alter_fails",
			createStmt: "CREATE CHANGEFEED FOR DATABASE d",
			alters: []alterSpec{
				{
					stmt:        "SET INCLUDE TABLES foo SET EXCLUDE TABLES bar",
					expectedErr: "cannot alter filter type from INCLUDE to EXCLUDE",
				},
			},
		},
		{
			name:       "include_then_exclude_single_alter_fails_same_table",
			createStmt: "CREATE CHANGEFEED FOR DATABASE d",
			alters: []alterSpec{
				{
					stmt:        "SET INCLUDE TABLES foo SET EXCLUDE TABLES foo",
					expectedErr: "cannot alter filter type from INCLUDE to EXCLUDE",
				},
			},
		},
		{
			name:       "include_then_exclude_two_alters_fails",
			createStmt: "CREATE CHANGEFEED FOR DATABASE d",
			alters: []alterSpec{
				{stmt: "SET INCLUDE TABLES foo"},
				{
					stmt:        "SET EXCLUDE TABLES bar",
					expectedErr: "cannot alter filter type from INCLUDE to EXCLUDE",
				},
			},
		},
		{
			name:       "exclude_then_include_single_alter_fails",
			createStmt: "CREATE CHANGEFEED FOR DATABASE d",
			alters: []alterSpec{
				{
					stmt:        "SET EXCLUDE TABLES foo SET INCLUDE TABLES bar",
					expectedErr: "cannot alter filter type from EXCLUDE to INCLUDE",
				},
			},
		},
		{
			name:       "exclude_then_include_two_alters_fails",
			createStmt: "CREATE CHANGEFEED FOR DATABASE d",
			alters: []alterSpec{
				{stmt: "SET EXCLUDE TABLES foo"},
				{
					stmt:        "SET INCLUDE TABLES bar",
					expectedErr: "cannot alter filter type from EXCLUDE to INCLUDE",
				},
			},
		},
		{
			name:           "unset_include_when_no_filter_is_fine",
			createStmt:     "CREATE CHANGEFEED FOR DATABASE d",
			alters:         []alterSpec{{stmt: "UNSET INCLUDE TABLES"}},
			expectPayloads: map[string]bool{"foo": true, "bar": true, "baz": true},
		},
		{
			name:           "unset_exclude_when_no_filter_is_fine",
			createStmt:     "CREATE CHANGEFEED FOR DATABASE d",
			alters:         []alterSpec{{stmt: "UNSET EXCLUDE TABLES"}},
			expectPayloads: map[string]bool{"foo": true, "bar": true, "baz": true},
		},
		{
			name:       "set_include_then_unset_exclude_fails",
			createStmt: "CREATE CHANGEFEED FOR DATABASE d",
			alters: []alterSpec{
				{stmt: "SET INCLUDE TABLES foo"},
				{
					stmt:        "UNSET EXCLUDE TABLES",
					expectedErr: "cannot alter filter type from INCLUDE to EXCLUDE",
				},
			},
		},
		{
			name:       "unset_include_then_set_exclude_single_alter",
			createStmt: "CREATE CHANGEFEED FOR DATABASE d INCLUDE TABLES foo",
			alters:     []alterSpec{{stmt: "UNSET INCLUDE TABLES SET EXCLUDE TABLES bar"}},
			// UNSET INCLUDE clears the filter, then SET EXCLUDE should succeed
			// since the filter check passes when tables list is empty.
			expectPayloads: map[string]bool{"foo": true, "bar": false, "baz": true},
		},
		{
			name:           "unset_include_then_set_exclude",
			createStmt:     "CREATE CHANGEFEED FOR DATABASE d INCLUDE TABLES foo",
			alters:         []alterSpec{{stmt: "UNSET INCLUDE TABLES"}, {stmt: "SET EXCLUDE TABLES bar"}},
			expectPayloads: map[string]bool{"foo": true, "bar": false, "baz": true},
		},
		{
			name:       "set_exclude_then_unset_include_fails",
			createStmt: "CREATE CHANGEFEED FOR DATABASE d",
			alters: []alterSpec{
				{stmt: "SET EXCLUDE TABLES foo"},
				{
					stmt:        "UNSET INCLUDE TABLES",
					expectedErr: "cannot alter filter type from EXCLUDE to INCLUDE",
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
				sqlDB := sqlutils.MakeSQLRunner(s.DB)
				sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
				sqlDB.Exec(t, `CREATE TABLE bar (a INT PRIMARY KEY, b STRING)`)
				sqlDB.Exec(t, `CREATE TABLE baz (a INT PRIMARY KEY, b STRING)`)

				feed := feed(t, f, tc.createStmt)
				defer closeFeed(t, feed)

				jobID := feed.(cdctest.EnterpriseTestFeed).JobID()

				sqlDB.Exec(t, `PAUSE JOB $1`, jobID)
				waitForJobState(sqlDB, t, jobID, `paused`)

				for _, alter := range tc.alters {
					if alter.expectedErr != "" {
						sqlDB.ExpectErr(
							t,
							alter.expectedErr,
							fmt.Sprintf(`ALTER CHANGEFEED %d %s`, jobID, alter.stmt),
						)
						// If we expect an error, that's the end of the test.
						return
					}

					sqlDB.Exec(t, fmt.Sprintf(`ALTER CHANGEFEED %d %s`, jobID, alter.stmt))
				}

				sqlDB.Exec(t, `RESUME JOB $1`, jobID)
				waitForJobState(sqlDB, t, jobID, `running`)

				sqlDB.Exec(t, `INSERT INTO foo VALUES (0, 'foo')`)
				sqlDB.Exec(t, `INSERT INTO bar VALUES (0, 'bar')`)
				sqlDB.Exec(t, `INSERT INTO baz VALUES (0, 'baz')`)

				var expected []string
				for _, table := range []string{"foo", "bar", "baz"} {
					if tc.expectPayloads[table] {
						expectation := fmt.Sprintf(
							`%s: [0]->{"after": {"a": 0, "b": "%s"}}`, table, table,
						)
						expected = append(expected, expectation)
					}
				}

				assertPayloads(t, feed, expected)
			}

			cdcTest(t, testFn, feedTestEnterpriseSinks)
		})
	}
}

// TestAlterChangefeedFilterChangesDontEmitPreAlterEvents tests that if
// we alter a changefeed's filter options in a way that means new tables are
// being watched that we do not emit events for the new tables from before the
// ALTER CHANGEFEED statement.
func TestAlterChangefeedFilterChangesDontEmitPreAlterEvents(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
		sqlDB.Exec(t, `CREATE TABLE bar (a INT PRIMARY KEY, b STRING)`)

		// If the checkpoint advances, that will be the point we resume from for
		// all tables, including 'bar' skipping the pre-alter event. This makes
		// the test fail more consistently if we didn't add 'bar' to the frontier.
		knobs := s.TestingKnobs.
			DistSQL.(*execinfra.TestingKnobs).
			Changefeed.(*TestingKnobs)
		knobs.ShouldCheckpointToJobRecord = func(hw hlc.Timestamp) bool {
			return false
		}

		testFeed := feed(t, f, `CREATE CHANGEFEED FOR DATABASE d INCLUDE TABLES foo`)
		defer closeFeed(t, testFeed)

		sqlDB.Exec(t, `INSERT INTO foo VALUES (1, 'before alter')`)
		sqlDB.Exec(t, `INSERT INTO bar VALUES (1, 'before alter')`)

		assertPayloads(t, testFeed, []string{
			`foo: [1]->{"after": {"a": 1, "b": "before alter"}}`,
		})

		sqlDB.Exec(t, fmt.Sprintf(`PAUSE JOB %d`, testFeed.(cdctest.EnterpriseTestFeed).JobID()))
		waitForJobState(sqlDB, t, testFeed.(cdctest.EnterpriseTestFeed).JobID(), `paused`)

		sqlDB.Exec(t, fmt.Sprintf(`ALTER CHANGEFEED %d UNSET INCLUDE TABLES`,
			testFeed.(cdctest.EnterpriseTestFeed).JobID()))

		sqlDB.Exec(t, fmt.Sprintf(`RESUME JOB %d`, testFeed.(cdctest.EnterpriseTestFeed).JobID()))
		waitForJobState(sqlDB, t, testFeed.(cdctest.EnterpriseTestFeed).JobID(), `running`)

		sqlDB.Exec(t, `INSERT INTO foo VALUES (2, 'after alter')`)
		sqlDB.Exec(t, `INSERT INTO bar VALUES (2, 'after alter')`)

		// We should only see the event from bar that was inserted after the alter,
		// but we should see both events from foo. This test may falsely pass if
		// we "get lucky" and the pre-alter event for bar is the last one emitted.
		assertPayloads(t, testFeed, []string{
			`foo: [2]->{"after": {"a": 2, "b": "after alter"}}`,
			`bar: [2]->{"after": {"a": 2, "b": "after alter"}}`,
		})
	}

	cdcTest(t, testFn, feedTestEnterpriseSinks)
}

// TestAlterChangefeedFilterChangesWriteProgressForTablesThatWillBeAdded tests
// that if we alter a changefeed's filter to include a table added after the
// highwater mark, that we write progress for it, so that it starts from the
// ALTER CHANGEFEED statement time and not the table's creation time.
func TestAlterChangefeedFilterChangesWriteProgressForTablesThatWillBeAdded(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)

		knobs := s.TestingKnobs.
			DistSQL.(*execinfra.TestingKnobs).
			Changefeed.(*TestingKnobs)
		knobs.ShouldCheckpointToJobRecord = func(hw hlc.Timestamp) bool {
			return false
		}

		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
		testFeed := feed(t, f, `CREATE CHANGEFEED FOR DATABASE d INCLUDE TABLES foo`)
		defer closeFeed(t, testFeed)

		sqlDB.Exec(t, `INSERT INTO foo VALUES (1, 'before alter')`)
		sqlDB.Exec(t, `CREATE TABLE bar (a INT PRIMARY KEY, b STRING)`)
		sqlDB.Exec(t, `INSERT INTO bar VALUES (1, 'before alter')`)

		assertPayloads(t, testFeed, []string{
			`foo: [1]->{"after": {"a": 1, "b": "before alter"}}`,
		})

		sqlDB.Exec(t, fmt.Sprintf(`PAUSE JOB %d`, testFeed.(cdctest.EnterpriseTestFeed).JobID()))
		waitForJobState(sqlDB, t, testFeed.(cdctest.EnterpriseTestFeed).JobID(), `paused`)

		sqlDB.Exec(t, fmt.Sprintf(`ALTER CHANGEFEED %d UNSET INCLUDE TABLES`,
			testFeed.(cdctest.EnterpriseTestFeed).JobID()))

		// Once the tableset watcher is working, we can directly check that the
		// event for bar from before the alter does NOT get emitted, but an event
		// for bar from after the alter does.
		require.NoError(t, s.Server.InternalDB().(isql.DB).Txn(
			context.Background(),
			func(ctx context.Context, txn isql.Txn) error {
				spans, found, err := jobfrontier.GetResolvedSpans(
					ctx, txn, testFeed.(cdctest.EnterpriseTestFeed).JobID(), `alter_changefeed`,
				)
				if err != nil {
					return err
				}
				require.True(t, found)
				require.Equal(t, 1, len(spans))
				return nil
			},
		))
	}

	cdcTest(t, testFn, feedTestEnterpriseSinks)
}

// TestAlterChangefeedFilterChangesDontWriteProgressForTablesThatWillBeDropped
// tests that if a table is dropped between the highwater and the ALTER CHANGEFEED
// statement, that we do not store progress for it. This table will never be
// watched by the changefeed.
func TestAlterChangefeedFilterChangesDontWriteProgressForTablesThatWillBeDropped(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)

		knobs := s.TestingKnobs.
			DistSQL.(*execinfra.TestingKnobs).
			Changefeed.(*TestingKnobs)
		knobs.ShouldCheckpointToJobRecord = func(hw hlc.Timestamp) bool {
			return false
		}

		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
		sqlDB.Exec(t, `CREATE TABLE bar (a INT PRIMARY KEY, b STRING)`)
		testFeed := feed(t, f, `CREATE CHANGEFEED FOR DATABASE d EXCLUDE TABLES foo`)
		defer closeFeed(t, testFeed)

		sqlDB.Exec(t, `DROP TABLE foo`)

		sqlDB.Exec(t, fmt.Sprintf(`PAUSE JOB %d`, testFeed.(cdctest.EnterpriseTestFeed).JobID()))
		waitForJobState(sqlDB, t, testFeed.(cdctest.EnterpriseTestFeed).JobID(), `paused`)

		sqlDB.Exec(t, fmt.Sprintf(`ALTER CHANGEFEED %d UNSET EXCLUDE TABLES`,
			testFeed.(cdctest.EnterpriseTestFeed).JobID()))

		require.NoError(t, s.Server.InternalDB().(isql.DB).Txn(
			context.Background(),
			func(ctx context.Context, txn isql.Txn) error {
				_, found, err := jobfrontier.GetResolvedSpans(
					ctx, txn, testFeed.(cdctest.EnterpriseTestFeed).JobID(), `alter_changefeed`,
				)
				if err != nil {
					return err
				}
				// Writing this progress when we should not have will not effect
				// which events are emitted, so we have to check it directly.
				require.False(t, found)
				return nil
			},
		))
	}

	cdcTest(t, testFn, feedTestEnterpriseSinks)
}

// TestAlterChangefeedFilterChangesDontOverwriteProgress tests that if we alter
// a changefeed's filter options twice, we will not remove progress (the time we
// should start watching the new table) for other tables.
func TestAlterChangefeedFilterChangesDontOverwriteProgress(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)

		knobs := s.TestingKnobs.
			DistSQL.(*execinfra.TestingKnobs).
			Changefeed.(*TestingKnobs)
		knobs.ShouldCheckpointToJobRecord = func(hw hlc.Timestamp) bool {
			return false
		}

		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
		sqlDB.Exec(t, `CREATE TABLE bar (a INT PRIMARY KEY, b STRING)`)
		sqlDB.Exec(t, `CREATE TABLE baz (a INT PRIMARY KEY, b STRING)`)

		testFeed := feed(t, f, `CREATE CHANGEFEED FOR DATABASE d INCLUDE TABLES foo`)
		defer closeFeed(t, testFeed)

		sqlDB.Exec(t, `INSERT INTO foo VALUES (0, 'initial')`)
		sqlDB.Exec(t, `INSERT INTO bar VALUES (0, 'initial')`)
		sqlDB.Exec(t, `INSERT INTO baz VALUES (0, 'initial')`)

		sqlDB.Exec(t, fmt.Sprintf(`PAUSE JOB %d`, testFeed.(cdctest.EnterpriseTestFeed).JobID()))
		waitForJobState(sqlDB, t, testFeed.(cdctest.EnterpriseTestFeed).JobID(), `paused`)

		sqlDB.Exec(t, fmt.Sprintf(`ALTER CHANGEFEED %d SET INCLUDE TABLES foo, bar`,
			testFeed.(cdctest.EnterpriseTestFeed).JobID()))

		sqlDB.Exec(t, `INSERT INTO foo VALUES (1, 'after include bar')`)
		sqlDB.Exec(t, `INSERT INTO bar VALUES (1, 'after include bar')`)
		sqlDB.Exec(t, `INSERT INTO baz VALUES (1, 'after include bar')`)

		sqlDB.Exec(t, fmt.Sprintf(`ALTER CHANGEFEED %d SET INCLUDE TABLES foo, bar, baz`,
			testFeed.(cdctest.EnterpriseTestFeed).JobID()))

		sqlDB.Exec(t, `INSERT INTO foo VALUES (2, 'after include baz')`)
		sqlDB.Exec(t, `INSERT INTO bar VALUES (2, 'after include baz')`)
		sqlDB.Exec(t, `INSERT INTO baz VALUES (2, 'after include baz')`)

		sqlDB.Exec(t, fmt.Sprintf(`RESUME JOB %d`, testFeed.(cdctest.EnterpriseTestFeed).JobID()))
		waitForJobState(sqlDB, t, testFeed.(cdctest.EnterpriseTestFeed).JobID(), `running`)

		assertPayloads(t, testFeed, []string{
			// foo sees all three payloads.
			`foo: [0]->{"after": {"a": 0, "b": "initial"}}`,
			`foo: [1]->{"after": {"a": 1, "b": "after include bar"}}`,
			`foo: [2]->{"after": {"a": 2, "b": "after include baz"}}`,
			// bar sees the payloads from after it was added.
			// When this test fails, we expect to see the payload for bar with key 0,
			// which means that when it was picked up by the changefeed, we did
			// not resume from a persisted frontier timestamp.
			`bar: [1]->{"after": {"a": 1, "b": "after include bar"}}`,
			`bar: [2]->{"after": {"a": 2, "b": "after include baz"}}`,
			// baz only sees the payloads from after it was added.
			`baz: [2]->{"after": {"a": 2, "b": "after include baz"}}`,
		})

		// Verify directly that the progress contains both bar and baz.
		require.NoError(t, s.Server.InternalDB().(isql.DB).Txn(
			context.Background(),
			func(ctx context.Context, txn isql.Txn) error {
				spans, found, err := jobfrontier.GetResolvedSpans(
					ctx, txn, testFeed.(cdctest.EnterpriseTestFeed).JobID(), `alter_changefeed`,
				)
				if err != nil {
					return err
				}
				require.True(t, found)
				require.Equal(t, 2, len(spans))
				require.NotEqual(t, spans[0].Timestamp, spans[1].Timestamp)
				return nil
			},
		))
	}

	cdcTest(t, testFn, feedTestEnterpriseSinks)
}

// TestAlterChangefeedDatabaseLevelChangefeedWithInitialScan tests that we
// can alter a database level changefeed with initial scan set and if we change
// the filters to include more tables, those will NOT have an initial scan
// performed for them.
func TestAlterChangefeedDatabaseLevelChangefeedWithInitialScan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
		sqlDB.Exec(t, `CREATE TABLE bar (a INT PRIMARY KEY, b STRING)`)

		sqlDB.Exec(t, `INSERT INTO foo VALUES (0, 'initial')`)
		sqlDB.Exec(t, `INSERT INTO bar VALUES (0, 'initial')`)

		feed := feed(t, f, `CREATE CHANGEFEED FOR DATABASE d INCLUDE TABLES foo WITH initial_scan = 'yes'`)
		defer closeFeed(t, feed)
		assertPayloads(t, feed, []string{
			`foo: [0]->{"after": {"a": 0, "b": "initial"}}`,
		})

		jobID := feed.(cdctest.EnterpriseTestFeed).JobID()
		sqlDB.Exec(t, fmt.Sprintf(`PAUSE JOB %d`, jobID))
		waitForJobState(sqlDB, t, jobID, `paused`)
		sqlDB.Exec(t, fmt.Sprintf(`ALTER CHANGEFEED %d UNSET INCLUDE TABLES`, jobID))
		sqlDB.Exec(t, fmt.Sprintf(`RESUME JOB %d`, jobID))
		waitForJobState(sqlDB, t, jobID, `running`)

		sqlDB.Exec(t, `INSERT INTO foo VALUES (1, 'after alter')`)
		sqlDB.Exec(t, `INSERT INTO bar VALUES (1, 'after alter')`)

		// We do not expect to see a payload for bar with key 0 since we
		// are not doing an initial scan for bar.
		assertPayloads(t, feed, []string{
			`foo: [1]->{"after": {"a": 1, "b": "after alter"}}`,
			`bar: [1]->{"after": {"a": 1, "b": "after alter"}}`,
		})
	}
	cdcTest(t, testFn, feedTestEnterpriseSinks)
}

func TestAlterChangefeedDatabaseScopeUnqualifiedName(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE DATABASE movr`)
		sqlDB.Exec(t, `CREATE DATABASE new_movr`)

		sqlDB.Exec(t, `CREATE TABLE movr.drivers (id INT PRIMARY KEY, name STRING)`)
		sqlDB.Exec(t, `CREATE TABLE new_movr.drivers (id INT PRIMARY KEY, name STRING)`)

		sqlDB.Exec(t,
			`INSERT INTO movr.drivers VALUES (1, 'Alice')`,
		)

		sqlDB.Exec(t, `USE movr`)
		testFeed := feed(t, f, `CREATE CHANGEFEED FOR drivers WITH diff, resolved = '100ms'`,
			optOutOfMetamorphicDBLevelChangefeed{
				reason: "changefeed watches tables not in the default database",
			})
		defer closeFeed(t, testFeed)

		assertPayloads(t, testFeed, []string{
			`drivers: [1]->{"after": {"id": 1, "name": "Alice"}, "before": null}`,
		})

		expectResolvedTimestamp(t, testFeed)

		feed, ok := testFeed.(cdctest.EnterpriseTestFeed)
		require.True(t, ok)

		require.NoError(t, feed.Pause())

		sqlDB.Exec(t, `USE new_movr`)

		sqlDB.Exec(t, fmt.Sprintf(`ALTER CHANGEFEED %d UNSET diff`, feed.JobID()))

		require.NoError(t, feed.Resume())

		sqlDB.Exec(t,
			`INSERT INTO movr.drivers VALUES (2, 'Bob')`,
		)

		assertPayloads(t, testFeed, []string{
			`drivers: [2]->{"after": {"id": 2, "name": "Bob"}}`,
		})
	}

	cdcTest(t, testFn, feedTestEnterpriseSinks, feedTestNoExternalConnection, feedTestUseRootUserConnection)
}

func TestAlterChangefeedColumnFamilyDatabaseScope(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE DATABASE movr`)
		sqlDB.Exec(t, `CREATE TABLE movr.drivers (id INT PRIMARY KEY, name STRING, FAMILY onlyid (id), FAMILY onlyname (name))`)

		sqlDB.Exec(t,
			`INSERT INTO movr.drivers VALUES (1, 'Alice')`,
		)

		var args []any
		if _, ok := f.(*webhookFeedFactory); ok {
			args = append(args, optOutOfMetamorphicEnrichedEnvelope{reason: "metamorphic enriched envelope does not support column families for webhook sinks"})
		}
		args = append(args, optOutOfMetamorphicDBLevelChangefeed{
			reason: "changefeed watches tables not in the default database",
		})
		testFeed := feed(t, f, `CREATE CHANGEFEED FOR movr.drivers WITH diff, split_column_families`, args...)
		defer closeFeed(t, testFeed)

		assertPayloads(t, testFeed, []string{
			`drivers.onlyid: [1]->{"after": {"id": 1}, "before": null}`,
			`drivers.onlyname: [1]->{"after": {"name": "Alice"}, "before": null}`,
		})

		feed, ok := testFeed.(cdctest.EnterpriseTestFeed)
		require.True(t, ok)

		require.NoError(t, feed.Pause())

		sqlDB.Exec(t, `USE movr`)

		sqlDB.Exec(t, fmt.Sprintf(`ALTER CHANGEFEED %d DROP movr.drivers ADD movr.drivers FAMILY onlyid ADD drivers FAMILY onlyname UNSET diff`, feed.JobID()))

		require.NoError(t, feed.Resume())

		sqlDB.Exec(t,
			`INSERT INTO movr.drivers VALUES (2, 'Bob')`,
		)

		assertPayloads(t, testFeed, []string{
			`drivers.onlyid: [2]->{"after": {"id": 2}}`,
			`drivers.onlyname: [2]->{"after": {"name": "Bob"}}`,
		})
	}

	cdcTest(t, testFn, feedTestEnterpriseSinks, feedTestNoExternalConnection, feedTestUseRootUserConnection)
}

func TestAlterChangefeedAlterTableName(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE DATABASE movr`)
		sqlDB.Exec(t, `CREATE TABLE movr.users (id INT PRIMARY KEY, name STRING)`)
		sqlDB.Exec(t,
			`INSERT INTO movr.users VALUES (1, 'Alice')`,
		)

		// TODO(#145927): currently the metamorphic enriched envelope system for
		// webhook uses source.table_name as the topic. This test expects the
		// topic name to be durable across table renames, which is not expected
		// to be true for source.table_name.
		var args []any
		if _, ok := f.(*webhookFeedFactory); ok {
			args = append(args, optOutOfMetamorphicEnrichedEnvelope{reason: "see comment"})
		}
		args = append(args, optOutOfMetamorphicDBLevelChangefeed{
			reason: "changefeed watches tables not in the default database",
		})

		testFeed := feed(t, f, `CREATE CHANGEFEED FOR movr.users WITH diff, resolved = '100ms'`, args...)
		defer closeFeed(t, testFeed)

		assertPayloads(t, testFeed, []string{
			`users: [1]->{"after": {"id": 1, "name": "Alice"}, "before": null}`,
		})

		expectResolvedTimestamp(t, testFeed)

		sqlDB.Exec(t, `ALTER TABLE movr.users RENAME TO movr.riders`)
		sqlDB.CheckQueryResultsRetry(t, "SELECT count(*) FROM [SHOW TABLES FROM movr] WHERE table_name = 'riders'", [][]string{{"1"}})

		var tsLogical string
		sqlDB.QueryRow(t, `SELECT cluster_logical_timestamp()`).Scan(&tsLogical)

		ts := parseTimeToHLC(t, tsLogical)

		// ensure that the high watermark has progressed past the time in which the
		// schema change occurred
		testutils.SucceedsSoon(t, func() error {
			resolvedTS, _ := expectResolvedTimestamp(t, testFeed)
			if resolvedTS.Less(ts) {
				return errors.New("waiting for resolved timestamp to progress past the schema change event")
			}
			return nil
		})

		feed, ok := testFeed.(cdctest.EnterpriseTestFeed)
		require.True(t, ok)

		require.NoError(t, feed.Pause())

		sqlDB.Exec(t, fmt.Sprintf(`ALTER CHANGEFEED %d UNSET diff`, feed.JobID()))

		require.NoError(t, feed.Resume())

		sqlDB.Exec(t,
			`INSERT INTO movr.riders VALUES (2, 'Bob')`,
		)
		assertPayloads(t, testFeed, []string{
			`users: [2]->{"after": {"id": 2, "name": "Bob"}}`,
		})
	}

	cdcTest(t, testFn, feedTestEnterpriseSinks, feedTestNoExternalConnection, feedTestUseRootUserConnection)
}

func TestAlterChangefeedAddTargetsDuringSchemaChangeBackfill(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Set verbose log to confirm whether or not we hit the same nil row issue as in #140669
	testutils.SetVModule(t, "kv_feed=2,changefeed_processors=2")

	rnd, seed := randutil.NewPseudoRand()
	t.Logf("random seed: %d", seed)

	testFn := func(t *testing.T, s TestServerWithSystem, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		// Metemorphically disable declarative schema changer to ensure we cover
		// BACKFILL and RESTART boundary events.
		_ = maybeDisableDeclarativeSchemaChangesForTest(t, sqlDB)

		knobs := s.TestingKnobs.
			DistSQL.(*execinfra.TestingKnobs).
			Changefeed.(*TestingKnobs)

		sqlDB.Exec(t, `CREATE TABLE foo(val INT PRIMARY KEY)`)
		sqlDB.Exec(t, `INSERT INTO foo (val) SELECT * FROM generate_series(0, 999)`)

		sqlDB.Exec(t, `CREATE TABLE bar(val INT PRIMARY KEY)`)
		sqlDB.Exec(t, `INSERT INTO bar (val) SELECT * FROM generate_series(0, 999)`)

		// Ensure Scan Requests are always small enough that we receive multiple
		// resolved events during a backfill
		knobs.FeedKnobs.BeforeScanRequest = func(b *kv.Batch) error {
			b.Header.MaxSpanRequestKeys = 10
			return nil
		}

		testFeed := feed(t, f, `CREATE CHANGEFEED FOR foo
WITH resolved = '1s', no_initial_scan, min_checkpoint_frequency='1ns'`,
			optOutOfMetamorphicDBLevelChangefeed{
				reason: "db level changefeeds don't support ADD/DROP TARGETS in ALTER CHANGEFEEDs",
			})
		jobFeed := testFeed.(cdctest.EnterpriseTestFeed)
		jobRegistry := s.Server.JobRegistry().(*jobs.Registry)

		// Kafka feeds are not buffered, so we have to consume messages.
		// Track which bar keys we see to verify the initial scan.
		var seenBarKeys struct {
			syncutil.Mutex
			keys map[string]struct{}
		}
		seenBarKeys.keys = make(map[string]struct{})
		g := ctxgroup.WithContext(context.Background())
		g.Go(func() error {
			for {
				msg, err := testFeed.Next()
				if err != nil {
					return err
				}
				if msg != nil && msg.Topic == `bar` && len(msg.Key) > 0 {
					seenBarKeys.Lock()
					seenBarKeys.keys[string(msg.Key)] = struct{}{}
					seenBarKeys.Unlock()
				}
			}
		})
		defer func() {
			closeFeed(t, testFeed)
			_ = g.Wait()
		}()

		// Ensure initial backfill completes
		waitForHighwater(t, jobFeed, jobRegistry)

		// Pause job and setup overrides to force a checkpoint
		require.NoError(t, jobFeed.Pause())

		var maxCheckpointSize int64 = 100 << 20
		// Ensure that checkpoints happen every time by setting a large checkpoint size.
		// Because setting 0 for the SpanCheckpointInterval disables checkpointing,
		// setting 1 nanosecond is the smallest possible value.
		changefeedbase.SpanCheckpointInterval.Override(
			context.Background(), &s.Server.ClusterSettings().SV, 1*time.Nanosecond)
		changefeedbase.SpanCheckpointMaxBytes.Override(
			context.Background(), &s.Server.ClusterSettings().SV, maxCheckpointSize)

		// backfillTimestamp is used to track the timestamp of the backfill scan.
		// We only emit checkpoints that are part of the boundary event or part of
		// the backfill scan. This way we ensure we construct a partial checkpoint
		// for the backfill scan.
		var backfillTimestamp hlc.Timestamp

		// We track skipped spans to ensure we never emit a checkpoint that
		// overlaps with a span we already skipped.
		skippedSpans := interval.NewRangeTree()

		knobs.FilterSpanWithMutation = func(r *jobspb.ResolvedSpan) (bool, error) {
			t.Logf("span %s %s %s", r.Span.String(), r.BoundaryType.String(), r.Timestamp.String())

			if r.BoundaryType != jobspb.ResolvedSpan_NONE {
				t.Logf("found the boundary event")
				backfillTimestamp = r.Timestamp.Next()
				return false, nil
			}
			if backfillTimestamp.IsEmpty() {
				t.Logf("dropping span %s because we haven't found the boundary event yet", r.Span.String())
				return true, nil
			}

			spanInterval := interval.Range{
				Start: interval.Comparable(r.Span.Key),
				End:   interval.Comparable(r.Span.EndKey),
			}
			switch {
			case !r.Timestamp.Equal(backfillTimestamp):
				t.Logf("dropping span %s because it is not at the boundary timestamp", r.Span.String())
				return true, nil
			case skippedSpans.Overlaps(spanInterval):
				t.Logf("skipping span %s because it overlaps with an already skipped span", r.Span.String())
				return true, nil
			case rnd.Intn(2) != 1:
				t.Logf("randomly skipping span %s to create gaps in the frontier", r.Span.String())
				skippedSpans.Add(spanInterval)
				return true, nil
			default:
				t.Logf("passing span %s through", r.Span.String())
				return false, nil
			}
		}

		require.NoError(t, jobFeed.Resume())
		sqlDB.Exec(t, `ALTER TABLE foo ADD COLUMN b STRING DEFAULT 'd'`)

		// Wait for a checkpoint to have been set
		testutils.SucceedsSoon(t, func() error {
			progress := loadProgress(t, jobFeed, jobRegistry)
			if checkpoint := loadCheckpoint(t, progress); checkpoint == nil {
				return errors.Newf("waiting for checkpoint")
			}
			require.NoError(t, jobFeed.FetchTerminalJobErr())
			return nil
		})

		require.NoError(t, jobFeed.Pause())
		waitForJobState(sqlDB, t, jobFeed.JobID(), `paused`)

		// Adding a target with initial_scan during a schema change backfill
		// is supported — the new table's spans are merged into the existing
		// checkpoint.
		sqlDB.Exec(t, fmt.Sprintf(`ALTER CHANGEFEED %d ADD bar WITH initial_scan`, jobFeed.JobID()))

		// Verify the highwater was reset so the initial scan triggers.
		progress := loadProgress(t, jobFeed, jobRegistry)
		h := progress.GetHighWater()
		require.True(t, h == nil || h.IsEmpty())

		// Stop filtering spans so the changefeed can make progress.
		knobs.FilterSpanWithMutation = func(r *jobspb.ResolvedSpan) (bool, error) {
			return false, nil
		}

		require.NoError(t, jobFeed.Resume())

		// Wait for highwater to advance, proving the schema change backfill
		// completed and bar's initial scan succeeded.
		waitForHighwater(t, jobFeed, jobRegistry)

		// Verify we received bar's initial scan for all 1000 rows.
		testutils.SucceedsSoon(t, func() error {
			seenBarKeys.Lock()
			defer seenBarKeys.Unlock()
			if len(seenBarKeys.keys) < 1000 {
				return errors.Newf("waiting for bar keys: %d/1000", len(seenBarKeys.keys))
			}
			return nil
		})
	}

	cdcTestWithSystem(t, testFn, feedTestEnterpriseSinks, feedTestNoExternalConnection)
}

func TestAlterChangefeedAddTargetsDuringBackfill(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var rndMu struct {
		syncutil.Mutex
		rnd *rand.Rand
	}
	rndMu.rnd, _ = randutil.NewTestRand()
	const maxCheckpointSize = 1 << 20
	const numRowsPerTable = 1000

	testFn := func(t *testing.T, s TestServerWithSystem, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE foo(val INT PRIMARY KEY)`)
		sqlDB.Exec(t, `INSERT INTO foo (val) SELECT * FROM generate_series(0, $1)`, numRowsPerTable-1)

		sqlDB.Exec(t, `CREATE TABLE bar(val INT PRIMARY KEY)`)
		sqlDB.Exec(t, `INSERT INTO bar (val) SELECT * FROM generate_series(0, $1)`, numRowsPerTable-1)

		fooDesc := desctestutils.TestingGetPublicTableDescriptor(
			s.SystemServer.DB(), s.Codec, "d", "foo")
		fooTableSpan := fooDesc.PrimaryIndexSpan(s.Codec)

		knobs := s.TestingKnobs.
			DistSQL.(*execinfra.TestingKnobs).
			Changefeed.(*TestingKnobs)

		// Ensure Scan Requests are always small enough that we receive multiple
		// resolvedFoo events during a backfill.
		const maxBatchSize = numRowsPerTable / 5
		knobs.FeedKnobs.BeforeScanRequest = func(b *kv.Batch) error {
			rndMu.Lock()
			defer rndMu.Unlock()
			// We don't want batch sizes that are too small because they could cause
			// the initial scan to take too long, leading to the waitForHighwater
			// call below to time out. The formula below is completely arbitrary and
			// was chosen to ensure that the batch sizes aren't too small (i.e. in
			// the 1-2 digit range) but are still small enough that at least a few
			// batches will be necessary.
			b.Header.MaxSpanRequestKeys = maxBatchSize/2 + rndMu.rnd.Int63n(maxBatchSize/2)
			t.Logf("set max span request keys: %d", b.Header.MaxSpanRequestKeys)
			return nil
		}

		// Emit resolved events for the majority of spans. Be extra paranoid and ensure that
		// we have at least 1 span for which we don't emit resolvedFoo timestamp (to force checkpointing).
		// We however also need to ensure there's at least one span that isn't filtered out.
		var allowedOne, haveGaps bool
		knobs.FilterSpanWithMutation = func(r *jobspb.ResolvedSpan) (filter bool, _ error) {
			rndMu.Lock()
			defer rndMu.Unlock()
			defer func() {
				t.Logf("resolved span: %s@%s, filter: %t", r.Span, r.Timestamp, filter)
			}()

			if r.Span.Equal(fooTableSpan) {
				return true, nil
			}
			if !allowedOne {
				allowedOne = true
				return false, nil
			}
			if !haveGaps {
				haveGaps = true
				return true, nil
			}
			return rndMu.rnd.Intn(10) > 7, nil
		}

		// Checkpoint progress frequently, and set the checkpoint size limit.
		changefeedbase.SpanCheckpointInterval.Override(
			context.Background(), &s.Server.ClusterSettings().SV, 1)
		changefeedbase.SpanCheckpointMaxBytes.Override(
			context.Background(), &s.Server.ClusterSettings().SV, maxCheckpointSize)

		registry := s.Server.JobRegistry().(*jobs.Registry)
		testFeed := feed(t, f, `CREATE CHANGEFEED FOR foo
WITH resolved = '100ms', min_checkpoint_frequency='1ns'`, optOutOfMetamorphicDBLevelChangefeed{
			reason: "db level changefeeds don't support ADD/DROP TARGETS in ALTER CHANGEFEEDs",
		})

		g := ctxgroup.WithContext(context.Background())
		g.Go(func() error {
			// Kafka feeds are not buffered, so we have to consume messages.
			// We just want to ensure that eventually, we get all the rows from foo and bar.
			expectedValues := make([]string, 2*numRowsPerTable)
			for j := 0; j < numRowsPerTable; j++ {
				expectedValues[j] = fmt.Sprintf(`foo: [%d]->{"after": {"val": %d}}`, j, j)
				expectedValues[j+numRowsPerTable] = fmt.Sprintf(`bar: [%d]->{"after": {"val": %d}}`, j, j)
			}
			return assertPayloadsBaseErr(context.Background(), testFeed, expectedValues, false, false, nil, changefeedbase.OptEnvelopeWrapped)
		})

		defer func() {
			require.NoError(t, g.Wait())
			closeFeed(t, testFeed)
		}()

		jobFeed := testFeed.(cdctest.EnterpriseTestFeed)

		// Wait for non-nil checkpoint.
		waitForCheckpoint(t, jobFeed, registry)

		// Pause the job and read and verify the latest checkpoint information.
		require.NoError(t, jobFeed.Pause())
		progress := loadProgress(t, jobFeed, registry)
		require.NotNil(t, progress.GetChangefeed())
		h := progress.GetHighWater()
		noHighWater := h == nil || h.IsEmpty()
		require.True(t, noHighWater)

		checkpoint := makeSpanGroupFromCheckpoint(t, loadCheckpoint(t, progress))
		require.Greater(t, checkpoint.Len(), 0)

		sqlDB.Exec(t, fmt.Sprintf(`ALTER CHANGEFEED %d ADD bar WITH initial_scan`, jobFeed.JobID()))

		// Collect spans we attempt to resolve after when we resume.
		var resolvedFoo []roachpb.Span
		knobs.FilterSpanWithMutation = func(r *jobspb.ResolvedSpan) (filter bool, _ error) {
			defer func() {
				t.Logf("resolved span: %s@%s, filter: %t", r.Span, r.Timestamp, filter)
			}()
			if !r.Span.Equal(fooTableSpan) {
				resolvedFoo = append(resolvedFoo, r.Span)
			}
			return false, nil
		}

		require.NoError(t, jobFeed.Resume())

		// Wait for highwater to be set, which signifies that the initial scan is complete.
		waitForHighwater(t, jobFeed, registry)

		// At this point, highwater mark should be set, and previous checkpoint should be gone.
		progress = loadProgress(t, jobFeed, registry)
		require.Nil(t, loadCheckpoint(t, progress))

		require.NoError(t, jobFeed.Pause())

		// Verify that none of the resolvedFoo spans after resume were checkpointed.
		for _, sp := range resolvedFoo {
			require.Falsef(t, checkpoint.Contains(sp.Key), "span should not have been resolved: %s", sp)
		}
	}

	cdcTestWithSystem(t, testFn, feedTestEnterpriseSinks, feedTestNoExternalConnection)
}

func TestAlterChangefeedDropTargetDuringInitialScan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	rnd, _ := randutil.NewPseudoRand()

	testFn := func(t *testing.T, s TestServerWithSystem, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)

		sqlDB.Exec(t, `CREATE TABLE foo(val INT PRIMARY KEY)`)
		sqlDB.Exec(t, `INSERT INTO foo (val) SELECT * FROM generate_series(1, 100)`)

		sqlDB.Exec(t, `CREATE TABLE bar(val INT PRIMARY KEY)`)
		sqlDB.Exec(t, `INSERT INTO bar (val) SELECT * FROM generate_series(1, 100)`)

		fooDesc := desctestutils.TestingGetPublicTableDescriptor(
			s.SystemServer.DB(), s.Codec, "d", "foo")
		fooTableSpan := fooDesc.PrimaryIndexSpan(s.Codec)

		barDesc := desctestutils.TestingGetPublicTableDescriptor(
			s.SystemServer.DB(), s.Codec, "d", "bar")
		barTableSpan := barDesc.PrimaryIndexSpan(s.Codec)

		knobs := s.TestingKnobs.
			DistSQL.(*execinfra.TestingKnobs).
			Changefeed.(*TestingKnobs)

		// Make scan requests small enough so that we're guaranteed multiple
		// resolved events during the initial scan.
		knobs.FeedKnobs.BeforeScanRequest = func(b *kv.Batch) error {
			b.Header.MaxSpanRequestKeys = 10
			return nil
		}

		var allSpans roachpb.SpanGroup
		allSpans.Add(fooTableSpan, barTableSpan)
		var allSpansResolved atomic.Bool

		// Skip some spans for both tables so that the initial scan can't complete.
		var skippedFooSpans, skippedBarSpans roachpb.SpanGroup
		knobs.FilterSpanWithMutation = func(r *jobspb.ResolvedSpan) (bool, error) {
			defer func() {
				allSpans.Sub(r.Span)
				if allSpans.Len() == 0 {
					allSpansResolved.Store(true)
				}
			}()

			if r.Span.Equal(fooTableSpan) || r.Span.Equal(barTableSpan) ||
				skippedFooSpans.Encloses(r.Span) || skippedBarSpans.Encloses(r.Span) {
				return true, nil
			}

			if fooTableSpan.Contains(r.Span) && (skippedFooSpans.Len() == 0 || rnd.Intn(3) == 0) {
				skippedFooSpans.Add(r.Span)
				return true, nil
			}

			if barTableSpan.Contains(r.Span) && (skippedBarSpans.Len() == 0 || rnd.Intn(3) == 0) {
				skippedBarSpans.Add(r.Span)
				return true, nil
			}

			return false, nil
		}

		// Create a changefeed watching both tables.
		targets := "foo, bar"
		if rnd.Intn(2) == 0 {
			targets = "bar, foo"
		}
		testFeed := feed(t, f, fmt.Sprintf(`CREATE CHANGEFEED for %s`, targets),
			optOutOfMetamorphicDBLevelChangefeed{
				reason: "db level changefeeds don't support ADD/DROP TARGETS in ALTER CHANGEFEEDs",
			})
		defer closeFeed(t, testFeed)

		// Wait for all spans to have been resolved.
		testutils.SucceedsSoon(t, func() error {
			if allSpansResolved.Load() {
				return nil
			}
			return errors.New("expected all spans to be resolved")
		})

		// Pause the changefeed and make sure the initial scan hasn't completed yet.
		feed, ok := testFeed.(cdctest.EnterpriseTestFeed)
		require.True(t, ok)
		require.NoError(t, feed.Pause())
		hw, err := feed.HighWaterMark()
		require.NoError(t, err)
		require.Zero(t, hw)

		// Alter the changefeed to stop watching the second table.
		sqlDB.Exec(t, fmt.Sprintf(`ALTER CHANGEFEED %d DROP bar`, feed.JobID()))

		allSpans.Add(fooTableSpan)
		knobs.FilterSpanWithMutation = func(r *jobspb.ResolvedSpan) (bool, error) {
			if barTableSpan.Contains(r.Span) {
				t.Fatalf("span from dropped table should not have been resolved: %#v", r.Span)
			}
			allSpans.Sub(r.Span)
			return false, nil
		}

		require.NoError(t, feed.Resume())
		require.NoError(t, feed.WaitForHighWaterMark(hlc.Timestamp{}))
		require.Zero(t, allSpans.Len())
	}

	cdcTestWithSystem(t, testFn, feedTestEnterpriseSinks, feedTestNoExternalConnection)
}

func TestAlterChangefeedInitialScan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(initialScanOption string) cdcTestFn {
		return func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
			sqlDB := sqlutils.MakeSQLRunner(s.DB)
			sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY)`)
			sqlDB.Exec(t, `INSERT INTO foo VALUES (1), (2), (3)`)
			sqlDB.Exec(t, `CREATE TABLE bar (a INT PRIMARY KEY)`)
			sqlDB.Exec(t, `INSERT INTO bar VALUES (1), (2), (3)`)

			testFeed := feed(t, f, `CREATE CHANGEFEED FOR foo WITH resolved = '1s', no_initial_scan`,
				optOutOfMetamorphicDBLevelChangefeed{
					reason: "db level changefeeds don't support ADD/DROP TARGETS in ALTER CHANGEFEEDs",
				})
			defer closeFeed(t, testFeed)

			expectResolvedTimestamp(t, testFeed)

			feed, ok := testFeed.(cdctest.EnterpriseTestFeed)
			require.True(t, ok)

			sqlDB.Exec(t, `PAUSE JOB $1`, feed.JobID())
			waitForJobState(sqlDB, t, feed.JobID(), `paused`)

			sqlDB.Exec(t, fmt.Sprintf(`ALTER CHANGEFEED %d ADD bar WITH %s`, feed.JobID(), initialScanOption))

			sqlDB.Exec(t, fmt.Sprintf(`RESUME JOB %d`, feed.JobID()))
			waitForJobState(sqlDB, t, feed.JobID(), `running`)

			expectPayloads := initialScanOption == "initial_scan = 'yes'" || initialScanOption == "initial_scan"
			if expectPayloads {
				assertPayloads(t, testFeed, []string{
					`bar: [1]->{"after": {"a": 1}}`,
					`bar: [2]->{"after": {"a": 2}}`,
					`bar: [3]->{"after": {"a": 3}}`,
				})
			}

			sqlDB.Exec(t, `INSERT INTO bar VALUES (4)`)
			assertPayloads(t, testFeed, []string{
				`bar: [4]->{"after": {"a": 4}}`,
			})
		}
	}

	for _, initialScanOpt := range []string{
		"initial_scan = 'yes'",
		"initial_scan = 'no'",
		"initial_scan",
		"no_initial_scan",
	} {
		cdcTest(t, testFn(initialScanOpt), feedTestForceSink("kafka"), feedTestNoExternalConnection)
	}
}

// This test checks that the time used to get table descriptors in alter
// changefeed is the time from which changefeed will resume (check
// validateNewTargets for more info on how this time is calculated).
func TestAlterChangefeedWithOldCursorFromCreateChangefeed(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		registry := s.Server.JobRegistry().(*jobs.Registry)

		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)

		var tsLogical string
		sqlDB.QueryRow(t, `SELECT cluster_logical_timestamp()`).Scan(&tsLogical)
		cursor := parseTimeToHLC(t, tsLogical)

		testFeed := feed(t, f, `CREATE CHANGEFEED FOR foo WITH cursor=$1`, tsLogical)
		defer closeFeed(t, testFeed)

		sqlDB.Exec(t, `INSERT INTO foo VALUES (1, 'before')`)
		assertPayloads(t, testFeed, []string{
			`foo: [1]->{"after": {"a": 1, "b": "before"}}`,
		})

		castedFeed, ok := testFeed.(cdctest.EnterpriseTestFeed)
		require.True(t, ok)

		testutils.SucceedsSoon(t, func() error {
			progress := loadProgress(t, castedFeed, registry)
			if hw := progress.GetHighWater(); hw != nil && cursor.LessEq(*hw) {
				return nil
			}
			return errors.New("waiting for checkpoint advance")
		})

		sqlDB.Exec(t, `PAUSE JOB $1`, castedFeed.JobID())
		waitForJobState(sqlDB, t, castedFeed.JobID(), `paused`)

		sqlDB.Exec(t, `INSERT INTO foo VALUES (2, 'after')`)

		// Simulate that a significant time has passed since the create
		// change feed command was given - if the highwater mark is not
		// used in the following alter changefeed command, then we will
		// get an error when we try to get a table descriptors using
		// cursor time.
		calculateCursor := func(currentTime *hlc.Timestamp) string {
			return "-3h"
		}
		knobs := s.TestingKnobs.DistSQL.(*execinfra.TestingKnobs).Changefeed.(*TestingKnobs)
		knobs.OverrideCursor = calculateCursor

		sqlDB.Exec(t, fmt.Sprintf(`ALTER CHANGEFEED %d UNSET resolved`, castedFeed.JobID()))

		sqlDB.Exec(t, fmt.Sprintf(`RESUME JOB %d`, castedFeed.JobID()))
		waitForJobState(sqlDB, t, castedFeed.JobID(), `running`)

		assertPayloads(t, testFeed, []string{
			`foo: [2]->{"after": {"a": 2, "b": "after"}}`,
		})
	}

	cdcTest(t, testFn, feedTestEnterpriseSinks, feedTestNoExternalConnection)
}

// TestChangefeedJobControl tests if a user can modify and existing changefeed
// based on their privileges.
func TestAlterChangefeedAccessControl(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		ChangefeedJobPermissionsTestSetup(t, s)
		rootDB := sqlutils.MakeSQLRunner(s.DB)

		createFeed := func(stmt string) (cdctest.EnterpriseTestFeed, func()) {
			successfulFeed := feed(t, f, stmt, optOutOfMetamorphicDBLevelChangefeed{
				reason: "db level changefeeds don't support ADD/DROP TARGETS in ALTER CHANGEFEEDs",
			})
			closeCf := func() {
				closeFeed(t, successfulFeed)
			}
			_, err := successfulFeed.Next()
			require.NoError(t, err)
			return successfulFeed.(cdctest.EnterpriseTestFeed), closeCf
		}

		// Create a changefeed and pause it.
		var currentFeed cdctest.EnterpriseTestFeed
		var closeCf func()
		asUser(t, f, `feedCreator`, func(_ *sqlutils.SQLRunner) {
			currentFeed, closeCf = createFeed(`CREATE CHANGEFEED FOR table_a, table_b`)
		})
		rootDB.Exec(t, "PAUSE job $1", currentFeed.JobID())
		waitForJobState(rootDB, t, currentFeed.JobID(), `paused`)
		rootDB.Exec(t, "ALTER JOB $1 OWNER TO feedowner", currentFeed.JobID())

		// Verify who can modify the existing changefeed.
		asUser(t, f, `userWithAllGrants`, func(userDB *sqlutils.SQLRunner) {
			userDB.Exec(t, fmt.Sprintf(`ALTER CHANGEFEED %d DROP table_b`, currentFeed.JobID()))
		})
		asUser(t, f, `adminUser`, func(userDB *sqlutils.SQLRunner) {
			userDB.Exec(t, fmt.Sprintf(`ALTER CHANGEFEED %d ADD table_b`, currentFeed.JobID()))
		})
		// jobController can access the job, but will hit an error re-creating the changefeed.
		asUser(t, f, `jobController`, func(userDB *sqlutils.SQLRunner) {
			userDB.ExpectErr(t, `pq: user "jobcontroller" requires the CHANGEFEED privilege on all target tables to be able to run an enterprise changefeed`, fmt.Sprintf(`ALTER CHANGEFEED %d DROP table_b`, currentFeed.JobID()))
		})
		asUser(t, f, `userWithSomeGrants`, func(userDB *sqlutils.SQLRunner) {
			userDB.ExpectErr(t, "does not have privileges for job", fmt.Sprintf(`ALTER CHANGEFEED %d ADD table_b`, currentFeed.JobID()))
		})
		asUser(t, f, `regularUser`, func(userDB *sqlutils.SQLRunner) {
			userDB.ExpectErr(t, "does not have privileges for job", fmt.Sprintf(`ALTER CHANGEFEED %d ADD table_b`, currentFeed.JobID()))
		})
		closeCf()

		// No one can modify changefeeds created by admins, except for admins.
		asUser(t, f, `adminUser`, func(_ *sqlutils.SQLRunner) {
			currentFeed, closeCf = createFeed(`CREATE CHANGEFEED FOR table_a, table_b`)
		})
		asUser(t, f, `otherAdminUser`, func(userDB *sqlutils.SQLRunner) {
			userDB.Exec(t, "PAUSE job $1", currentFeed.JobID())
			require.NoError(t, currentFeed.WaitForState(func(s jobs.State) bool {
				return s == jobs.StatePaused
			}))
		})
		asUser(t, f, `userWithAllGrants`, func(userDB *sqlutils.SQLRunner) {
			userDB.ExpectErr(t, "pq: only admins can control jobs owned by other admins", fmt.Sprintf(`ALTER CHANGEFEED %d ADD table_b`, currentFeed.JobID()))
		})
		asUser(t, f, `jobController`, func(userDB *sqlutils.SQLRunner) {
			userDB.ExpectErr(t, "pq: only admins can control jobs owned by other admins", fmt.Sprintf(`ALTER CHANGEFEED %d ADD table_b`, currentFeed.JobID()))
		})
		closeCf()
	}

	// Only enterprise sinks create jobs.
	cdcTest(t, testFn, feedTestEnterpriseSinks)
}

// TestAlterChangefeedAddDropSameTarget tests adding and dropping the same
// target multiple times in a statement.
func TestAlterChangefeedAddDropSameTarget(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY)`)
		sqlDB.Exec(t, `CREATE TABLE bar (a INT PRIMARY KEY)`)

		testFeed := feed(t, f, `CREATE CHANGEFEED FOR foo`, optOutOfMetamorphicDBLevelChangefeed{
			reason: "db level changefeeds don't support ADD/DROP TARGETS in ALTER CHANGEFEEDs",
		})
		defer closeFeed(t, testFeed)

		feed, ok := testFeed.(cdctest.EnterpriseTestFeed)
		require.True(t, ok)

		// Test removing and adding the same target.
		require.NoError(t, feed.Pause())
		sqlDB.Exec(t, fmt.Sprintf(`ALTER CHANGEFEED %d DROP foo ADD foo`, feed.JobID()))
		require.NoError(t, feed.Resume())
		sqlDB.Exec(t, `INSERT INTO foo VALUES(1)`)
		assertPayloads(t, testFeed, []string{
			`foo: [1]->{"after": {"a": 1}}`,
		})

		// Test adding and removing the same target.
		require.NoError(t, feed.Pause())
		sqlDB.Exec(t, fmt.Sprintf(`ALTER CHANGEFEED %d ADD bar DROP bar`, feed.JobID()))
		require.NoError(t, feed.Resume())
		var tsStr string
		sqlDB.Exec(t, `INSERT INTO bar VALUES(1)`)
		sqlDB.QueryRow(t, `INSERT INTO foo VALUES(2) RETURNING cluster_logical_timestamp()`).Scan(&tsStr)
		ts := parseTimeToHLC(t, tsStr)
		require.NoError(t, feed.WaitForHighWaterMark(ts))
		// We don't expect to see the row inserted into bar.
		assertPayloads(t, testFeed, []string{
			`foo: [2]->{"after": {"a": 2}}`,
		})

		// Test adding, removing, and adding the same target.
		require.NoError(t, feed.Pause())
		sqlDB.Exec(t, fmt.Sprintf(
			`ALTER CHANGEFEED %d ADD bar DROP bar ADD bar WITH initial_scan='yes'`, feed.JobID()))
		require.NoError(t, feed.Resume())
		sqlDB.Exec(t, `INSERT INTO bar VALUES(2)`)
		assertPayloads(t, testFeed, []string{
			`bar: [1]->{"after": {"a": 1}}`,
			`bar: [2]->{"after": {"a": 2}}`,
		})
	}

	cdcTest(t, testFn, feedTestEnterpriseSinks, feedTestNoExternalConnection)
}

// TestAlterChangefeedRandomizedTargetChanges tests altering a changefeed
// with randomized adding and dropping of targets.
func TestAlterChangefeedRandomizedTargetChanges(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testutils.SetVModule(t, "helpers_test=1")

	rnd, _ := randutil.NewPseudoRand()

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)

		// The tables in this test will have the rows 0, ..., tableRowCounts[tableName]-1.
		tables := make(map[string]struct{})
		tableRowCounts := make(map[string]int)

		makeExpectedRow := func(tableName string, row int, updated hlc.Timestamp) string {
			return fmt.Sprintf(`%s: [%[2]d]->{"after": {"a": %[2]d}, "updated": "%s"}`,
				tableName, row, updated.AsOfSystemTime())
		}

		insertRowsIntoTable := func(tableName string, numRows int) []string {
			rows := make([]string, 0, numRows)
			for range numRows {
				row := tableRowCounts[tableName]
				var tsStr string
				insertStmt := fmt.Sprintf(`INSERT INTO %s VALUES (%d)`, tableName, row)
				t.Log(insertStmt)
				sqlDB.QueryRow(t,
					fmt.Sprintf(`%s RETURNING cluster_logical_timestamp()`, insertStmt),
				).Scan(&tsStr)
				ts := parseTimeToHLC(t, tsStr)
				rows = append(rows, makeExpectedRow(tableName, row, ts))
				tableRowCounts[tableName] += 1
			}
			return rows
		}

		// Create 10 tables with a single row to start.
		const numTables = 10
		t.Logf("creating %d tables", numTables)
		for i := range numTables {
			tableName := fmt.Sprintf("table%d", i)
			createStmt := fmt.Sprintf(`CREATE TABLE %s (a INT PRIMARY KEY)`, tableName)
			t.Log(createStmt)
			sqlDB.Exec(t, createStmt)
			tables[tableName] = struct{}{}
			insertRowsIntoTable(tableName, 1 /* numRows */)
		}

		// makeInitialScanRows returns the expected initial scan rows assuming
		// every row in the table will be included in the initial scan.
		makeInitialScanRows := func(newTables []string, scanTime hlc.Timestamp) []string {
			var rows []string
			for _, t := range newTables {
				for i := range tableRowCounts[t] {
					rows = append(rows, makeExpectedRow(t, i, scanTime))
				}
			}
			return rows
		}

		// Randomly select some subset of tables to be the initial changefeed targets.
		initialTables := getNFromSet(rnd, tables, 1+rnd.Intn(numTables))
		watchedTables := makeSet(initialTables)
		nonWatchedTables := setDifference(tables, watchedTables)

		// Create the changefeed.
		createStmt := fmt.Sprintf(
			`CREATE CHANGEFEED FOR %s WITH updated`, strings.Join(initialTables, ", "))
		t.Log(createStmt)
		testFeed := feed(t, f, createStmt, optOutOfMetamorphicDBLevelChangefeed{
			reason: "db level changefeeds don't support ADD/DROP TARGETS in ALTER CHANGEFEEDs",
		})
		defer closeFeed(t, testFeed)

		feed, ok := testFeed.(cdctest.EnterpriseTestFeed)
		require.True(t, ok)

		d, err := feed.Details()
		require.NoError(t, err)
		statementTime := d.StatementTime
		require.NoError(t, feed.WaitForHighWaterMark(statementTime))
		assertPayloads(t, testFeed, makeInitialScanRows(initialTables, statementTime))

		const numAlters = 10
		t.Logf("will perform %d alters", numAlters)
		for i := range numAlters {
			t.Logf("performing alter #%d", i+1)

			require.NoError(t, feed.Pause())

			hw, err := feed.HighWaterMark()
			require.NoError(t, err)

			var alterStmtBuilder strings.Builder
			write := func(format string, args ...any) {
				_, err := fmt.Fprintf(&alterStmtBuilder, format, args...)
				require.NoError(t, err)
			}
			write(`ALTER CHANGEFEED %d`, feed.JobID())

			// We get the set of tables to add/drop first to ensure we are
			// selecting without replacement.
			numAdds := rnd.Intn(len(nonWatchedTables) + 1)
			numDrops := rnd.Intn(len(watchedTables))
			if numAdds == 0 && numDrops == 0 {
				t.Logf("skipping alter #%d", i+1)
				continue
			}
			adds := getNFromSet(rnd, nonWatchedTables, numAdds)
			drops := getNFromSet(rnd, watchedTables, numDrops)

			var expectedRows []string
			for len(adds) > 0 || len(drops) > 0 {
				// Randomize the order of adds and drops.
				if add := len(adds) > 0 && (len(drops) == 0 || rnd.Intn(2) == 0); add {
					addTarget := adds[0]
					adds = adds[1:]
					delete(nonWatchedTables, addTarget)
					watchedTables[addTarget] = struct{}{}

					write(` ADD %s`, addTarget)

					switch rnd.Intn(3) {
					case 0:
						write(` WITH initial_scan='yes'`)
						expectedRows = append(expectedRows, makeInitialScanRows([]string{addTarget}, hw)...)
					case 1:
						write(` WITH initial_scan='no'`)
					case 2:
						// The default option is initial_scan='no'.
					}
					expectedRows = append(expectedRows,
						insertRowsIntoTable(addTarget, 2 /* numRows */)...)
				} else { // Drop a target.
					dropTarget := drops[0]
					drops = drops[1:]
					delete(watchedTables, dropTarget)
					nonWatchedTables[dropTarget] = struct{}{}

					write(` DROP %s`, dropTarget)

					// Insert some more rows into the table that
					// should NOT be emitted by the changefeed.
					insertRowsIntoTable(dropTarget, 3 /* numRows */)
				}
			}
			require.Empty(t, adds)
			require.Empty(t, drops)

			alterStmt := alterStmtBuilder.String()
			t.Log(alterStmt)
			sqlDB.Exec(t, alterStmt)

			require.NoError(t, feed.Resume())

			// Wait for highwater to advance past the current time so that
			// we're sure no more rows are expected.
			var tsStr string
			sqlDB.QueryRow(t, `SELECT cluster_logical_timestamp()`).Scan(&tsStr)
			ts := parseTimeToHLC(t, tsStr)
			require.NoError(t, feed.WaitForHighWaterMark(ts))

			assertPayloads(t, testFeed, expectedRows)
		}
	}

	cdcTest(t, testFn, feedTestEnterpriseSinks, feedTestNoExternalConnection)
}

func TestAlterChangefeedSetPartitionAlgOption(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
		sqlDB.Exec(t, `SET CLUSTER SETTING changefeed.partition_alg.enabled = true`)

		testFeed := feed(t, f, `CREATE CHANGEFEED FOR foo WITH partition_alg='murmur2'`)
		defer closeFeed(t, testFeed)

		feed, ok := testFeed.(cdctest.EnterpriseTestFeed)
		require.True(t, ok)

		sqlDB.Exec(t, `PAUSE JOB $1`, feed.JobID())
		waitForJobState(sqlDB, t, feed.JobID(), `paused`)

		sqlDB.Exec(t, fmt.Sprintf(`ALTER CHANGEFEED %d SET partition_alg='fnv-1a'`, feed.JobID()))

		sqlDB.Exec(t, fmt.Sprintf(`RESUME JOB %d`, feed.JobID()))
		waitForJobState(sqlDB, t, feed.JobID(), `running`)

		sqlDB.Exec(t, `INSERT INTO foo VALUES (0, 'initial')`)
		assertPayloads(t, testFeed, []string{
			`foo: [0]->{"after": {"a": 0, "b": "initial"}}`,
		})

		// When disabled, unable to set partition_alg option.
		sqlDB.Exec(t, `SET CLUSTER SETTING changefeed.partition_alg.enabled = false`)
		sqlDB.Exec(t, `PAUSE JOB $1`, feed.JobID())
		waitForJobState(sqlDB, t, feed.JobID(), `paused`)

		sqlDB.ExpectErr(t,
			`option "partition_alg" requires cluster setting "changefeed.partition_alg.enabled" to be enabled`,
			fmt.Sprintf(`ALTER CHANGEFEED %d SET partition_alg='murmur2'`, feed.JobID()),
		)
	}

	cdcTest(t, testFn, feedTestForceSink("kafka"), feedTestNoExternalConnection)
}

// makeSet returns a new set with the elements in the provided slice.
func makeSet[K cmp.Ordered](ks []K) map[K]struct{} {
	m := make(map[K]struct{}, len(ks))
	for _, k := range ks {
		m[k] = struct{}{}
	}
	return m
}

// setDifference returns a new set that is s - t.
func setDifference[K cmp.Ordered](s map[K]struct{}, t map[K]struct{}) map[K]struct{} {
	difference := make(map[K]struct{})
	for e := range s {
		if _, ok := t[e]; !ok {
			difference[e] = struct{}{}
		}
	}
	return difference
}

// getNFromSet returns a slice with n random elements from s.
func getNFromSet[K cmp.Ordered](rnd *rand.Rand, s map[K]struct{}, n int) []K {
	if len(s) < n {
		panic(fmt.Sprintf("not enough elements in set, wanted %d, found %d", n, len(s)))
	}
	ks := slices.Sorted(maps.Keys(s))
	rnd.Shuffle(len(ks), func(i, j int) {
		ks[i], ks[j] = ks[j], ks[i]
	})
	return ks[:n]
}
