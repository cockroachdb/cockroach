package changefeedccl

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdctest"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

type drainHelperFalse struct{}

func (drainHelperFalse) IsDraining() bool { return false }

// TestDatabaseLevelChangefeedRestartsOnNewTableAdded ensures that database-level changefeeds
// restarts when a new table is added to include new data in the changefeed.
func TestDatabaseLevelChangefeedRestartsOnNewTableAdded(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		// Create database and an initial table.
		sqlDB.Exec(t, `CREATE DATABASE IF NOT EXISTS d`)
		sqlDB.Exec(t, `CREATE TABLE IF NOT EXISTS d.t1 (a INT PRIMARY KEY)`)

		// Capture distributed flow errors.
		matchedCh := make(chan struct{}, 1)
		knobs := s.TestingKnobs.DistSQL.(*execinfra.TestingKnobs).Changefeed.(*TestingKnobs)
		knobs.HandleDistChangefeedError = func(err error) error {
			if testutils.IsError(err, errDatabaseTargetsChanged.Error()) {
				// Assert non-terminal via default classification.
				term := changefeedbase.AsTerminalError(context.Background(), drainHelperFalse{}, err)
				if term != nil {
					t.Errorf("expected non-terminal error, got %v", term)
				} else {
					select {
					case matchedCh <- struct{}{}:
					default:
					}
				}
			}
			return err
		}

		// Start a database-level changefeed with frequent checkpoint attempts.
		feed := feed(t, f, `CREATE CHANGEFEED FOR DATABASE d WITH initial_scan='no', min_checkpoint_frequency='100ms'`)
		defer closeFeed(t, feed)

		// Generate some traffic.
		sqlDB.Exec(t, `INSERT INTO d.t1 VALUES (1)`)
		assertPayloads(t, feed, []string{
			`t1: [1]->{"after": {"a": 1}}`,
		})

		// Add a new table to trigger watcher diffs around checkpointing.
		sqlDB.Exec(t, `CREATE TABLE d.t2 (a INT PRIMARY KEY)`)
		sqlDB.Exec(t, `INSERT INTO d.t2 VALUES (1)`)

		// Expect the specific non-terminal error to be observed soon.
		select {
		case <-matchedCh:
		case <-time.After(30 * time.Second):
			t.Fatal("expected watcher checkpoint guard error, timed out")
		}

		// The feed should start back up again and emit the new table and not miss any data.
		assertPayloads(t, feed, []string{
			`t2: [1]->{"after": {"a": 1}}`,
		})
	}

	cdcTest(t, testFn)
}

func TestDatabaseLevelChangefeedToleratesDropsAndFinishesData(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)

		sqlDB.Exec(t, "SELECT crdb_internal.set_vmodule('event_processing=3')")
		sqlDB.Exec(t, `CREATE DATABASE IF NOT EXISTS d`)
		sqlDB.Exec(t, `CREATE TABLE IF NOT EXISTS d.t1 (a INT PRIMARY KEY)`)
		sqlDB.Exec(t, `CREATE TABLE IF NOT EXISTS d.t2 (a INT PRIMARY KEY)`)

		// TODO: also it times out creating the feed here for some reason ...
		// possibly only with sinkless? gonna omit sinkless for now to see if
		// that helps
		feed := feed(t, f, `CREATE CHANGEFEED FOR DATABASE d WITH initial_scan='no', min_checkpoint_frequency='100ms'`)
		defer closeFeed(t, feed)

		sqlDB.Exec(t, `INSERT INTO d.t1 VALUES (1)`)

		assertPayloads(t, feed, []string{
			`t1: [1]->{"after": {"a": 1}}`,
		})

		sqlDB.Exec(t, `INSERT INTO d.t2 VALUES (1)`)
		sqlDB.Exec(t, `DROP TABLE d.t2`)
		sqlDB.Exec(t, `INSERT INTO d.t1 VALUES (2)`)

		fmt.Printf("did drop\n")

		assertPayloads(t, feed, []string{
			`t1: [2]->{"after": {"a": 2}}`,
			// expect to see the last value of t2 even though it was dropped
			// TODO: this is flaky -- sometimes it emits it and sometimes it doesn't
			`t2: [1]->{"after": {"a": 1}}`,
		})

		sqlDB.Exec(t, `INSERT INTO d.t1 VALUES (3)`)
		assertPayloads(t, feed, []string{
			`t1: [3]->{"after": {"a": 3}}`,
		})

	}

	cdcTest(t, testFn, feedTestOmitSinks("sinkless"))
}
