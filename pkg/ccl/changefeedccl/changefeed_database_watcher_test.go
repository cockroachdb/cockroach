package changefeedccl

import (
	"context"
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
