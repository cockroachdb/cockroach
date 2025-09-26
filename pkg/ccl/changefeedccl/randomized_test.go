// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdctest"
	"github.com/cockroachdb/cockroach/pkg/internal/sqlsmith"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

// TestChangefeedRandomized is a randomized test for changefeeds.
// It creates a table with a randomly generated schema, populates
// it with random data, creates a changefeed watching that table,
// then applies more random mutations. The test validates
// correctness using CDC validators on the feed.
func TestChangefeedRandomized(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	// skip.UnderStress(t)
	skip.UnderRace(t)

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		defer s.DB.Close()
		sqlDB := sqlutils.MakeSQLRunner(s.DB)
		rng, _ := randutil.NewTestRand()
		ctx := context.Background()
		sqlDB.Exec(t, "SET CLUSTER SETTING sql.stats.automatic_collection.enabled = false")

		// TODO(#134118): Support multiple tables.
		tableName := "rand_table"
		createTblStmt := randgen.RandCreateTableWithName(
			ctx,
			rng,
			tableName,
			1,
			randgen.TableOptSkipColumnFamilyMutations,
		)
		stmt := tree.SerializeForDisplay(createTblStmt)
		t.Log(stmt)
		sqlDB.Exec(t, stmt)

		// Insert some initial data.
		var inserts []string

		// Note: we do not care how many rows successfully populate
		// the given table
		numInserts := rng.Intn(100) - 1
		var err error
		if numInserts, err = randgen.PopulateTableWithRandData(rng, s.DB, tableName,
			numInserts, &inserts); err != nil {
			t.Fatal(err)
		}
		t.Logf("Added %d rows into table %s", numInserts, tableName)
		t.Log(strings.Join(inserts, "\n"))

		// Initialize the query generator.
		queryGen, err := sqlsmith.NewSmither(s.DB, rng,
			sqlsmith.MutationsOnly(),
			sqlsmith.SetScalarComplexity(0.5),
			sqlsmith.SetComplexity(0.1),
			// TODO(harding): Validators don't handle geometry types correctly.
			sqlsmith.SimpleScalarTypes(),
			// TODO(#129072): Reenable cross joins when the likelihood of generating
			// queries that could hang decreases.
			sqlsmith.DisableCrossJoins(),
		)
		require.NoError(t, err)
		defer queryGen.Close()

		// Determine what options to use with the changefeed. Always specify resolved
		// and updated to use the BeforeAndAfter validator.
		// TODO(#134119): Support a random assortment of options.
		options := []string{
			"resolved",
			"updated",
		}

		// Set up validators.
		var validators []*cdctest.CountValidator
		var vs cdctest.Validators
		nov := cdctest.NewOrderValidator(tableName)
		vs = append(vs, nov)
		// TODO(#134159): Enable fingerprint validator.
		// TODO(#134158): Enable BeforeAfterValidator. It only works with both resolved and updated timestamps.
		validators = append(validators, cdctest.NewCountValidator(vs))

		// Create a changefeed that watches the table.
		createStmt := `CREATE CHANGEFEED FOR ` + tableName + " WITH " + strings.Join(options, ", ")
		t.Log(createStmt)
		feed, err := f.Feed(createStmt)
		require.NoError(t, err)

		// Kick off validation.
		go validate(t, feed, validators)

		// Insert more data.
		numInserts = 100
		for i := range numInserts {
			start := time.Now()
			query := queryGen.Generate()
			log.Infof(ctx, "generated query %d in %s", i, time.Since(start))
			t.Logf("query %d: %s", i, query)
			start = time.Now()
			_, _ = s.DB.Exec(query)
			log.Infof(ctx, "executed query %d in %s", i, time.Since(start))
		}

		// Closing the feed will cause the validation routine to exit.
		t.Logf("Closing feed")
		closeFeedIgnoreError(t, feed)
	}

	// TODO(#134150): Support additional sinks.
	cdcTest(t, testFn, feedTestForceSink("kafka"))
}

func validate(t *testing.T, feed cdctest.TestFeed, validators []*cdctest.CountValidator) {
	for {
		msg, err := feed.Next()
		if err != nil {
			return
		}
		t.Logf("validate: %+v", msg)
		updated, resolved, err := cdctest.ParseJSONValueTimestamps(append(msg.Value, msg.Resolved...))
		if err != nil {
			t.Errorf("validation error (aborting): %s", err)
			return
		}
		for _, v := range validators {
			if len(msg.Key) > 0 {
				err := v.NoteRow(msg.Partition, string(msg.Key), string(msg.Value), updated)
				if err != nil {
					t.Errorf("validation error: %s", err)
					continue
				}
			} else {
				err := v.NoteResolved(msg.Partition, resolved)
				if err != nil {
					t.Errorf("validation error: %s", err)
					continue
				}
			}
		}
	}
}
