// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

func TestGCExpiredIndexes(t *testing.T) {
	defer leaktest.AfterTest(t)()

	defer func(oldAdoptInterval, oldGCInterval time.Duration) {
		jobs.DefaultAdoptInterval = oldAdoptInterval
		MinSqlGCInterval = oldGCInterval
	}(jobs.DefaultAdoptInterval, MinSqlGCInterval)
	jobs.DefaultAdoptInterval = 100 * time.Millisecond
	MinSqlGCInterval = 100 * time.Millisecond

	type DropItem int
	const (
		INDEX = iota
		TABLE
	)

	for _, dropItem := range []DropItem{INDEX, TABLE} {
		for _, lowerTTL := range []bool{true, false} {
			s, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
			ctx := context.Background()
			defer s.Stopper().Stop(ctx)
			sqlDB := sqlutils.MakeSQLRunner(db)

			jobRegistry := s.JobRegistry().(*jobs.Registry)

			sqlDB.Exec(t, "CREATE DATABASE my_db")
			sqlDB.Exec(t, "USE my_db")
			sqlDB.Exec(t, "CREATE TABLE my_table (a int primary key, b int, index (b))")
			if lowerTTL {
				sqlDB.Exec(t, "ALTER TABLE my_table CONFIGURE ZONE USING gc.ttlseconds = 1")
			}
			fmt.Println(sqlDB.QueryStr(t, "SELECT * FROM system.namespace"))
			descID := sqlbase.ID(keys.MinUserDescID + 3)

			var droppedDesc *sqlbase.TableDescriptor
			if err := kvDB.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
				var err error
				droppedDesc, err = sqlbase.GetTableDescFromID(ctx, txn, descID)
				return err
			}); err != nil {
				t.Fatal(err)
			}

			// Start the job that drops an index.
			dropTime := timeutil.Now().UnixNano()
			var details jobspb.WaitingForGCDetails
			switch dropItem {
			case INDEX:
				details = jobspb.WaitingForGCDetails{
					Indexes: []jobspb.WaitingForGCDetails_DroppedIndex{
						{
							IndexID:  sqlbase.IndexID(2),
							DropTime: timeutil.Now().UnixNano(),
						},
					},
					Tables: []jobspb.WaitingForGCDetails_DroppedID{
						{
							ID: descID,
						},
					},
				}
			case TABLE:
				details = jobspb.WaitingForGCDetails{
					Tables: []jobspb.WaitingForGCDetails_DroppedID{
						{
							ID:       descID,
							DropTime: dropTime,
						},
					},
				}
			}

			jobRecord := jobs.Record{
				Description:   fmt.Sprintf("GC Indexes"),
				Username:      "user",
				DescriptorIDs: sqlbase.IDs{descID},
				Details:       details,
				Progress:      jobspb.WaitingForGCProgress{},
				NonCancelable: true,
			}

			resultsCh := make(chan tree.Datums)
			job, _, err := jobRegistry.CreateAndStartJob(ctx, resultsCh, jobRecord)
			if err != nil {
				t.Fatal(err)
			}

			switch dropItem {
			case INDEX:
				droppedDesc.Indexes = droppedDesc.Indexes[:0]
				droppedDesc.GCMutations = append(droppedDesc.GCMutations, sqlbase.TableDescriptor_GCDescriptorMutation{
					IndexID:  sqlbase.IndexID(2),
					DropTime: time.Now().UnixNano(),
					JobID:    *job.ID(),
				})
			case TABLE:
				droppedDesc.State = sqlbase.TableDescriptor_DROP
				droppedDesc.DropTime = dropTime
			}

			if err := kvDB.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
				b := txn.NewBatch()
				descKey := sqlbase.MakeDescMetadataKey(descID)
				descDesc := sqlbase.WrapDescriptor(droppedDesc)
				b.Put(descKey, descDesc)
				return txn.Run(ctx, b)
			}); err != nil {
				t.Fatal(err)
			}

			// Check that the job started.
			jobIDStr := strconv.Itoa(int(*job.ID()))
			sqlDB.CheckQueryResults(t, "SELECT job_id, status FROM [SHOW JOBS]", [][]string{{jobIDStr, "running"}})

			if lowerTTL {
				sqlDB.CheckQueryResultsRetry(t, "SELECT job_id, status FROM [SHOW JOBS]", [][]string{{jobIDStr, "succeeded"}})
			} else {
				time.Sleep(500 * time.Millisecond)
			}

			if err := kvDB.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
				var err error
				droppedDesc, err = sqlbase.GetTableDescFromID(ctx, txn, descID)
				if lowerTTL && dropItem == TABLE {
					// We dropped the table, so expect it to not be found.
					require.EqualError(t, err, "descriptor not found")
					return nil
				}
				return err
			}); err != nil {
				t.Fatal(err)
			}

			switch dropItem {
			case INDEX:
				if lowerTTL {
					require.Equal(t, 0, len(droppedDesc.GCMutations))
				} else {
					require.Equal(t, 1, len(droppedDesc.GCMutations))
				}
			case TABLE:
				// Already handled the case where the TTL was lowered, since we expect
				// to not find the descriptor.
				// If the TTL was not lowered, we just expect to have not found an error
				// when fetching the TTL.
			}
		}
	}
}
