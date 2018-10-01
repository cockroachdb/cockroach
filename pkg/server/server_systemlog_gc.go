// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package server

import (
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// gcSystemLog deletes entries in the given system log table older than the
// given cutoffTimestamp if the server is the lease holder for range 1.
// Leaseholder constraint is present so that only one node in the cluster
// performs gc.
// The system log table is expected to have a "timestamp" column.
// It returns the the number of rows affected and error (if any).
func (s *Server) gcSystemLog(
	ctx context.Context, table string, cutoffTimestamp time.Time,
) (int64, error) {
	var totalRowsAffected int64
	repl, err := s.node.stores.GetReplicaForRangeID(roachpb.RangeID(1))
	if err != nil {
		return 0, err
	}

	if repl.IsFirstRange() && repl.OwnsValidLease(s.clock.Now()) {
		deleteStmt := fmt.Sprintf(
			`SELECT count(1), max(timestamp) FROM 
[DELETE FROM system.%s WHERE timestamp >= $1 AND timestamp <= $2 LIMIT 1000 RETURNING timestamp]`,
			table,
		)
		// A timestamp lower-bound is used in the delete query to avoid hitting
		// tombstones.
		// This lower bound is periodically updated after every successful delete
		// batch.
		timestampLowerBound := timeutil.Unix(0, 0)
		for {
			var rowsAffected int64
			err := s.db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
				var err error
				row, err := s.internalExecutor.QueryRow(
					ctx,
					table+"-gc",
					txn,
					deleteStmt,
					timestampLowerBound,
					cutoffTimestamp,
				)
				if err != nil {
					return err
				}

				if row != nil {
					rowCount, ok := row[0].(*tree.DInt)
					if !ok {
						return errors.Errorf("row count is of unknown type %T", row[0])
					}
					if rowCount == nil {
						return errors.New("error parsing row count")
					}
					rowsAffected = int64(*rowCount)

					if rowsAffected > 0 {
						maxTimestamp, ok := row[1].(*tree.DTimestamp)
						if !ok {
							return errors.Errorf("timestamp is of unknown type %T", row[1])
						}
						if maxTimestamp == nil {
							return errors.New("error parsing timestamp")
						}
						timestampLowerBound = maxTimestamp.Time
					}
				}
				return nil
			})
			totalRowsAffected += rowsAffected
			if err != nil || rowsAffected == 0 {
				return totalRowsAffected, err
			}
		}
	}

	return 0, nil
}

// startSystemLogsGC starts a worker which periodically GCs system.rangelog
// and system.eventlog.
// The TTLs for each of these logs is retrieved from cluster settings.
func (s *Server) startSystemLogsGC(ctx context.Context) {
	systemLogsToGC := map[string]*settings.DurationSetting{
		"rangelog": RangeLogTTL,
		"eventlog": EventLogTTL,
	}

	s.stopper.RunWorker(ctx, func(ctx context.Context) {
		period := 10 * time.Minute
		if storeKnobs, ok := s.cfg.TestingKnobs.Store.(*storage.StoreTestingKnobs); ok && storeKnobs.SystemLogsGCPeriod != 0 {
			period = storeKnobs.SystemLogsGCPeriod
		}

		t := time.NewTicker(period)
		defer t.Stop()

		for {
			select {
			case <-t.C:
				for table, tableTTL := range systemLogsToGC {
					ttl := tableTTL.Get(&s.cfg.Settings.SV)
					if ttl > 0 {
						cutoffTimestamp := timeutil.Unix(0, s.clock.PhysicalNow()-int64(ttl))
						rowsAffected, err := s.gcSystemLog(ctx, table, cutoffTimestamp)
						if err != nil {
							log.Errorf(
								ctx,
								"error garbage collecting %s %v",
								table,
								err,
							)
						}

						if rowsAffected > 0 {
							log.Infof(ctx, "garbage collected %d rows from %s", rowsAffected, table)
						}
					}
				}

				if storeKnobs, ok := s.cfg.TestingKnobs.Store.(*storage.StoreTestingKnobs); ok && storeKnobs.SystemLogsGCGCDone != nil {
					select {
					case storeKnobs.SystemLogsGCGCDone <- struct{}{}:
					case <-s.stopper.ShouldStop():
						// Test has finished.
						return
					default:
					}
				}
			case <-s.stopper.ShouldStop():
				return
			}
		}
	})
}
