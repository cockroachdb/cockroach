// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
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
	"math"
	"math/rand"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
)

var schemaChangeLeaseDuration = settings.RegisterNonNegativeDurationSetting(
	"schemachanger.lease.duration",
	"the duration of a schema change lease",
	time.Minute*5,
)

var schemaChangeLeaseRenewFraction = settings.RegisterFloatSetting(
	"schemachanger.lease.renew_fraction",
	"the fraction of schemachanger.lease_duration remaining to trigger a renew of the lease",
	0.5,
)

// This is a delay [0.9 * asyncSchemaChangeDelay, 1.1 * asyncSchemaChangeDelay)
// added to an attempt to run a schema change via the asynchronous path.
// This delay allows the synchronous path to execute the schema change
// in all likelihood. We'd like the synchronous path to execute
// the schema change so that it doesn't have to poll and wait for
// another node to execute the schema change. Polling can add a polling
// delay to the normal execution of a schema change. This interval is also
// used to reattempt execution of a schema change. We don't want this to
// be too low because once a node has started executing a schema change
// the other nodes should not cause a storm by rapidly try to grab the
// schema change lease.
//
// TODO(mjibson): Refine the job coordinator to elect a new job coordinator
// on coordinator failure without causing a storm of polling requests
// attempting to become the job coordinator.
const asyncSchemaChangeDelay = 1 * time.Minute

const (
	// RunningStatusDrainingNames is for jobs that are currently in progress and
	// are draining names.
	RunningStatusDrainingNames jobs.RunningStatus = "draining names"
	// RunningStatusWaitingGC is for jobs that are currently in progress and
	// are waiting for the GC interval to expire
	RunningStatusWaitingGC jobs.RunningStatus = "waiting for GC TTL"
	// RunningStatusCompaction is for jobs that are currently in progress and
	// undergoing RocksDB compaction
	RunningStatusCompaction jobs.RunningStatus = "RocksDB compaction"
	// RunningStatusDeleteOnly is for jobs that are currently waiting on
	// the cluster to converge to seeing the schema element in the DELETE_ONLY
	// state.
	RunningStatusDeleteOnly jobs.RunningStatus = "waiting in DELETE-ONLY"
	// RunningStatusDeleteAndWriteOnly is for jobs that are currently waiting on
	// the cluster to converge to seeing the schema element in the
	// DELETE_AND_WRITE_ONLY state.
	RunningStatusDeleteAndWriteOnly jobs.RunningStatus = "waiting in DELETE-AND-WRITE_ONLY"
	// RunningStatusBackfill is for jobs that are currently running a backfill
	// for a schema element.
	RunningStatusBackfill jobs.RunningStatus = "populating schema"
	// RunningStatusValidation is for jobs that are currently validating
	// a schema element.
	RunningStatusValidation jobs.RunningStatus = "validating schema"
)

type droppedIndex struct {
	indexID  sqlbase.IndexID
	dropTime int64
	deadline int64
}

// SchemaChanger is used to change the schema on a table.
type SchemaChanger struct {
	tableID    sqlbase.ID
	mutationID sqlbase.MutationID
	nodeID     roachpb.NodeID
	db         *client.DB
	leaseMgr   *LeaseManager
	// The SchemaChangeManager can attempt to execute this schema
	// changer after this time.
	execAfter time.Time

	// table.DropTime.
	dropTime int64

	dropIndexTimes []droppedIndex

	testingKnobs   *SchemaChangerTestingKnobs
	distSQLPlanner *DistSQLPlanner
	jobRegistry    *jobs.Registry
	// Keep a reference to the job related to this schema change
	// so that we don't need to read the job again while updating
	// the status of the job. This job can be one of two jobs: the
	// original schema change job for the sql command, or the
	// rollback job for the rollback of the schema change.
	job *jobs.Job
	// Caches updated by DistSQL.
	rangeDescriptorCache *kv.RangeDescriptorCache
	leaseHolderCache     *kv.LeaseHolderCache
	clock                *hlc.Clock
	settings             *cluster.Settings
	execCfg              *ExecutorConfig
	ieFactory            sqlutil.SessionBoundInternalExecutorFactory
}

// NewSchemaChangerForTesting only for tests.
func NewSchemaChangerForTesting(
	tableID sqlbase.ID,
	mutationID sqlbase.MutationID,
	nodeID roachpb.NodeID,
	db client.DB,
	leaseMgr *LeaseManager,
	jobRegistry *jobs.Registry,
	execCfg *ExecutorConfig,
	settings *cluster.Settings,
) SchemaChanger {
	return SchemaChanger{
		tableID:     tableID,
		mutationID:  mutationID,
		nodeID:      nodeID,
		db:          &db,
		leaseMgr:    leaseMgr,
		jobRegistry: jobRegistry,
		settings:    settings,
		execCfg:     execCfg,
	}
}

func (sc *SchemaChanger) createSchemaChangeLease() sqlbase.TableDescriptor_SchemaChangeLease {
	return sqlbase.TableDescriptor_SchemaChangeLease{
		NodeID: sc.nodeID,
		ExpirationTime: timeutil.Now().Add(
			schemaChangeLeaseDuration.Get(&sc.settings.SV),
		).UnixNano(),
	}
}

// isPermanentSchemaChangeError returns true if the error results in
// a permanent failure of a schema change. This function is a whitelist
// instead of a blacklist: only known safe errors are confirmed to not be
// permanent errors. Anything unknown is assumed to be permanent.
func isPermanentSchemaChangeError(err error) bool {
	if err == nil {
		return false
	}

	if grpcutil.IsClosedConnection(err) {
		return false
	}

	// Ignore error thrown because of a read at a very old timestamp.
	// The Backfill will grab a new timestamp to read at for the rest
	// of the backfill.
	// TODO(knz): this should really use errors.Is(). However until/unless
	// we are not receiving errors from 19.1 any more, a string
	// comparison must remain.
	if strings.Contains(err.Error(), "must be after replica GC threshold") {
		return false
	}

	if pgerror.IsSQLRetryableError(err) {
		return false
	}

	if errors.IsAny(err,
		context.Canceled,
		context.DeadlineExceeded,
		errExistingSchemaChangeLease,
		errExpiredSchemaChangeLease,
		errNotHitGCTTLDeadline,
		errSchemaChangeDuringDrain,
		errSchemaChangeNotFirstInLine,
		errTableVersionMismatchSentinel,
	) {
		return false
	}

	switch pgerror.GetPGCode(err) {
	case pgcode.SerializationFailure, pgcode.InternalConnectionFailure, pgcode.DeprecatedInternalConnectionFailure:
		return false

	case pgcode.Internal, pgcode.RangeUnavailable, pgcode.DeprecatedRangeUnavailable:
		if strings.Contains(err.Error(), context.DeadlineExceeded.Error()) {
			return false
		}
	}

	return true
}

var (
	errExistingSchemaChangeLease  = errors.Newf("an outstanding schema change lease exists")
	errExpiredSchemaChangeLease   = errors.Newf("the schema change lease has expired")
	errSchemaChangeNotFirstInLine = errors.Newf("schema change not first in line")
	errNotHitGCTTLDeadline        = errors.Newf("not hit gc ttl deadline")
	errSchemaChangeDuringDrain    = errors.Newf("a schema change ran during the drain phase, re-increment")
)

func shouldLogSchemaChangeError(err error) bool {
	return !errors.IsAny(err,
		errExistingSchemaChangeLease,
		errSchemaChangeNotFirstInLine,
		errNotHitGCTTLDeadline,
	)
}

type errTableVersionMismatch struct {
	version  sqlbase.DescriptorVersion
	expected sqlbase.DescriptorVersion
}

var errTableVersionMismatchSentinel = errTableVersionMismatch{}

func makeErrTableVersionMismatch(version, expected sqlbase.DescriptorVersion) error {
	return errors.Mark(errors.WithStack(errTableVersionMismatch{
		version:  version,
		expected: expected,
	}), errTableVersionMismatchSentinel)
}

func (e errTableVersionMismatch) Error() string {
	return fmt.Sprintf("table version mismatch: %d, expected: %d", e.version, e.expected)
}

// AcquireLease acquires a schema change lease on the table if
// an unexpired lease doesn't exist. It returns the lease.
func (sc *SchemaChanger) AcquireLease(
	ctx context.Context,
) (sqlbase.TableDescriptor_SchemaChangeLease, error) {
	var lease sqlbase.TableDescriptor_SchemaChangeLease
	err := sc.db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		if err := txn.SetSystemConfigTrigger(); err != nil {
			return err
		}
		tableDesc, err := sqlbase.GetTableDescFromID(ctx, txn, sc.tableID)
		if err != nil {
			return err
		}

		// A second to deal with the time uncertainty across nodes.
		// It is perfectly valid for two or more goroutines to hold a valid
		// lease and execute a schema change in parallel, because schema
		// changes are executed using transactions that run sequentially.
		// This just reduces the probability of a write collision.
		expirationTimeUncertainty := time.Second

		if tableDesc.Lease != nil {
			if timeutil.Unix(0, tableDesc.Lease.ExpirationTime).Add(expirationTimeUncertainty).After(timeutil.Now()) {
				return errExistingSchemaChangeLease
			}
			log.Infof(ctx, "Overriding existing expired lease %v", tableDesc.Lease)
		}
		lease = sc.createSchemaChangeLease()
		tableDesc.Lease = &lease
		b := txn.NewBatch()
		if err := writeDescToBatch(ctx, false /* kvTrace */, sc.settings, b, tableDesc.GetID(), tableDesc); err != nil {
			return err
		}
		return txn.Run(ctx, b)
	})
	return lease, err
}

func (sc *SchemaChanger) findTableWithLease(
	ctx context.Context, txn *client.Txn, lease sqlbase.TableDescriptor_SchemaChangeLease,
) (*sqlbase.TableDescriptor, error) {
	tableDesc, err := sqlbase.GetTableDescFromID(ctx, txn, sc.tableID)
	if err != nil {
		return nil, err
	}
	if tableDesc.Lease == nil {
		return nil, errors.AssertionFailedf("no lease present for tableID: %d", errors.Safe(sc.tableID))
	}
	if *tableDesc.Lease != lease {
		log.Errorf(ctx, "table: %d has lease: %v, expected: %v", sc.tableID, tableDesc.Lease, lease)
		return nil, errExpiredSchemaChangeLease
	}
	return tableDesc, nil
}

// ReleaseLease releases the table lease if it is the one registered with
// the table descriptor.
func (sc *SchemaChanger) ReleaseLease(
	ctx context.Context, lease sqlbase.TableDescriptor_SchemaChangeLease,
) error {
	return sc.db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		tableDesc, err := sc.findTableWithLease(ctx, txn, lease)
		if err != nil {
			return err
		}
		tableDesc.Lease = nil
		if err := txn.SetSystemConfigTrigger(); err != nil {
			return err
		}
		b := txn.NewBatch()
		if err := writeDescToBatch(ctx, false /* kvTrace */, sc.settings, b, tableDesc.GetID(), tableDesc); err != nil {
			return err
		}
		return txn.Run(ctx, b)
	})
}

// ExtendLease for the current leaser. This needs to be called often while
// doing a schema change to prevent more than one node attempting to apply a
// schema change (which is still safe, but unwise). It updates existingLease
// with the new lease.
func (sc *SchemaChanger) ExtendLease(
	ctx context.Context, existingLease *sqlbase.TableDescriptor_SchemaChangeLease,
) error {
	// Check if there is still time on this lease.
	minDuration := time.Duration(float64(schemaChangeLeaseDuration.Get(&sc.settings.SV)) *
		schemaChangeLeaseRenewFraction.Get(&sc.settings.SV))
	if timeutil.Unix(0, existingLease.ExpirationTime).After(timeutil.Now().Add(minDuration)) {
		return nil
	}
	// Update lease.
	var lease sqlbase.TableDescriptor_SchemaChangeLease
	if err := sc.db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		tableDesc, err := sc.findTableWithLease(ctx, txn, *existingLease)
		if err != nil {
			return err
		}

		lease = sc.createSchemaChangeLease()
		tableDesc.Lease = &lease
		if err := txn.SetSystemConfigTrigger(); err != nil {
			return err
		}
		b := txn.NewBatch()
		if err := writeDescToBatch(ctx, false /* kvTrace */, sc.settings, b, tableDesc.GetID(), tableDesc); err != nil {
			return err
		}
		return txn.Run(ctx, b)
	}); err != nil {
		return err
	}
	*existingLease = lease
	return nil
}

func (sc *SchemaChanger) canClearRangeForDrop(index *sqlbase.IndexDescriptor) bool {
	return !index.IsInterleaved()
}

// DropTableDesc removes a descriptor from the KV database.
func (sc *SchemaChanger) DropTableDesc(
	ctx context.Context, tableDesc *sqlbase.TableDescriptor, traceKV bool,
) error {
	descKey := sqlbase.MakeDescMetadataKey(tableDesc.ID)
	zoneKeyPrefix := config.MakeZoneKeyPrefix(uint32(tableDesc.ID))

	// Finished deleting all the table data, now delete the table meta data.
	return sc.db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		// Delete table descriptor
		b := &client.Batch{}
		if traceKV {
			log.VEventf(ctx, 2, "Del %s", descKey)
			log.VEventf(ctx, 2, "DelRange %s", zoneKeyPrefix)
		}
		// Delete the descriptor.
		b.Del(descKey)
		// Delete the zone config entry for this table.
		b.DelRange(zoneKeyPrefix, zoneKeyPrefix.PrefixEnd(), false /* returnKeys */)
		if err := txn.SetSystemConfigTrigger(); err != nil {
			return err
		}

		if tableDesc.GetDropJobID() != 0 {
			if err := sc.updateDropTableJob(
				ctx,
				txn,
				tableDesc.GetDropJobID(),
				tableDesc.ID,
				jobspb.Status_DONE,
				func(ctx context.Context, txn *client.Txn, job *jobs.Job) error {
					// Delete the zone config entry for the dropped database associated
					// with the job, if it exists.
					details := job.Details().(jobspb.SchemaChangeDetails)
					if details.DroppedDatabaseID == sqlbase.InvalidID {
						return nil
					}
					dbZoneKeyPrefix := config.MakeZoneKeyPrefix(uint32(details.DroppedDatabaseID))
					if traceKV {
						log.VEventf(ctx, 2, "DelRange %s", zoneKeyPrefix)
					}
					b.DelRange(dbZoneKeyPrefix, dbZoneKeyPrefix.PrefixEnd(), false /* returnKeys */)
					return nil
				}); err != nil {
				log.Warningf(ctx, "failed to update job %d: %+v", errors.Safe(tableDesc.GetDropJobID()), err)
			}
		}
		return txn.Run(ctx, b)
	})
}

// truncateTable deletes all of the data in the specified table.
func (sc *SchemaChanger) truncateTable(
	ctx context.Context,
	lease *sqlbase.TableDescriptor_SchemaChangeLease,
	table *sqlbase.TableDescriptor,
) error {
	// If DropTime isn't set, assume this drop request is from a version
	// 1.1 server and invoke legacy code that uses DeleteRange and range GC.
	if table.DropTime == 0 {
		return truncateTableInChunks(ctx, table, sc.db, false /* traceKV */)
	}

	tableKey := roachpb.RKey(keys.MakeTablePrefix(uint32(table.ID)))
	tableSpan := roachpb.RSpan{Key: tableKey, EndKey: tableKey.PrefixEnd()}

	// ClearRange requests lays down RocksDB range deletion tombstones that have
	// serious performance implications (#24029). The logic below attempts to
	// bound the number of tombstones in one store by sending the ClearRange
	// requests to each range in the table in small, sequential batches rather
	// than letting DistSender send them all in parallel, to hopefully give the
	// compaction queue time to compact the range tombstones away in between
	// requests.
	//
	// As written, this approach has several deficiencies. It does not actually
	// wait for the compaction queue to compact the tombstones away before
	// sending the next request. It is likely insufficient if multiple DROP
	// TABLEs are in flight at once. It does not save its progress in case the
	// coordinator goes down. These deficiences could be addressed, but this code
	// was originally a stopgap to avoid the range tombstone performance hit. The
	// RocksDB range tombstone implementation has since been improved and the
	// performance implications of many range tombstones has been reduced
	// dramatically making this simplistic throttling sufficient.

	// These numbers were chosen empirically for the clearrange roachtest and
	// could certainly use more tuning.
	const batchSize = 100
	const waitTime = 500 * time.Millisecond

	var n int
	lastKey := tableSpan.Key
	ri := kv.NewRangeIterator(sc.execCfg.DistSender)
	for ri.Seek(ctx, tableSpan.Key, kv.Ascending); ; ri.Next(ctx) {
		if !ri.Valid() {
			return ri.Error().GoError()
		}

		// This call is a no-op unless the lease is nearly expired.
		if err := sc.ExtendLease(ctx, lease); err != nil {
			return err
		}

		if n++; n >= batchSize || !ri.NeedAnother(tableSpan) {
			endKey := ri.Desc().EndKey
			if tableSpan.EndKey.Less(endKey) {
				endKey = tableSpan.EndKey
			}
			var b client.Batch
			b.AddRawRequest(&roachpb.ClearRangeRequest{
				RequestHeader: roachpb.RequestHeader{
					Key:    lastKey.AsRawKey(),
					EndKey: endKey.AsRawKey(),
				},
			})
			log.VEventf(ctx, 2, "ClearRange %s - %s", lastKey, endKey)
			if err := sc.db.Run(ctx, &b); err != nil {
				return err
			}
			n = 0
			lastKey = endKey
			time.Sleep(waitTime)
		}

		if !ri.NeedAnother(tableSpan) {
			break
		}
	}

	return nil
}

// maybe Drop a table. Return nil if successfully dropped.
func (sc *SchemaChanger) maybeDropTable(
	ctx context.Context, inSession bool, table *sqlbase.TableDescriptor,
) error {
	if !table.Dropped() || inSession {
		return nil
	}

	// This can happen if a change other than the drop originally
	// scheduled the changer for this table. If that's the case,
	// we still need to wait for the deadline to expire.
	if table.DropTime != 0 {
		var timeRemaining time.Duration
		if err := sc.db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
			timeRemaining = 0
			_, zoneCfg, _, err := GetZoneConfigInTxn(ctx, txn, uint32(table.ID),
				&sqlbase.IndexDescriptor{}, "", false /* getInheritedDefault */)
			if err != nil {
				return err
			}
			deadline := table.DropTime + int64(zoneCfg.GC.TTLSeconds)*time.Second.Nanoseconds()
			timeRemaining = timeutil.Since(timeutil.Unix(0, deadline))
			return nil
		}); err != nil {
			return err
		}
		if timeRemaining < 0 {
			return errNotHitGCTTLDeadline
		}
	}

	// Acquire lease.
	lease, err := sc.AcquireLease(ctx)
	if err != nil {
		return err
	}
	needRelease := true
	// Always try to release lease.
	defer func() {
		// If the schema changer deleted the descriptor, there's no longer a lease to be
		// released.
		if !needRelease {
			return
		}
		if err := sc.ReleaseLease(ctx, lease); err != nil {
			log.Warning(ctx, err)
		}
	}()

	// Do all the hard work of deleting the table data and the table ID.
	if err := sc.truncateTable(ctx, &lease, table); err != nil {
		return err
	}

	if err := sc.DropTableDesc(ctx, table, false /* traceKV */); err != nil {
		return err
	}
	// The descriptor was deleted.
	needRelease = false
	return nil
}

// maybe backfill a created table by executing the AS query. Return nil if
// successfully backfilled.
//
// Note that this does not connect to the tracing settings of the
// surrounding SQL transaction. This should be OK as (at the time of
// this writing) this code path is only used for standalone CREATE
// TABLE AS statements, which cannot be traced.
func (sc *SchemaChanger) maybeBackfillCreateTableAs(
	ctx context.Context, table *sqlbase.TableDescriptor,
) error {
	if table.Adding() && table.IsAs() {
		// Acquire lease.
		lease, err := sc.AcquireLease(ctx)
		if err != nil {
			return err
		}
		// Always try to release lease.
		defer func() {
			if err := sc.ReleaseLease(ctx, lease); err != nil {
				log.Warning(ctx, err)
			}
		}()

		// We need to maintain our lease *while* our backfill runs.
		maintainLease := make(chan struct{})
		g := ctxgroup.WithContext(ctx)
		g.GoCtx(func(ctx context.Context) error {
			done := ctx.Done()
			ticker := time.NewTicker(time.Minute)
			defer ticker.Stop()

			for {
				select {
				case <-done:
					return nil
				case <-maintainLease:
					return nil
				case <-ticker.C:
					if err := sc.ExtendLease(ctx, &lease); err != nil {
						return err
					}
				}
			}
		})

		g.GoCtx(func(ctx context.Context) error {
			defer close(maintainLease)
			return sc.db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
				txn.SetFixedTimestamp(ctx, table.CreateAsOfTime)

				// Create an internal planner as the planner used to serve the user query
				// would have committed by this point.
				p, cleanup := NewInternalPlanner("ctasBackfill", txn, security.RootUser, &MemoryMetrics{}, sc.execCfg)
				defer cleanup()
				localPlanner := p.(*planner)
				stmt, err := parser.ParseOne(table.CreateQuery)
				if err != nil {
					return err
				}

				// Construct an optimized logical plan of the AS source stmt.
				localPlanner.stmt = &Statement{Statement: stmt}
				localPlanner.optPlanningCtx.init(localPlanner)

				localPlanner.runWithOptions(resolveFlags{skipCache: true}, func() {
					err = localPlanner.makeOptimizerPlan(ctx)
				})

				if err != nil {
					return err
				}
				defer localPlanner.curPlan.close(ctx)

				res := roachpb.BulkOpSummary{}
				rw := newCallbackResultWriter(func(ctx context.Context, row tree.Datums) error {
					// TODO(adityamaru): Use the BulkOpSummary for either telemetry or to
					// return to user.
					var counts roachpb.BulkOpSummary
					if err := protoutil.Unmarshal([]byte(*row[0].(*tree.DBytes)), &counts); err != nil {
						return err
					}
					res.Add(counts)
					return nil
				})
				recv := MakeDistSQLReceiver(
					ctx,
					rw,
					tree.Rows,
					sc.execCfg.RangeDescriptorCache,
					sc.execCfg.LeaseHolderCache,
					txn,
					func(ts hlc.Timestamp) {
						_ = sc.clock.Update(ts)
					},
					// Make a session tracing object on-the-fly. This is OK
					// because it sets "enabled: false" and thus none of the
					// other fields are used.
					&SessionTracing{},
				)
				defer recv.Release()

				rec, err := sc.distSQLPlanner.checkSupportForNode(localPlanner.curPlan.plan)
				var planAndRunErr error
				localPlanner.runWithOptions(resolveFlags{skipCache: true}, func() {
					// Resolve subqueries before running the queries' physical plan.
					if len(localPlanner.curPlan.subqueryPlans) != 0 {
						if !sc.distSQLPlanner.PlanAndRunSubqueries(
							ctx, localPlanner, localPlanner.ExtendedEvalContextCopy,
							localPlanner.curPlan.subqueryPlans, recv, rec == canDistribute,
						) {
							if planAndRunErr = rw.Err(); planAndRunErr != nil {
								return
							}
							if planAndRunErr = recv.commErr; planAndRunErr != nil {
								return
							}
						}
					}

					isLocal := err != nil || rec == cannotDistribute
					out := execinfrapb.ProcessorCoreUnion{BulkRowWriter: &execinfrapb.BulkRowWriterSpec{
						Table: *table,
					}}

					PlanAndRunCTAS(ctx, sc.distSQLPlanner, localPlanner,
						txn, isLocal, localPlanner.curPlan.plan, out, recv)
					if planAndRunErr = rw.Err(); planAndRunErr != nil {
						return
					}
					if planAndRunErr = recv.commErr; planAndRunErr != nil {
						return
					}
				})

				return planAndRunErr
			})
		})
		return g.Wait()
	}
	return nil
}

// maybe make a table PUBLIC if it's in the ADD state.
func (sc *SchemaChanger) maybeMakeAddTablePublic(
	ctx context.Context, table *sqlbase.TableDescriptor,
) error {
	if table.Adding() {
		fks := table.AllActiveAndInactiveForeignKeys()
		for _, fk := range fks {
			if err := sc.waitToUpdateLeases(ctx, fk.ReferencedTableID); err != nil {
				return err
			}
		}

		if _, err := sc.leaseMgr.Publish(
			ctx,
			table.ID,
			func(tbl *sqlbase.MutableTableDescriptor) error {
				if !tbl.Adding() {
					return errDidntUpdateDescriptor
				}
				tbl.State = sqlbase.TableDescriptor_PUBLIC
				return nil
			},
			func(txn *client.Txn) error { return nil },
		); err != nil {
			return err
		}
	}

	return nil
}

func (sc *SchemaChanger) maybeGCMutations(
	ctx context.Context, inSession bool, table *sqlbase.TableDescriptor,
) error {
	if inSession || len(table.GCMutations) == 0 || len(sc.dropIndexTimes) == 0 {
		return nil
	}

	// Don't perform GC work if there are non-GC mutations waiting.
	if len(table.Mutations) > 0 {
		return nil
	}

	// Find dropped index with earliest GC deadline.
	dropped := sc.dropIndexTimes[0]
	for i := 1; i < len(sc.dropIndexTimes); i++ {
		if other := sc.dropIndexTimes[i]; other.deadline < dropped.deadline {
			dropped = other
		}
	}

	var mutation sqlbase.TableDescriptor_GCDescriptorMutation
	found := false
	for _, gcm := range table.GCMutations {
		if gcm.IndexID == sc.dropIndexTimes[0].indexID {
			found = true
			mutation = gcm
			break
		}
	}
	if !found {
		return errors.AssertionFailedf("no GC mutation for index %d", errors.Safe(sc.dropIndexTimes[0].indexID))
	}

	// Check if the deadline for GC'd dropped index expired because
	// a change other than the drop could have scheduled the changer
	// for this table.
	timeRemaining := timeutil.Since(timeutil.Unix(0, dropped.deadline))
	if timeRemaining < 0 {
		// Return nil to allow other any mutations to make progress.
		return nil
	}

	// Acquire lease.
	lease, err := sc.AcquireLease(ctx)
	if err != nil {
		return err
	}
	// Always try to release lease.
	defer func() {
		if err := sc.ReleaseLease(ctx, lease); err != nil {
			log.Warning(ctx, err)
		}
	}()

	if err := sc.truncateIndexes(ctx, &lease, table.Version, []sqlbase.IndexDescriptor{{ID: mutation.IndexID}}); err != nil {
		return err
	}

	_, err = sc.leaseMgr.Publish(
		ctx,
		table.ID,
		func(tbl *sqlbase.MutableTableDescriptor) error {
			found := false
			for i := 0; i < len(tbl.GCMutations); i++ {
				if other := tbl.GCMutations[i]; other.IndexID == mutation.IndexID {
					tbl.GCMutations = append(tbl.GCMutations[:i], tbl.GCMutations[i+1:]...)
					found = true
					break
				}
			}

			if !found {
				return errDidntUpdateDescriptor
			}

			return nil
		},
		func(txn *client.Txn) error {
			job, err := sc.jobRegistry.LoadJobWithTxn(ctx, mutation.JobID, txn)
			if err != nil {
				log.Warningf(ctx, "ignoring error during logEvent while GCing mutations: %+v", err)
				return nil
			}
			return job.WithTxn(txn).Succeeded(ctx, nil)
		},
	)

	return err
}

func (sc *SchemaChanger) updateDropTableJob(
	ctx context.Context,
	txn *client.Txn,
	jobID int64,
	tableID sqlbase.ID,
	status jobspb.Status,
	onSuccess func(context.Context, *client.Txn, *jobs.Job) error,
) error {
	job, err := sc.jobRegistry.LoadJobWithTxn(ctx, jobID, txn)
	if err != nil {
		return err
	}

	schemaDetails, ok := job.Details().(jobspb.SchemaChangeDetails)
	if !ok {
		return errors.AssertionFailedf("unexpected details for job %d: %T", errors.Safe(*job.ID()), job.Details())
	}

	lowestStatus := jobspb.Status_DONE
	for i := range schemaDetails.DroppedTables {
		if tableID == schemaDetails.DroppedTables[i].ID {
			schemaDetails.DroppedTables[i].Status = status
		}

		if lowestStatus > schemaDetails.DroppedTables[i].Status {
			lowestStatus = schemaDetails.DroppedTables[i].Status
		}
	}

	var runningStatus jobs.RunningStatus
	switch lowestStatus {
	case jobspb.Status_DRAINING_NAMES:
		runningStatus = RunningStatusDrainingNames
	case jobspb.Status_WAIT_FOR_GC_INTERVAL:
		runningStatus = RunningStatusWaitingGC
	case jobspb.Status_ROCKSDB_COMPACTION:
		runningStatus = RunningStatusCompaction
	case jobspb.Status_DONE:
		return job.WithTxn(txn).Succeeded(ctx, func(ctx context.Context, txn *client.Txn) error {
			return onSuccess(ctx, txn, job)
		})
	default:
		return errors.AssertionFailedf("unexpected dropped table status %d", errors.Safe(lowestStatus))
	}

	if err := job.WithTxn(txn).SetDetails(ctx, schemaDetails); err != nil {
		return err
	}

	return job.WithTxn(txn).RunningStatus(ctx, func(ctx context.Context, _ jobspb.Details) (jobs.RunningStatus, error) {
		return runningStatus, nil
	})
}

// Drain old names from the cluster.
func (sc *SchemaChanger) drainNames(ctx context.Context) error {
	// Publish a new version with all the names drained after everyone
	// has seen the version with the new name. All the draining names
	// can be reused henceforth.
	var namesToReclaim []sqlbase.TableDescriptor_NameInfo
	var dropJobID int64
	_, err := sc.leaseMgr.Publish(
		ctx,
		sc.tableID,
		func(desc *sqlbase.MutableTableDescriptor) error {
			if sc.testingKnobs.OldNamesDrainedNotification != nil {
				sc.testingKnobs.OldNamesDrainedNotification()
			}
			// Free up the old name(s) for reuse.
			namesToReclaim = desc.DrainingNames
			desc.DrainingNames = nil
			dropJobID = desc.GetDropJobID()
			return nil
		},
		// Reclaim all the old names.
		func(txn *client.Txn) error {
			b := txn.NewBatch()
			for _, drain := range namesToReclaim {
				err := sqlbase.RemoveObjectNamespaceEntry(ctx, txn, drain.ParentID, drain.ParentSchemaID,
					drain.Name, false /* KVTrace */)
				if err != nil {
					return err
				}
			}

			if dropJobID != 0 {
				if err := sc.updateDropTableJob(
					ctx, txn, dropJobID, sc.tableID, jobspb.Status_WAIT_FOR_GC_INTERVAL,
					func(context.Context, *client.Txn, *jobs.Job) error {
						return nil
					}); err != nil {
					return err
				}
			}

			return txn.Run(ctx, b)
		},
	)
	return err
}

// Execute the entire schema change in steps.
// inSession is set to false when this is called from the asynchronous
// schema change execution path.
//
// If the txn that queued the schema changer did not commit, this will be a
// no-op, as we'll fail to find the job for our mutation in the jobs registry.
func (sc *SchemaChanger) exec(ctx context.Context, inSession bool) error {
	ctx = logtags.AddTag(ctx, "scExec", nil)

	tableDesc, notFirst, err := sc.notFirstInLine(ctx)
	if err != nil {
		return err
	}
	if notFirst {
		log.Infof(ctx,
			"schema change on %s (%d v%d) mutation %d: another change is still in progress",
			tableDesc.Name, sc.tableID, tableDesc.Version, sc.mutationID,
		)
		return errSchemaChangeNotFirstInLine
	}

	if tableDesc.HasDrainingNames() {
		if err := sc.drainNames(ctx); err != nil {
			return err
		}
	}

	log.Infof(ctx,
		"schema change on %s (%d v%d) mutation %d starting execution...",
		tableDesc.Name, sc.tableID, tableDesc.Version, sc.mutationID,
	)

	// Delete dropped table data if possible.
	if err := sc.maybeDropTable(ctx, inSession, tableDesc); err != nil {
		return err
	}

	if err := sc.maybeBackfillCreateTableAs(ctx, tableDesc); err != nil {
		return err
	}

	if err := sc.maybeMakeAddTablePublic(ctx, tableDesc); err != nil {
		return err
	}

	if err := sc.maybeGCMutations(ctx, inSession, tableDesc); err != nil {
		return err
	}

	// Wait for the schema change to propagate to all nodes after this function
	// returns, so that the new schema is live everywhere. This is not needed for
	// correctness but is done to make the UI experience/tests predictable.
	waitToUpdateLeases := func(refreshStats bool) error {
		if err := sc.waitToUpdateLeases(ctx, sc.tableID); err != nil {
			if errors.Is(err, sqlbase.ErrDescriptorNotFound) {
				return err
			}
			log.Warningf(ctx, "waiting to update leases: %+v", err)
			// As we are dismissing the error, go through the recording motions.
			// This ensures that any important error gets reported to Sentry, etc.
			sqltelemetry.RecordError(ctx, err, &sc.settings.SV)
		}
		// We wait to trigger a stats refresh until we know the leases have been
		// updated.
		if refreshStats {
			sc.refreshStats()
		}
		return nil
	}

	if sc.mutationID == sqlbase.InvalidMutationID {
		// Nothing more to do.
		isCreateTableAs := tableDesc.Adding() && tableDesc.IsAs()
		return waitToUpdateLeases(isCreateTableAs /* refreshStats */)
	}

	// Acquire lease.
	lease, err := sc.AcquireLease(ctx)
	if err != nil {
		log.Infof(ctx,
			"schema change on %s (%d v%d) mutation %d: another node is currently operating on this table",
			tableDesc.Name, sc.tableID, tableDesc.Version, sc.mutationID,
		)
		return err
	}
	// Always try to release lease.
	defer func() {
		if err := sc.ReleaseLease(ctx, lease); err != nil {
			log.Warningf(ctx, "while releasing schema change lease: %+v", err)
			// Go through the recording motions. See comment above.
			sqltelemetry.RecordError(ctx, err, &sc.settings.SV)
		}
	}()

	// Find our job.
	foundJobID := false
	for _, g := range tableDesc.MutationJobs {
		if g.MutationID == sc.mutationID {
			job, err := sc.jobRegistry.LoadJob(ctx, g.JobID)
			if err != nil {
				return err
			}
			sc.job = job
			foundJobID = true
			break
		}
	}

	if !foundJobID {
		// No job means we've already run and completed this schema change
		// successfully, so we can just exit.
		return nil
	}

	if err := sc.job.Started(ctx); err != nil {
		if log.V(2) {
			log.Infof(ctx, "Failed to mark job %d as started: %+v", *sc.job.ID(), err)
		}
		// Go through the recording motions. See comment above.
		sqltelemetry.RecordError(ctx, err, &sc.settings.SV)
	}

	if err := sc.initJobRunningStatus(ctx); err != nil {
		if log.V(2) {
			log.Infof(ctx, "Failed to update job %d running status: %+v", *sc.job.ID(), err)
		}
		// Go through the recording motions. See comment above.
		sqltelemetry.RecordError(ctx, err, &sc.settings.SV)
	}

	// Run through mutation state machine and backfill.
	err = sc.runStateMachineAndBackfill(ctx, &lease)

	defer func() {
		if err := waitToUpdateLeases(err == nil /* refreshStats */); err != nil && !errors.Is(err, sqlbase.ErrDescriptorNotFound) {
			// We only expect ErrDescriptorNotFound to be returned. This happens
			// when the table descriptor was deleted. We can ignore this error.

			log.Warningf(ctx, "unexpected error while waiting for leases to update: %+v", err)
			// As we are dismissing the error, go through the recording motions.
			// This ensures that any important error gets reported to Sentry, etc.
			sqltelemetry.RecordError(ctx, err, &sc.settings.SV)
		}
	}()

	// Purge the mutations if the application of the mutations failed due to
	// a permanent error. All other errors are transient errors that are
	// resolved by retrying the backfill.
	if isPermanentSchemaChangeError(err) {
		if rollbackErr := sc.rollbackSchemaChange(ctx, err, &lease); rollbackErr != nil {
			// Note: the "err" object is captured by rollbackSchemaChange(), so
			// it does not simply disappear.
			return errors.Wrap(rollbackErr, "while rolling back schema change")
		}
	}

	return err
}

// initialize the job running status.
func (sc *SchemaChanger) initJobRunningStatus(ctx context.Context) error {
	return sc.db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		desc, err := sqlbase.GetTableDescFromID(ctx, txn, sc.tableID)
		if err != nil {
			return err
		}

		var runStatus jobs.RunningStatus
		for _, mutation := range desc.Mutations {
			if mutation.MutationID != sc.mutationID {
				// Mutations are applied in a FIFO order. Only apply the first set of
				// mutations if they have the mutation ID we're looking for.
				break
			}

			switch mutation.Direction {
			case sqlbase.DescriptorMutation_ADD:
				switch mutation.State {
				case sqlbase.DescriptorMutation_DELETE_ONLY:
					runStatus = RunningStatusDeleteOnly
				}

			case sqlbase.DescriptorMutation_DROP:
				switch mutation.State {
				case sqlbase.DescriptorMutation_DELETE_AND_WRITE_ONLY:
					runStatus = RunningStatusDeleteAndWriteOnly
				}
			}
		}
		if runStatus != "" && !desc.Dropped() {
			if err := sc.job.WithTxn(txn).RunningStatus(
				ctx, func(ctx context.Context, details jobspb.Details) (jobs.RunningStatus, error) {
					return runStatus, nil
				}); err != nil {
				return errors.Wrapf(err,
					"failed to update running status of job %d", errors.Safe(*sc.job.ID()))
			}
		}
		return nil
	})
}

func (sc *SchemaChanger) rollbackSchemaChange(
	ctx context.Context, err error, lease *sqlbase.TableDescriptor_SchemaChangeLease,
) error {
	log.Warningf(ctx, "reversing schema change %d due to irrecoverable error: %s", *sc.job.ID(), err)
	if errReverse := sc.reverseMutations(ctx, err); errReverse != nil {
		// Although the backfill did hit an integrity constraint violation
		// and made a decision to reverse the mutations,
		// reverseMutations() failed. If exec() is called again the entire
		// schema change will be retried.

		// Note: we capture the original error as "secondary" to ensure it
		// does not fully disappear.
		// However, since it is not in the main causal chain any more,
		// it will become invisible to further telemetry. So before
		// we relegate it to a secondary, go through the recording motions.
		// This ensures that any important error gets reported to Sentry, etc.
		secondary := errors.Wrap(err, "original error when reversing mutations")
		sqltelemetry.RecordError(ctx, secondary, &sc.settings.SV)
		return errors.WithSecondaryError(errReverse, secondary)
	}

	// After this point the schema change has been reversed and any retry
	// of the schema change will act upon the reversed schema change.
	if errPurge := sc.runStateMachineAndBackfill(ctx, lease); errPurge != nil {
		// Don't return this error because we do want the caller to know
		// that an integrity constraint was violated with the original
		// schema change. The reversed schema change will be
		// retried via the async schema change manager.

		// Since the errors are going to disappear, do the recording
		// motions on them. This ensures that any assertion failure or
		// other important error underneath gets recorded properly.
		log.Warningf(ctx, "error purging mutation: %+v\nwhile handling error: %+v", errPurge, err)
		sqltelemetry.RecordError(ctx, err, &sc.settings.SV)
		sqltelemetry.RecordError(ctx, errPurge, &sc.settings.SV)
	}
	return nil
}

// RunStateMachineBeforeBackfill moves the state machine forward
// and wait to ensure that all nodes are seeing the latest version
// of the table.
func (sc *SchemaChanger) RunStateMachineBeforeBackfill(ctx context.Context) error {
	var runStatus jobs.RunningStatus
	if _, err := sc.leaseMgr.Publish(ctx, sc.tableID, func(desc *sqlbase.MutableTableDescriptor) error {

		runStatus = ""
		// Apply mutations belonging to the same version.
		for i, mutation := range desc.Mutations {
			if mutation.MutationID != sc.mutationID {
				// Mutations are applied in a FIFO order. Only apply the first set of
				// mutations if they have the mutation ID we're looking for.
				break
			}
			switch mutation.Direction {
			case sqlbase.DescriptorMutation_ADD:
				switch mutation.State {
				case sqlbase.DescriptorMutation_DELETE_ONLY:
					// TODO(vivek): while moving up the state is appropriate,
					// it will be better to run the backfill of a unique index
					// twice: once in the DELETE_ONLY state to confirm that
					// the index can indeed be created, and subsequently in the
					// DELETE_AND_WRITE_ONLY state to fill in the missing elements of the
					// index (INSERT and UPDATE that happened in the interim).
					desc.Mutations[i].State = sqlbase.DescriptorMutation_DELETE_AND_WRITE_ONLY
					runStatus = RunningStatusDeleteAndWriteOnly

				case sqlbase.DescriptorMutation_DELETE_AND_WRITE_ONLY:
					// The state change has already moved forward.
				}

			case sqlbase.DescriptorMutation_DROP:
				switch mutation.State {
				case sqlbase.DescriptorMutation_DELETE_ONLY:
					// The state change has already moved forward.

				case sqlbase.DescriptorMutation_DELETE_AND_WRITE_ONLY:
					desc.Mutations[i].State = sqlbase.DescriptorMutation_DELETE_ONLY
					runStatus = RunningStatusDeleteOnly
				}
			}
		}
		if doNothing := runStatus == "" || desc.Dropped(); doNothing {
			// Return error so that Publish() doesn't increment the version.
			return errDidntUpdateDescriptor
		}
		return nil
	}, func(txn *client.Txn) error {
		if sc.job != nil {
			if err := sc.job.WithTxn(txn).RunningStatus(ctx, func(ctx context.Context, details jobspb.Details) (jobs.RunningStatus, error) {
				return runStatus, nil
			}); err != nil {
				return errors.Wrapf(err,
					"failed to update running status of job %d", errors.Safe(*sc.job.ID()))
			}
		}
		return nil
	}); err != nil {
		return err
	}

	// wait for the state change to propagate to all leases.
	return sc.waitToUpdateLeases(ctx, sc.tableID)
}

// Wait until the entire cluster has been updated to the latest version
// of the table descriptor.
func (sc *SchemaChanger) waitToUpdateLeases(ctx context.Context, tableID sqlbase.ID) error {
	// Aggressively retry because there might be a user waiting for the
	// schema change to complete.
	retryOpts := retry.Options{
		InitialBackoff: 20 * time.Millisecond,
		MaxBackoff:     200 * time.Millisecond,
		Multiplier:     2,
	}
	log.Infof(ctx, "waiting for a single version of table %d...", tableID)
	version, err := sc.leaseMgr.WaitForOneVersion(ctx, tableID, retryOpts)
	log.Infof(ctx, "waiting for a single version of table %d... done (at v %d)", tableID, version)
	return err
}

// done finalizes the mutations (adds new cols/indexes to the table).
// It ensures that all nodes are on the current (pre-update) version of the
// schema.
// Returns the updated descriptor.
func (sc *SchemaChanger) done(ctx context.Context) (*sqlbase.ImmutableTableDescriptor, error) {
	isRollback := false
	jobSucceeded := true
	now := timeutil.Now().UnixNano()

	// Get the other tables whose foreign key backreferences need to be removed.
	// We make a call to PublishMultiple to handle the situation to add Foreign Key backreferences.
	var fksByBackrefTable map[sqlbase.ID][]*sqlbase.ConstraintToUpdate
	var interleaveParents map[sqlbase.ID]struct{}
	err := sc.db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		fksByBackrefTable = make(map[sqlbase.ID][]*sqlbase.ConstraintToUpdate)
		interleaveParents = make(map[sqlbase.ID]struct{})

		desc, err := sqlbase.GetTableDescFromID(ctx, txn, sc.tableID)
		if err != nil {
			return err
		}
		for _, mutation := range desc.Mutations {
			if mutation.MutationID != sc.mutationID {
				break
			}
			if constraint := mutation.GetConstraint(); constraint != nil &&
				constraint.ConstraintType == sqlbase.ConstraintToUpdate_FOREIGN_KEY &&
				mutation.Direction == sqlbase.DescriptorMutation_ADD &&
				constraint.ForeignKey.Validity == sqlbase.ConstraintValidity_Unvalidated {
				// Add backref table to referenced table with an unvalidated foreign key constraint
				fk := &constraint.ForeignKey
				if fk.ReferencedTableID != desc.ID {
					fksByBackrefTable[constraint.ForeignKey.ReferencedTableID] = append(fksByBackrefTable[constraint.ForeignKey.ReferencedTableID], constraint)
				}
			} else if swap := mutation.GetPrimaryKeySwap(); swap != nil {
				// If any old indexes (including the old primary index) being rewritten are interleaved
				// children, we will have to update their parents as well.
				for _, idxID := range append([]sqlbase.IndexID{swap.OldPrimaryIndexId}, swap.OldIndexes...) {
					oldIndex, err := desc.FindIndexByID(idxID)
					if err != nil {
						return err
					}
					if len(oldIndex.Interleave.Ancestors) != 0 {
						ancestor := oldIndex.Interleave.Ancestors[len(oldIndex.Interleave.Ancestors)-1]
						if ancestor.TableID != desc.ID {
							interleaveParents[ancestor.TableID] = struct{}{}
						}
					}
				}
				// Because we are not currently supporting primary key changes on tables/indexes
				// that are interleaved parents, we don't check oldPrimaryIndex.InterleavedBy.
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	tableIDsToUpdate := make([]sqlbase.ID, 0, len(fksByBackrefTable)+1)
	tableIDsToUpdate = append(tableIDsToUpdate, sc.tableID)
	for id := range fksByBackrefTable {
		tableIDsToUpdate = append(tableIDsToUpdate, id)
	}
	for id := range interleaveParents {
		if _, ok := fksByBackrefTable[id]; !ok {
			tableIDsToUpdate = append(tableIDsToUpdate, id)
		}
	}

	update := func(descs map[sqlbase.ID]*sqlbase.MutableTableDescriptor) error {
		// Reset vars here because update function can be called multiple times in a retry.
		isRollback = false
		jobSucceeded = true

		i := 0
		scDesc, ok := descs[sc.tableID]
		if !ok {
			return errors.AssertionFailedf("required table with ID %d not provided to update closure", sc.tableID)
		}
		for _, mutation := range scDesc.Mutations {
			if mutation.MutationID != sc.mutationID {
				// Mutations are applied in a FIFO order. Only apply the first set of
				// mutations if they have the mutation ID we're looking for.
				break
			}
			isRollback = mutation.Rollback
			if indexDesc := mutation.GetIndex(); mutation.Direction == sqlbase.DescriptorMutation_DROP &&
				indexDesc != nil {
				if sc.canClearRangeForDrop(indexDesc) {
					jobSucceeded = false
					scDesc.GCMutations = append(
						scDesc.GCMutations,
						sqlbase.TableDescriptor_GCDescriptorMutation{
							IndexID:  indexDesc.ID,
							DropTime: now,
							JobID:    *sc.job.ID(),
						})
				}
			}
			if constraint := mutation.GetConstraint(); constraint != nil &&
				constraint.ConstraintType == sqlbase.ConstraintToUpdate_FOREIGN_KEY &&
				mutation.Direction == sqlbase.DescriptorMutation_ADD &&
				constraint.ForeignKey.Validity == sqlbase.ConstraintValidity_Unvalidated {
				// Add backreference on the referenced table (which could be the same table)
				backrefTable, ok := descs[constraint.ForeignKey.ReferencedTableID]
				if !ok {
					return errors.AssertionFailedf("required table with ID %d not provided to update closure", sc.tableID)
				}
				backrefTable.InboundFKs = append(backrefTable.InboundFKs, constraint.ForeignKey)
			}
			if err := scDesc.MakeMutationComplete(mutation); err != nil {
				return err
			}
			if pkSwap := mutation.GetPrimaryKeySwap(); pkSwap != nil {
				if fn := sc.testingKnobs.RunBeforePrimaryKeySwap; fn != nil {
					fn()
				}
				// If any old index had an interleaved parent, remove the backreference from the parent.
				for _, idxID := range append(
					[]sqlbase.IndexID{pkSwap.OldPrimaryIndexId}, pkSwap.OldIndexes...) {
					oldIndex, err := scDesc.FindIndexByID(idxID)
					if err != nil {
						return err
					}
					if len(oldIndex.Interleave.Ancestors) != 0 {
						ancestorInfo := oldIndex.Interleave.Ancestors[len(oldIndex.Interleave.Ancestors)-1]
						ancestor := descs[ancestorInfo.TableID]
						ancestorIdx, err := ancestor.FindIndexByID(ancestorInfo.IndexID)
						if err != nil {
							return err
						}
						foundAncestor := false
						for k, ref := range ancestorIdx.InterleavedBy {
							if ref.Table == scDesc.ID && ref.Index == oldIndex.ID {
								if foundAncestor {
									return errors.AssertionFailedf(
										"ancestor entry in %s for %s@%s found more than once",
										ancestor.Name, scDesc.Name, oldIndex.Name)
								}
								ancestorIdx.InterleavedBy = append(
									ancestorIdx.InterleavedBy[:k], ancestorIdx.InterleavedBy[k+1:]...)
								foundAncestor = true
							}
						}
					}
				}

				// If we performed MakeMutationComplete on a PrimaryKeySwap mutation, then we need to start
				// a job for the index deletion mutations that the primary key swap mutation added, if any.
				mutationID := scDesc.ClusterVersion.NextMutationID
				span := scDesc.PrimaryIndexSpan()
				var spanList []jobspb.ResumeSpanList
				for j := len(scDesc.ClusterVersion.Mutations); j < len(scDesc.Mutations); j++ {
					spanList = append(spanList,
						jobspb.ResumeSpanList{
							ResumeSpans: roachpb.Spans{span},
						},
					)
				}
				jobRecord := jobs.Record{
					Description:   fmt.Sprintf("Cleanup job for '%s'", sc.job.Payload().Description),
					Username:      sc.job.Payload().Username,
					DescriptorIDs: sqlbase.IDs{scDesc.GetID()},
					Details:       jobspb.SchemaChangeDetails{ResumeSpanList: spanList},
					Progress:      jobspb.SchemaChangeProgress{},
				}
				job := sc.jobRegistry.NewJob(jobRecord)
				if err := job.Created(ctx); err != nil {
					return err
				}
				scDesc.MutationJobs = append(scDesc.MutationJobs, sqlbase.TableDescriptor_MutationJob{
					MutationID: mutationID,
					JobID:      *job.ID(),
				})
			}
			i++
		}
		if i == 0 {
			// The table descriptor is unchanged. Don't let Publish() increment
			// the version.
			return errDidntUpdateDescriptor
		}
		// Trim the executed mutations from the descriptor.
		scDesc.Mutations = scDesc.Mutations[i:]

		for i, g := range scDesc.MutationJobs {
			if g.MutationID == sc.mutationID {
				// Trim the executed mutation group from the descriptor.
				scDesc.MutationJobs = append(scDesc.MutationJobs[:i], scDesc.MutationJobs[i+1:]...)
				break
			}
		}
		return nil
	}

	descs, err := sc.leaseMgr.PublishMultiple(ctx, tableIDsToUpdate, update, func(txn *client.Txn) error {
		// If the job already has a terminal status, we shouldn't need to update
		// its status again. One way this may happen is when a table is dropped
		// all jobs that mutate that table are marked successful. So if is a job
		// that mutates a table that is dropped in the same txn, then it will
		// already be successful. These jobs don't need their status to be updated.
		if !sc.job.WithTxn(txn).CheckTerminalStatus(ctx) {
			if jobSucceeded {
				if err := sc.job.WithTxn(txn).Succeeded(ctx, nil); err != nil {
					return errors.Wrapf(err,
						"failed to mark job %d as successful", errors.Safe(*sc.job.ID()))
				}
			} else {
				if err := sc.job.WithTxn(txn).RunningStatus(ctx, func(ctx context.Context, details jobspb.Details) (jobs.RunningStatus, error) {
					return RunningStatusWaitingGC, nil
				}); err != nil {
					return errors.Wrapf(err,
						"failed to update running status of job %d", errors.Safe(*sc.job.ID()))
				}
			}
		}

		schemaChangeEventType := EventLogFinishSchemaChange
		if isRollback {
			schemaChangeEventType = EventLogFinishSchemaRollback
		}

		// Log "Finish Schema Change" or "Finish Schema Change Rollback"
		// event. Only the table ID and mutation ID are logged; this can
		// be correlated with the DDL statement that initiated the change
		// using the mutation id.
		return MakeEventLogger(sc.execCfg).InsertEventRecord(
			ctx,
			txn,
			schemaChangeEventType,
			int32(sc.tableID),
			int32(sc.nodeID),
			struct {
				MutationID uint32
			}{uint32(sc.mutationID)},
		)
	})
	if err != nil {
		return nil, err
	}
	return descs[sc.tableID], nil
}

// notFirstInLine returns true whenever the schema change has been queued
// up for execution after another schema change.
func (sc *SchemaChanger) notFirstInLine(
	ctx context.Context,
) (*sqlbase.TableDescriptor, bool, error) {
	var notFirst bool
	var desc *sqlbase.TableDescriptor
	err := sc.db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		notFirst = false
		var err error
		desc, err = sqlbase.GetTableDescFromID(ctx, txn, sc.tableID)
		if err != nil {
			return err
		}
		for i, mutation := range desc.Mutations {
			if mutation.MutationID == sc.mutationID {
				notFirst = i != 0
				break
			}
		}
		return nil
	})
	return desc, notFirst, err
}

// runStateMachineAndBackfill runs the schema change state machine followed by
// the backfill.
func (sc *SchemaChanger) runStateMachineAndBackfill(
	ctx context.Context, lease *sqlbase.TableDescriptor_SchemaChangeLease,
) error {
	if fn := sc.testingKnobs.RunBeforePublishWriteAndDelete; fn != nil {
		fn()
	}
	// Run through mutation state machine before backfill.
	if err := sc.RunStateMachineBeforeBackfill(ctx); err != nil {
		return err
	}

	// Run backfill(s).
	if err := sc.runBackfill(ctx, lease); err != nil {
		return err
	}

	// Mark the mutations as completed.
	_, err := sc.done(ctx)
	return err
}

func (sc *SchemaChanger) refreshStats() {
	// Initiate an asynchronous run of CREATE STATISTICS. We use a large number
	// for rowsAffected because we want to make sure that stats always get
	// created/refreshed here.
	sc.execCfg.StatsRefresher.NotifyMutation(sc.tableID, math.MaxInt32 /* rowsAffected */)
}

// reverseMutations reverses the direction of all the mutations with the
// mutationID. This is called after hitting an irrecoverable error while
// applying a schema change. If a column being added is reversed and dropped,
// all new indexes referencing the column will also be dropped.
func (sc *SchemaChanger) reverseMutations(ctx context.Context, causingError error) error {
	// Reverse the flow of the state machine.
	var scJob *jobs.Job

	// Get the other tables whose foreign key backreferences need to be removed.
	var fksByBackrefTable map[sqlbase.ID][]*sqlbase.ConstraintToUpdate
	err := sc.db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		fksByBackrefTable = make(map[sqlbase.ID][]*sqlbase.ConstraintToUpdate)
		var err error
		desc, err := sqlbase.GetTableDescFromID(ctx, txn, sc.tableID)
		if err != nil {
			return err
		}
		for _, mutation := range desc.Mutations {
			if mutation.MutationID != sc.mutationID {
				break
			}
			if constraint := mutation.GetConstraint(); constraint != nil &&
				constraint.ConstraintType == sqlbase.ConstraintToUpdate_FOREIGN_KEY &&
				mutation.Direction == sqlbase.DescriptorMutation_ADD &&
				constraint.ForeignKey.Validity == sqlbase.ConstraintValidity_Validating {
				fk := &constraint.ForeignKey
				if fk.ReferencedTableID != desc.ID {
					fksByBackrefTable[constraint.ForeignKey.ReferencedTableID] = append(fksByBackrefTable[constraint.ForeignKey.ReferencedTableID], constraint)
				}
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	tableIDsToUpdate := make([]sqlbase.ID, 0, len(fksByBackrefTable)+1)
	tableIDsToUpdate = append(tableIDsToUpdate, sc.tableID)
	for id := range fksByBackrefTable {
		tableIDsToUpdate = append(tableIDsToUpdate, id)
	}

	// Create update closure for the table and all other tables with backreferences
	var droppedMutations map[sqlbase.MutationID]struct{}
	update := func(descs map[sqlbase.ID]*sqlbase.MutableTableDescriptor) error {
		scDesc, ok := descs[sc.tableID]
		if !ok {
			return errors.AssertionFailedf("required table with ID %d not provided to update closure", sc.tableID)
		}
		// Keep track of the column mutations being reversed so that indexes
		// referencing them can be dropped.
		columns := make(map[string]struct{})
		droppedMutations = nil

		for i, mutation := range scDesc.Mutations {
			if mutation.MutationID != sc.mutationID {
				// Only reverse the first set of mutations if they have the
				// mutation ID we're looking for.
				if i == 0 {
					return errDidntUpdateDescriptor
				}
				break
			}

			if mutation.Rollback {
				// Can actually never happen. This prevents a rollback of
				// an already rolled back mutation.
				return errors.AssertionFailedf("mutation already rolled back: %v", mutation)
			}

			log.Warningf(ctx, "reverse schema change mutation: %+v", mutation)
			scDesc.Mutations[i], columns = sc.reverseMutation(mutation, false /*notStarted*/, columns)

			// If the mutation is for validating a constraint that is being added,
			// drop the constraint because validation has failed
			if constraint := mutation.GetConstraint(); constraint != nil &&
				mutation.Direction == sqlbase.DescriptorMutation_ADD {
				log.Warningf(ctx, "dropping constraint %+v", constraint)
				if err := sc.maybeDropValidatingConstraint(ctx, scDesc, constraint); err != nil {
					return err
				}
				// Get the foreign key backreferences to remove.
				if constraint.ConstraintType == sqlbase.ConstraintToUpdate_FOREIGN_KEY {
					fk := &constraint.ForeignKey
					backrefTable, ok := descs[fk.ReferencedTableID]
					if !ok {
						return errors.AssertionFailedf("required table with ID %d not provided to update closure", sc.tableID)
					}
					if err := removeFKBackReferenceFromTable(backrefTable, fk.Name, scDesc.TableDesc()); err != nil {
						return err
					}
				}
			}
			scDesc.Mutations[i].Rollback = true
		}

		// Delete all mutations that reference any of the reversed columns
		// by running a graph traversal of the mutations.
		if len(columns) > 0 {
			var err error
			droppedMutations, err = sc.deleteIndexMutationsWithReversedColumns(ctx, scDesc, columns)
			if err != nil {
				return err
			}
		}

		// PublishMultiple() will increment the version.
		return nil
	}

	_, err = sc.leaseMgr.PublishMultiple(ctx, tableIDsToUpdate, update, func(txn *client.Txn) error {
		// Read the table descriptor from the store. The Version of the
		// descriptor has already been incremented in the transaction and
		// this descriptor can be modified without incrementing the version.
		tableDesc, err := sqlbase.GetTableDescFromID(ctx, txn, sc.tableID)
		if err != nil {
			return err
		}

		// Mark the schema change job as failed and create a rollback job.
		scJob, err = sc.createRollbackJob(ctx, txn, tableDesc, causingError)
		if err != nil {
			return err
		}

		// Mark other reversed mutation jobs as failed.
		for m := range droppedMutations {
			_, err := markJobFailed(ctx, txn, tableDesc, m, sc.jobRegistry, causingError)
			if err != nil {
				return err
			}
		}

		// Log "Reverse Schema Change" event. Only the causing error and the
		// mutation ID are logged; this can be correlated with the DDL statement
		// that initiated the change using the mutation id.
		return MakeEventLogger(sc.execCfg).InsertEventRecord(
			ctx,
			txn,
			EventLogReverseSchemaChange,
			int32(sc.tableID),
			int32(sc.nodeID),
			struct {
				Error      string
				MutationID uint32
			}{fmt.Sprintf("%+v", causingError), uint32(sc.mutationID)},
		)
	})
	if err != nil {
		return err
	}

	if err := sc.waitToUpdateLeases(ctx, sc.tableID); err != nil {
		return err
	}
	for id := range fksByBackrefTable {
		if err := sc.waitToUpdateLeases(ctx, id); err != nil {
			return err
		}
	}

	// Only update the job if the transaction has succeeded. The schame change
	// job will now references the rollback job.
	if scJob != nil {
		sc.job = scJob
		return scJob.Started(ctx)
	}
	return nil
}

// Mark the job associated with the mutation as failed.
func markJobFailed(
	ctx context.Context,
	txn *client.Txn,
	tableDesc *sqlbase.TableDescriptor,
	mutationID sqlbase.MutationID,
	jobRegistry *jobs.Registry,
	causingError error,
) (*jobs.Job, error) {
	// Mark job as failed.
	jobID, err := getJobIDForMutationWithDescriptor(ctx, tableDesc, mutationID)
	if err != nil {
		return nil, err
	}
	job, err := jobRegistry.LoadJobWithTxn(ctx, jobID, txn)
	if err != nil {
		return nil, err
	}
	err = job.WithTxn(txn).Failed(ctx, causingError, nil)
	return job, err
}

// Mark the current schema change job as failed and create a new rollback job
// representing the schema change and return it.
func (sc *SchemaChanger) createRollbackJob(
	ctx context.Context, txn *client.Txn, tableDesc *sqlbase.TableDescriptor, causingError error,
) (*jobs.Job, error) {

	// Mark job as failed.
	job, err := markJobFailed(ctx, txn, tableDesc, sc.mutationID, sc.jobRegistry, causingError)
	if err != nil {
		return nil, err
	}

	// Create a new rollback job representing the reversal of the mutations.
	for i := range tableDesc.MutationJobs {
		if tableDesc.MutationJobs[i].MutationID == sc.mutationID {
			// Create a roll back job.
			//
			// Initialize refresh spans to scan the entire table.
			span := tableDesc.PrimaryIndexSpan()
			var spanList []jobspb.ResumeSpanList
			for _, m := range tableDesc.Mutations {
				if m.MutationID == sc.mutationID {
					spanList = append(spanList,
						jobspb.ResumeSpanList{
							ResumeSpans: []roachpb.Span{span},
						},
					)
				}
			}
			payload := job.Payload()
			rollbackJob := sc.jobRegistry.NewJob(jobs.Record{
				Description:   fmt.Sprintf("ROLL BACK JOB %d: %s", *job.ID(), payload.Description),
				Username:      payload.Username,
				DescriptorIDs: payload.DescriptorIDs,
				Details:       jobspb.SchemaChangeDetails{ResumeSpanList: spanList},
				Progress:      jobspb.SchemaChangeProgress{},
			})
			if err := rollbackJob.WithTxn(txn).Created(ctx); err != nil {
				return nil, err
			}
			// Set the transaction back to nil so that this job can
			// be used in other transactions.
			rollbackJob.WithTxn(nil)

			tableDesc.MutationJobs[i].JobID = *rollbackJob.ID()

			// write descriptor, the version has already been incremented.
			b := txn.NewBatch()
			if err := writeDescToBatch(ctx, false /* kvTrace */, sc.settings, b, tableDesc.GetID(), tableDesc); err != nil {
				return nil, err
			}
			if err := txn.Run(ctx, b); err != nil {
				return nil, err
			}
			return rollbackJob, nil
		}
	}
	// Cannot get here.
	return nil, errors.AssertionFailedf("no job found for table %d mutation %d", errors.Safe(sc.tableID), errors.Safe(sc.mutationID))
}

func (sc *SchemaChanger) maybeDropValidatingConstraint(
	ctx context.Context, desc *MutableTableDescriptor, constraint *sqlbase.ConstraintToUpdate,
) error {
	switch constraint.ConstraintType {
	case sqlbase.ConstraintToUpdate_CHECK, sqlbase.ConstraintToUpdate_NOT_NULL:
		if constraint.Check.Validity == sqlbase.ConstraintValidity_Unvalidated {
			return nil
		}
		for j, c := range desc.Checks {
			if c.Name == constraint.Check.Name {
				desc.Checks = append(desc.Checks[:j], desc.Checks[j+1:]...)
				return nil
			}
		}
		if log.V(2) {
			log.Infof(
				ctx,
				"attempted to drop constraint %s, but it hadn't been added to the table descriptor yet",
				constraint.Check.Name,
			)
		}
	case sqlbase.ConstraintToUpdate_FOREIGN_KEY:
		for i, fk := range desc.OutboundFKs {
			if fk.Name == constraint.ForeignKey.Name {
				desc.OutboundFKs = append(desc.OutboundFKs[:i], desc.OutboundFKs[i+1:]...)
				return nil
			}
		}
		if log.V(2) {
			log.Infof(
				ctx,
				"attempted to drop constraint %s, but it hadn't been added to the table descriptor yet",
				constraint.ForeignKey.Name,
			)
		}
	default:
		return errors.AssertionFailedf("unsupported constraint type: %d", errors.Safe(constraint.ConstraintType))
	}
	return nil
}

// deleteIndexMutationsWithReversedColumns deletes mutations with a
// different mutationID than the schema changer and with an index that
// references one of the reversed columns. Execute this as a breadth
// first search graph traversal.
func (sc *SchemaChanger) deleteIndexMutationsWithReversedColumns(
	ctx context.Context, desc *sqlbase.MutableTableDescriptor, columns map[string]struct{},
) (map[sqlbase.MutationID]struct{}, error) {
	dropMutations := make(map[sqlbase.MutationID]struct{})
	// Run breadth first search traversal that reverses mutations
	for {
		start := len(dropMutations)
		for _, mutation := range desc.Mutations {
			if mutation.MutationID != sc.mutationID {
				if idx := mutation.GetIndex(); idx != nil {
					for _, name := range idx.ColumnNames {
						if _, ok := columns[name]; ok {
							// Such an index mutation has to be with direction ADD and
							// in the DELETE_ONLY state. Live indexes referencing live
							// columns cannot be deleted and thus never have direction
							// DROP. All mutations with the ADD direction start off in
							// the DELETE_ONLY state.
							if mutation.Direction != sqlbase.DescriptorMutation_ADD ||
								mutation.State != sqlbase.DescriptorMutation_DELETE_ONLY {
								panic(fmt.Sprintf("mutation in bad state: %+v", mutation))
							}
							log.Warningf(ctx, "drop schema change mutation: %+v", mutation)
							dropMutations[mutation.MutationID] = struct{}{}
							break
						}
					}
				}
			}
		}

		if len(dropMutations) == start {
			// No more mutations to drop.
			break
		}
		// Drop mutations.
		newMutations := make([]sqlbase.DescriptorMutation, 0, len(desc.Mutations))
		for _, mutation := range desc.Mutations {
			if _, ok := dropMutations[mutation.MutationID]; ok {
				// Reverse mutation. Update columns to reflect additional
				// columns that have been purged. This mutation doesn't need
				// a rollback because it was not started.
				mutation, columns = sc.reverseMutation(mutation, true /*notStarted*/, columns)
				// Mark as complete because this mutation needs no backfill.
				if err := desc.MakeMutationComplete(mutation); err != nil {
					return nil, err
				}
			} else {
				newMutations = append(newMutations, mutation)
			}
		}
		// Reset mutations.
		desc.Mutations = newMutations
	}
	return dropMutations, nil
}

// Reverse a mutation. Returns the updated mutation and updated columns.
// notStarted is set to true only if the schema change state machine
// was not started for the mutation.
func (sc *SchemaChanger) reverseMutation(
	mutation sqlbase.DescriptorMutation, notStarted bool, columns map[string]struct{},
) (sqlbase.DescriptorMutation, map[string]struct{}) {
	switch mutation.Direction {
	case sqlbase.DescriptorMutation_ADD:
		mutation.Direction = sqlbase.DescriptorMutation_DROP
		// A column ADD being reversed gets placed in the map.
		if col := mutation.GetColumn(); col != nil {
			columns[col.Name] = struct{}{}
		}
		// PrimaryKeySwap doesn't have a concept of the state machine.
		if pkSwap := mutation.GetPrimaryKeySwap(); pkSwap != nil {
			return mutation, columns
		}
		if notStarted && mutation.State != sqlbase.DescriptorMutation_DELETE_ONLY {
			panic(fmt.Sprintf("mutation in bad state: %+v", mutation))
		}

	case sqlbase.DescriptorMutation_DROP:
		mutation.Direction = sqlbase.DescriptorMutation_ADD
		if notStarted && mutation.State != sqlbase.DescriptorMutation_DELETE_AND_WRITE_ONLY {
			panic(fmt.Sprintf("mutation in bad state: %+v", mutation))
		}
	}
	return mutation, columns
}

// TestingSchemaChangerCollection is an exported (for testing) version of
// schemaChangerCollection.
// TODO(andrei): get rid of this type once we can have tests internal to the sql
// package (as of April 2016 we can't because sql can't import server).
type TestingSchemaChangerCollection struct {
	scc *schemaChangerCollection
}

// ClearSchemaChangers clears the schema changers from the collection.
// If this is called from a SyncSchemaChangersFilter, no schema changer will be
// run.
func (tscc TestingSchemaChangerCollection) ClearSchemaChangers() {
	tscc.scc.schemaChangers = tscc.scc.schemaChangers[:0]
}

// SyncSchemaChangersFilter is the type of a hook to be installed through the
// ExecutorContext for blocking or otherwise manipulating schema changers run
// through the sync schema changers path.
type SyncSchemaChangersFilter func(TestingSchemaChangerCollection)

// SchemaChangerTestingKnobs for testing the schema change execution path
// through both the synchronous and asynchronous paths.
type SchemaChangerTestingKnobs struct {
	// SyncFilter is called before running schema changers synchronously (at
	// the end of a txn). The function can be used to clear the schema
	// changers (if the test doesn't want them run using the synchronous path)
	// or to temporarily block execution. Note that this has nothing to do
	// with the async path for running schema changers. To block that, set
	// AsyncExecNotification.
	SyncFilter SyncSchemaChangersFilter

	// RunBeforePublishWriteAndDelete is called just before publishing the
	// write+delete state for the schema change.
	RunBeforePublishWriteAndDelete func()

	// RunBeforeBackfill is called just before starting the backfill.
	RunBeforeBackfill func() error

	// RunBeforeIndexBackfill is called just before starting the index backfill, after
	// fixing the index backfill scan timestamp.
	RunBeforeIndexBackfill func()

	// RunBeforePrimaryKeySwap is called just before the primary key swap is committed.
	RunBeforePrimaryKeySwap func()

	// RunBeforeIndexValidation is called just before starting the index validation,
	// after setting the job status to validating.
	RunBeforeIndexValidation func() error

	// RunBeforeConstraintValidation is called just before starting the checks validation,
	// after setting the job status to validating.
	RunBeforeConstraintValidation func() error

	// OldNamesDrainedNotification is called during a schema change,
	// after all leases on the version of the descriptor with the old
	// names are gone, and just before the mapping of the old names to the
	// descriptor id are about to be deleted.
	OldNamesDrainedNotification func()

	// AsyncExecNotification is a function called before running a schema
	// change asynchronously. Returning an error will prevent the asynchronous
	// execution path from running.
	AsyncExecNotification func() error

	// AsyncExecQuickly executes queued schema changes as soon as possible.
	AsyncExecQuickly bool

	// WriteCheckpointInterval is the interval after which a checkpoint is
	// written.
	WriteCheckpointInterval time.Duration

	// BackfillChunkSize is to be used for all backfill chunked operations.
	BackfillChunkSize int64

	// TwoVersionLeaseViolation is called whenever a schema change
	// transaction is unable to commit because it is violating the two
	// version lease invariant.
	TwoVersionLeaseViolation func()

	// OnError is called with all the errors seen by the
	// synchronous code path.
	OnError func(err error)
}

// ModuleTestingKnobs is part of the base.ModuleTestingKnobs interface.
func (*SchemaChangerTestingKnobs) ModuleTestingKnobs() {}

// SchemaChangeManager processes pending schema changes seen in gossip
// updates. Most schema changes are executed synchronously by the node
// that created the schema change. If the node dies while
// processing the schema change this manager acts as a backup
// execution mechanism.
type SchemaChangeManager struct {
	ambientCtx   log.AmbientContext
	execCfg      *ExecutorConfig
	testingKnobs *SchemaChangerTestingKnobs
	// Create a schema changer for every outstanding schema change seen.
	schemaChangers map[sqlbase.ID]SchemaChanger
	// Create a schema changer for every table that is dropped or has
	// dropped indexes that needs to be GC-ed.
	forGC          map[sqlbase.ID]SchemaChanger
	distSQLPlanner *DistSQLPlanner
	ieFactory      sqlutil.SessionBoundInternalExecutorFactory
}

// NewSchemaChangeManager returns a new SchemaChangeManager.
func NewSchemaChangeManager(
	ambientCtx log.AmbientContext,
	execCfg *ExecutorConfig,
	testingKnobs *SchemaChangerTestingKnobs,
	db client.DB,
	nodeDesc roachpb.NodeDescriptor,
	dsp *DistSQLPlanner,
	ieFactory sqlutil.SessionBoundInternalExecutorFactory,
) *SchemaChangeManager {
	return &SchemaChangeManager{
		ambientCtx:     ambientCtx,
		execCfg:        execCfg,
		testingKnobs:   testingKnobs,
		schemaChangers: make(map[sqlbase.ID]SchemaChanger),
		forGC:          make(map[sqlbase.ID]SchemaChanger),
		distSQLPlanner: dsp,
		ieFactory:      ieFactory,
	}
}

// Creates a timer that is used by the manager to decide on
// when to run the next schema changer.
func (s *SchemaChangeManager) newTimer(changers map[sqlbase.ID]SchemaChanger) *time.Timer {
	if len(changers) == 0 {
		return &time.Timer{}
	}
	waitDuration := time.Duration(math.MaxInt64)
	now := timeutil.Now()
	for _, sc := range changers {
		d := sc.execAfter.Sub(now)
		if d < waitDuration {
			waitDuration = d
		}
	}
	return time.NewTimer(waitDuration)
}

// Start starts a goroutine that runs outstanding schema changes
// for tables received in the latest system configuration via gossip.
func (s *SchemaChangeManager) Start(stopper *stop.Stopper) {
	stopper.RunWorker(s.ambientCtx.AnnotateCtx(context.Background()), func(ctx context.Context) {
		descKeyPrefix := keys.MakeTablePrefix(uint32(sqlbase.DescriptorTable.ID))
		cfgFilter := gossip.MakeSystemConfigDeltaFilter(descKeyPrefix)
		k := keys.MakeTablePrefix(uint32(keys.ZonesTableID))
		k = encoding.EncodeUvarintAscending(k, uint64(keys.ZonesTablePrimaryIndexID))
		zoneCfgFilter := gossip.MakeSystemConfigDeltaFilter(k)
		gossipUpdateC := s.execCfg.Gossip.RegisterSystemConfigChannel()
		timer := &time.Timer{}
		gcTimer := &time.Timer{}
		// A jitter is added to reduce contention between nodes
		// attempting to run the schema change.
		delay := time.Duration(float64(asyncSchemaChangeDelay) * (0.9 + 0.2*rand.Float64()))
		if s.testingKnobs.AsyncExecQuickly {
			delay = 20 * time.Millisecond
		}
		defTTL := s.execCfg.DefaultZoneConfig.GC.TTLSeconds

		execOneSchemaChange := func(schemaChangers map[sqlbase.ID]SchemaChanger) {
			for tableID, sc := range schemaChangers {
				if timeutil.Since(sc.execAfter) > 0 {
					execCtx, cleanup := tracing.EnsureContext(ctx, s.ambientCtx.Tracer, "schema change [async]")
					err := sc.exec(execCtx, false /* inSession */)
					cleanup()

					// Advance the execAfter time so that this schema
					// changer doesn't get called again for a while.
					sc.execAfter = timeutil.Now().Add(delay)
					schemaChangers[tableID] = sc

					if err != nil {
						if shouldLogSchemaChangeError(err) {
							log.Warningf(ctx, "Error executing schema change: %s", err)
						}
						if errors.Is(err, sqlbase.ErrDescriptorNotFound) {
							// Someone deleted this table. Don't try to run the schema
							// changer again. Note that there's no gossip update for the
							// deletion which would remove this schemaChanger.
							delete(schemaChangers, tableID)
						}
					} else {
						// We successfully executed the schema change. Delete it.
						delete(schemaChangers, tableID)
					}

					// Only attempt to run one schema changer.
					break
				}
			}
		}

		for {
			select {
			case <-gossipUpdateC:
				cfg := s.execCfg.Gossip.GetSystemConfig()
				// Read all tables and their versions
				if log.V(2) {
					log.Info(ctx, "received a new config")
				}

				resetTimer := false
				// Check to see if the zone cfg has been modified.
				zoneCfgModified := false
				zoneCfgFilter.ForModified(cfg, func(kv roachpb.KeyValue) {
					zoneCfgModified = true
				})
				if zoneCfgModified {
					// Check to see if the GC TTL has changed for all the
					// tables that are currently waiting to be GC-ed. If the
					// GC TTL for a table has indeed changed it is modified
					// and enqueued with the new TTL timeout.
					for id, sc := range s.forGC {
						zoneCfg, placeholder, _, err := ZoneConfigHook(cfg, uint32(id))
						if err != nil {
							log.Errorf(ctx, "zone config for desc: %d, err = %+v", id, err)
							return
						}
						if zoneCfg == nil {
							// Do nothing, use the old zone config's TTL.
							continue
						}
						if placeholder == nil {
							placeholder = zoneCfg
						}

						newExecTime := sc.execAfter
						if sc.dropTime > 0 {
							deadline := sc.dropTime +
								int64(zoneCfg.GC.TTLSeconds)*time.Second.Nanoseconds() +
								int64(delay)
							newExecTime = timeutil.Unix(0, deadline)
						}
						if len(sc.dropIndexTimes) > 0 {
							var earliestIndexExec time.Time

							for i := 0; i < len(sc.dropIndexTimes); i++ {
								droppedIdx := &sc.dropIndexTimes[i]

								ttlSeconds := zoneCfg.GC.TTLSeconds
								if subzone := placeholder.GetSubzone(
									uint32(droppedIdx.indexID), ""); subzone != nil && subzone.Config.GC != nil {
									ttlSeconds = subzone.Config.GC.TTLSeconds
								}

								deadline := droppedIdx.dropTime +
									int64(ttlSeconds)*time.Second.Nanoseconds() +
									int64(delay)
								execTime := timeutil.Unix(0, deadline)
								droppedIdx.deadline = deadline

								if earliestIndexExec.IsZero() || execTime.Before(earliestIndexExec) {
									earliestIndexExec = execTime
								}
							}
							if newExecTime.After(earliestIndexExec) {
								newExecTime = earliestIndexExec
							}
						}
						if newExecTime != sc.execAfter {
							resetTimer = true
							sc.execAfter = newExecTime
							// Safe to modify map inplace while iterating over it.
							s.forGC[id] = sc
							if log.V(2) {
								log.Infof(ctx,
									"re-queue up pending drop table GC; table: %d", id)
							}
						}
					}
				}

				schemaChanger := SchemaChanger{
					execCfg:              s.execCfg,
					nodeID:               s.execCfg.NodeID.Get(),
					db:                   s.execCfg.DB,
					leaseMgr:             s.execCfg.LeaseManager,
					testingKnobs:         s.testingKnobs,
					distSQLPlanner:       s.distSQLPlanner,
					jobRegistry:          s.execCfg.JobRegistry,
					leaseHolderCache:     s.execCfg.LeaseHolderCache,
					rangeDescriptorCache: s.execCfg.RangeDescriptorCache,
					clock:                s.execCfg.Clock,
					settings:             s.execCfg.Settings,
					ieFactory:            s.ieFactory,
				}

				execAfter := timeutil.Now().Add(delay)
				cfgFilter.ForModified(cfg, func(kv roachpb.KeyValue) {
					resetTimer = true
					// Attempt to unmarshal config into a table/database descriptor.
					var descriptor sqlbase.Descriptor
					if err := kv.Value.GetProto(&descriptor); err != nil {
						log.Warningf(ctx, "%s: unable to unmarshal descriptor %v", kv.Key, kv.Value)
						return
					}
					switch union := descriptor.Union.(type) {
					case *sqlbase.Descriptor_Table:
						table := union.Table
						if err := table.MaybeFillInDescriptor(ctx, nil); err != nil {
							log.Errorf(ctx, "%s: failed to fill in table descriptor %v", kv.Key, table)
							return
						}
						if err := table.ValidateTable(); err != nil {
							log.Errorf(ctx, "%s: received invalid table descriptor: %s. Desc: %v",
								kv.Key, err, table,
							)
							return
						}

						schemaChanger.tableID = table.ID
						schemaChanger.mutationID = sqlbase.InvalidMutationID
						schemaChanger.execAfter = execAfter
						schemaChanger.dropTime = 0

						var minDeadline int64
						if len(table.GCMutations) > 0 {
							zoneCfg, placeholder, _, err := ZoneConfigHook(cfg, uint32(table.ID))
							if err != nil {
								log.Errorf(ctx, "zone config for desc: %d, err = %+v", table.ID, err)
								return
							}

							if placeholder == nil {
								placeholder = zoneCfg
							}

							for _, m := range table.GCMutations {
								// Initialize TTL without a zone config in case it's not present.
								ttlSeconds := defTTL
								if zoneCfg != nil {
									ttlSeconds = zoneCfg.GC.TTLSeconds
									if subzone := placeholder.GetSubzone(
										uint32(m.IndexID), ""); subzone != nil && subzone.Config.GC != nil {
										ttlSeconds = subzone.Config.GC.TTLSeconds
									}
								}
								deadline := m.DropTime + int64(delay) + int64(ttlSeconds)*time.Second.Nanoseconds()

								dropped := droppedIndex{m.IndexID, m.DropTime, deadline}
								if minDeadline == 0 || deadline < minDeadline {
									minDeadline = deadline
								}
								schemaChanger.dropIndexTimes = append(schemaChanger.dropIndexTimes, dropped)
							}
						}

						// Keep track of outstanding schema changes.
						pendingChanges := table.Adding() ||
							table.HasDrainingNames() || len(table.Mutations) > 0
						if pendingChanges {
							if log.V(2) {
								log.Infof(ctx, "%s: queue up pending schema change; table: %d, version: %d",
									kv.Key, table.ID, table.Version)
							}

							if len(table.Mutations) > 0 {
								schemaChanger.mutationID = table.Mutations[0].MutationID
							}
							s.schemaChangers[table.ID] = schemaChanger
						} else if table.Dropped() {
							// If the table is dropped add table to map forGC.
							if log.V(2) {
								log.Infof(ctx,
									"%s: queue up pending drop table GC; table: %d, version: %d",
									kv.Key, table.ID, table.Version)
							}

							if table.DropTime > 0 {
								schemaChanger.dropTime = table.DropTime
								zoneCfg, _, _, err := ZoneConfigHook(cfg, uint32(table.ID))
								if err != nil {
									log.Errorf(ctx, "zone config for desc: %d, err: %+v", table.ID, err)
									return
								}

								// Initialize deadline without a zone config in case it's not present.
								deadline := table.DropTime + int64(delay)
								if zoneCfg != nil {
									deadline += int64(zoneCfg.GC.TTLSeconds) * time.Second.Nanoseconds()
								} else {
									deadline += int64(defTTL) * time.Second.Nanoseconds()
								}
								if minDeadline == 0 || deadline < minDeadline {
									minDeadline = deadline
								}
							}
						}

						if minDeadline != 0 {
							schemaChanger.execAfter = timeutil.Unix(0, minDeadline)
						}

						if table.Dropped() || (!pendingChanges && len(table.GCMutations) > 0) {
							s.forGC[table.ID] = schemaChanger
							// Remove from schema change map if present because
							// this table has been dropped or only has other GC waiting mutations.
							delete(s.schemaChangers, table.ID)
						}

					case *sqlbase.Descriptor_Database:
						// Ignore.
					}
				})

				if resetTimer {
					timer = s.newTimer(s.schemaChangers)
					gcTimer = s.newTimer(s.forGC)
				}

			case <-timer.C:
				if s.testingKnobs.AsyncExecNotification != nil &&
					s.testingKnobs.AsyncExecNotification() != nil {
					timer = s.newTimer(s.schemaChangers)
					continue
				}

				execOneSchemaChange(s.schemaChangers)

				timer = s.newTimer(s.schemaChangers)

			case <-gcTimer.C:
				if s.testingKnobs.AsyncExecNotification != nil &&
					s.testingKnobs.AsyncExecNotification() != nil {
					gcTimer = s.newTimer(s.forGC)
					continue
				}

				execOneSchemaChange(s.forGC)

				gcTimer = s.newTimer(s.forGC)

			case <-stopper.ShouldStop():
				return
			}
		}
	})
}

// createSchemaChangeEvalCtx creates an extendedEvalContext() to be used for backfills.
//
// TODO(andrei): This EvalContext() will be broken for backfills trying to use
// functions marked with distsqlBlacklist.
// Also, the SessionTracing inside the context is unrelated to the one
// used in the surrounding SQL session, so session tracing is unable
// to capture schema change activity.
func createSchemaChangeEvalCtx(
	ctx context.Context, ts hlc.Timestamp, ieFactory sqlutil.SessionBoundInternalExecutorFactory,
) extendedEvalContext {
	dummyLocation := time.UTC

	sd := &sessiondata.SessionData{
		SearchPath: sqlbase.DefaultSearchPath,
		// The database is not supposed to be needed in schema changes, as there
		// shouldn't be unqualified identifiers in backfills, and the pure functions
		// that need it should have already been evaluated.
		//
		// TODO(andrei): find a way to assert that this field is indeed not used.
		// And in fact it is used by `current_schemas()`, which, although is a pure
		// function, takes arguments which might be impure (so it can't always be
		// pre-evaluated).
		Database:      "",
		SequenceState: sessiondata.NewSequenceState(),
		DataConversion: sessiondata.DataConversionConfig{
			Location: dummyLocation,
		},
		User: security.NodeUser,
	}

	evalCtx := extendedEvalContext{
		// Make a session tracing object on-the-fly. This is OK
		// because it sets "enabled: false" and thus none of the
		// other fields are used.
		Tracing: &SessionTracing{},
		EvalContext: tree.EvalContext{
			SessionData:      sd,
			InternalExecutor: ieFactory(ctx, sd),
			// TODO(andrei): This is wrong (just like on the main code path on
			// setupFlow). Each processor should override Ctx with its own context.
			Context:         ctx,
			Sequence:        &sqlbase.DummySequenceOperators{},
			Planner:         &sqlbase.DummyEvalPlanner{},
			SessionAccessor: &sqlbase.DummySessionAccessor{},
		},
	}
	// The backfill is going to use the current timestamp for the various
	// functions, like now(), that need it.  It's possible that the backfill has
	// been partially performed already by another SchemaChangeManager with
	// another timestamp.
	//
	// TODO(andrei): Figure out if this is what we want, and whether the
	// timestamp from the session that enqueued the schema change
	// is/should be used for impure functions like now().
	evalCtx.SetTxnTimestamp(timeutil.Unix(0 /* sec */, ts.WallTime))
	evalCtx.SetStmtTimestamp(timeutil.Unix(0 /* sec */, ts.WallTime))

	return evalCtx
}
