// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// XXX: Skipping jobs for now per Andrew's advice.
// XXX: Look at client_*_test.go files for client.DB usage/test ideas.
// XXX: How do cluster migrations start machinery today, on change?
// ANS: They just poll IsActive, and behave accordingly. No "migrations" happen.
// We want something like VersionUpgradeHook to actually set up the thing, run
// hook after every node will observe IsActive == true.
// XXX: Need to define "node upgrade" RPC subsystem.
package migration

import (
	"bytes"
	"context"
	"fmt"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sqlmigrations/leasemanager"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

type Migration func(context.Context, *Helper) error

type Helper struct {
	db         *kv.DB
	executor   *sql.InternalExecutor
	nodeDialer *nodedialer.Dialer
}

type Orchestrator struct {
	db           *kv.DB
	executor     *sql.InternalExecutor
	nodeDialer   *nodedialer.Dialer
	leaseManager *leasemanager.LeaseManager

	// XXX: Lots more stuff. Including facility to run SQL migrations
	// (eventually).
}

func NewOrchestrator(leaseManager *leasemanager.LeaseManager, executor *sql.InternalExecutor, db *kv.DB, nodeDialer *nodedialer.Dialer) *Orchestrator {
	return &Orchestrator{
		db:           db,
		leaseManager: leaseManager,
		executor:     executor,
		nodeDialer:   nodeDialer,
	}
}

// XXX: Use blockSize here as well. *adminServer.DecommissionStatus does
// something similar.
func (h *Helper) IterRangeDescriptors(
	f func(...roachpb.RangeDescriptor) error,
) error {
	// Paginate through all ranges and invoke f with chunks of size ~blockSize.
	// call h.Progress between invocations.
	const pageSize = 5
	descriptors := make([]roachpb.RangeDescriptor, pageSize)
	err := h.db.Txn(context.TODO(), func(ctx context.Context, txn *kv.Txn) error {
		return txn.Iterate(ctx, keys.MetaMin, keys.MetaMax, pageSize,
			func(rows []kv.KeyValue) error {
				for i, row := range rows {
					if err := row.ValueProto(&descriptors[i]); err != nil {
						return errors.Wrapf(err, "%s: unable to unmarshal range descriptor", row.Key)
					}
				}

				if err := f(descriptors...); err != nil {
					return err
				}

				time.Sleep(1 * time.Second)
				return nil
			})
	})
	if err != nil {
		return err
	}
	return nil
}

func (h *Helper) Retry(f func() error) error {
	for {
		err := f()
		if err != nil {
			continue
		}
		return err
	}
}

func (h *Helper) RequiredNodes(ctx context.Context) ([]roachpb.NodeID, error) {
	kvs, err := h.db.Scan(ctx, keys.NodeLivenessPrefix, keys.NodeLivenessKeyMax, 0)
	if err != nil {
		return nil, errors.Wrap(err, "unable to get liveness")
	}

	var results []roachpb.NodeID
	for _, kv := range kvs {
		if kv.Value == nil {
			return nil, errors.AssertionFailedf("missing liveness record")
		}
		var liveness kvserverpb.Liveness
		if err := kv.Value.GetProto(&liveness); err != nil {
			return nil, errors.Wrap(err, "invalid liveness record")
		}

		results = append(results, liveness.NodeID)
	}

	return results, nil
}

func (h *Helper) Progress(s string, num, denum int) {
	// Set progress of the current step to num/denum. Denum can be zero if final
	// steps not known (example: iterating through ranges)
	// Free-form message can be attached (PII?)
	//
	// XXX: this API is crap. Also, where do we get the denominator from?
}

func (h *Helper) EveryNode(ctx context.Context, op string, args ...interface{}) error {
	nodeIDs, err := h.RequiredNodes(ctx)
	if err != nil {
		return err
	}
	log.Infof(ctx, "xxx: performing %s on every node (nodeids=%s, args=%s)", op, nodeIDs, args)

	for {
		for _, nodeID := range nodeIDs {
			// XXX: This can be parallelized. Is errgroup appropriate for prod use?
			conn, err := h.nodeDialer.Dial(ctx, nodeID, rpc.DefaultClass)
			if err != nil {
				return err
			}

			var req *serverpb.EveryNodeRequest
			if op == "ack-pending-version" {
				// XXX: Need equivalent to batch_generated.go. We should change the
				// signature of this method to accept EveryNodeRequest instead.
				pendingV := args[0].(roachpb.Version)
				req = &serverpb.EveryNodeRequest{
					Request: serverpb.EveryNodeRequestUnion{
						Value: &serverpb.EveryNodeRequestUnion_AckPendingVersion{
							AckPendingVersion: &serverpb.AckPendingVersionRequest{
								Version: &pendingV,
							},
						},
					},
				}
			} else {
				return errors.Newf("unsupported op %s", op)
			}

			client := serverpb.NewAdminClient(conn)
			if _, err := client.EveryNode(ctx, req); err != nil {
				return err
			}
		}

		newNodeIDs, err := h.RequiredNodes(ctx)
		if err != nil {
			return err
		}

		if fmt.Sprintf("%v", newNodeIDs) == fmt.Sprintf("%v", nodeIDs) {
			break
		}
	}
	return nil
}

type V struct {
	roachpb.Version
	Migration // XXX: The hook, to be run post-gossip of cluster setting.
}

func (o *Orchestrator) RunMigrations(ctx context.Context, to roachpb.Version) error {
	// XXX: Refresh this lease during execution of LRM.
	_, err := o.leaseManager.AcquireLease(ctx, keys.LRMLease)
	if err != nil {
		return err
	}

	ml := MakeMigrationLogger(o.executor)

	// XXX: This should called by something higher up that can own the job, and
	// can set it up, etc. Probably sits on the orchestrator itself, and have a
	// way to grab leases (through sql/../leasemanager). Use a system table for in
	// progress state tracking.
	// XXX: Do we have to get this key from KV? aren't we using the system table
	// for it all? Can't we simply retrieve it from the store local key? Every
	// migration pushes the gate out globally, so.
	var ackedV roachpb.Version
	if err := o.db.GetProto(ctx, keys.ClusterVersionKey, &ackedV); err != nil {
		return err
	}

	if (ackedV == roachpb.Version{}) {
		ackedV = clusterversion.TestingBinaryVersion
	}

	log.Infof(ctx, "xxx: existing version=%s", ackedV)
	if ackedV == to {
		log.Info(ctx, "xxx: nothing to do here")
		return nil
	}

	// XXX: Hard-coded example data. These need to be assembled from a registry
	// while taking into account ackedV.

	// XXX: What should we do if no version is found in KV? We should write as
	// part of initial cluster data anyway. For migrating clusters, we'll simply
	// run all the migrations?

	vs := []V{
		{
			Version: to,
			Migration: func(ctx context.Context, h *Helper) error {
				txn := h.db.NewTxn(ctx, "xxx: txn")
				_, err := h.executor.Query(
					ctx, "xxx: select", txn,
					`SELECT 1`,
				)
				if err != nil {
					return err
				}

				time.Sleep(time.Second)

				log.Infof(ctx, "xxx: retrieved value=%s", ackedV)
				log.Infof(ctx, "xxx: ran named migration for version=%s", to)

				batch := 0
				// XXX: Shouldn't our intended range look like {keys.MetaMin, keys.TenantTableDataMax}
				_ = h.IterRangeDescriptors(func(descriptors ...roachpb.RangeDescriptor) error {
					batch += 1
					for _, desc := range descriptors {
						if bytes.Compare(desc.StartKey, keys.LocalMax) < 0 {
							// XXX: errors out out /Min, System/NodeLiveness
							// otherwise. Is this what we want?
							continue
						}
						if err := h.db.AdminMigrate(ctx, desc.StartKey, desc.EndKey); err != nil {
							return err
						}

					}
					if err := ml.InsertRecord(ctx, txn, fmt.Sprintf("finished batch %d", batch)); err != nil {
						return err
					}
					return nil
				})

				if err := ml.InsertRecord(ctx, txn, fmt.Sprintf("completed %s ==> %s", ackedV, to)); err != nil {
					return err
				}
				if err := txn.Commit(ctx); err != nil {
					return err
				}

				return nil
			},
		},
	}

	for _, v := range vs {
		h := &Helper{db: o.db, executor: o.executor, nodeDialer: o.nodeDialer}
		// h.Progress should basically call j.Status(v.String()+": "+s, num, denum)

		// Persist the beginning of this migration on all nodes. Basically they
		// will persist the version, then move the cluster setting forward, then
		// return.
		//
		// Should they store whether migration is ongoing? No.
		// - Argument for yes: We might have functionality that is only safe
		// when all nodes in the cluster have "done their part" (XXX: What does
		// this mean) to migrate into it, and which we don't want to delay for
		// one release.
		// - Argument for no: We could just gate that functionality's activation
		// on a later unstable version, and get the right behavior.
		if err := h.EveryNode(ctx, "ack-pending-version", v.Version); err != nil {
			return err
		}
		if err := v.Migration(ctx, h); err != nil {
			return err
		}
	}

	// XXX: Should this be a CPut instead?
	if err := o.db.Put(ctx, keys.ClusterVersionKey, &to); err != nil {
		return err
	}

	return nil
}

type MigrationLogger struct {
	*sql.InternalExecutor
}

func MakeMigrationLogger(ie *sql.InternalExecutor) MigrationLogger {
	return MigrationLogger{InternalExecutor: ie}
}

// InsertEventRecord inserts a single event into the event log as part of the
// provided transaction.
func (m MigrationLogger) InsertRecord(
	ctx context.Context,
	txn *kv.Txn,
	arg interface{},
) error {
	// Record event record insertion in local log output.
	txn.AddCommitTrigger(func(ctx context.Context) {
		log.Infof(ctx, "xxx: inserted record %v", arg)
	})

	const insertMigrationTableStmt = `
INSERT INTO system.lrm ("metadata") VALUES ($1)
`
	args := []interface{}{arg}
	rows, err := m.Exec(ctx, "log-event", txn, insertMigrationTableStmt, args...)
	if err != nil {
		return err
	}
	if rows != 1 {
		return errors.Errorf("%d rows affected by log insertion; expected exactly one row affected.", rows)
	}
	return nil
}
