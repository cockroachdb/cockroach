// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/errors"
)

func queryClusterLocks(
	ctx context.Context, ie *sql.InternalExecutor, shouldRedactKeys bool,
) ([]*serverpb.ClusterLocksStateResponse_LockInfo, error) {
	query := `
SELECT
  lock_key_pretty,
  database_name,
  schema_name,
  table_name,
  index_name,
  txn_id,
  lock_strength,
  duration,
  granted
FROM
  crdb_internal.cluster_locks
`

	it, err := ie.QueryIteratorEx(ctx, "cluster-locks", nil,
		sessiondata.InternalExecutorOverride{
			User: username.NodeUserName(),
		}, query)

	if err != nil {
		return nil, serverError(ctx, err)
	}

	lockKeyToLock := make(map[string]*serverpb.ClusterLocksStateResponse_LockInfo)

	const expectedNumDatums = 9
	var ok bool
	for ok, err = it.Next(ctx); ok; ok, err = it.Next(ctx) {
		var row tree.Datums
		if row = it.Cur(); row == nil {
			return nil, errors.New("unexpected null row")
		}

		if row.Len() != expectedNumDatums {
			return nil, errors.Newf("expected %d columns, received %d", expectedNumDatums, row.Len())
		}

		lockKeyPretty := string(tree.MustBeDString(row[0]))

		var lockInfo *serverpb.ClusterLocksStateResponse_LockInfo
		var exists bool
		if lockInfo, exists = lockKeyToLock[lockKeyPretty]; !exists {
			lockInfo = &serverpb.ClusterLocksStateResponse_LockInfo{
				DatabaseName: string(tree.MustBeDString(row[1])),
				SchemaName:   string(tree.MustBeDString(row[2])),
				TableName:    string(tree.MustBeDString(row[3])),
				IndexName:    string(tree.MustBeDString(row[4])),
			}

			if !shouldRedactKeys {
				lockInfo.KeyPrettyOrRedacted = lockKeyPretty
			}

			lockKeyToLock[lockKeyPretty] = lockInfo
		}

		txnIDDatum, ok := tree.AsDUuid(row[5])
		if !ok {
			continue
		}

		txnID := txnIDDatum.UUID
		lockStrength := string(tree.MustBeDString(row[6]))
		duration := time.Duration(tree.MustBeDInterval(row[7]).Duration.Nanos())

		transaction := &serverpb.ClusterLocksStateResponse_LockTxnInfo{
			TxnID:        txnID,
			Duration:     duration,
			LockStrength: lockStrength,
		}

		granted := bool(tree.MustBeDBool(row[8]))
		if granted {
			lockInfo.TxnLockHolder = transaction
		} else {
			lockInfo.WaitingTxns = append(lockInfo.WaitingTxns, transaction)
		}
	}

	if err != nil {
		return nil, serverError(ctx, err)
	}

	// Convert map to list.
	locks := make([]*serverpb.ClusterLocksStateResponse_LockInfo, 0, len(lockKeyToLock))

	for _, lock := range lockKeyToLock {
		locks = append(locks, lock)
	}

	return locks, nil
}

func (s *statusServer) ClusterLocksState(
	ctx context.Context, _ *serverpb.ClusterLocksStateRequest,
) (*serverpb.ClusterLocksStateResponse, error) {
	ctx = propagateGatewayMetadata(ctx)
	ctx = s.AnnotateCtx(ctx)

	if err := s.privilegeChecker.requireViewActivityOrViewActivityRedactedPermission(ctx); err != nil {
		return nil, err
	}

	hasViewActivityRedacted, err := s.privilegeChecker.hasViewActivityRedactedPermissionAndNoAdmin(ctx)
	if err != nil {
		return nil, serverError(ctx, err)
	}

	locks, err := queryClusterLocks(ctx, s.sqlServer.internalExecutor, hasViewActivityRedacted /* shouldRedactKeys*/)
	if err != nil {
		return nil, err
	}

	return &serverpb.ClusterLocksStateResponse{
		Locks: locks,
	}, nil
}

// ClusterLocksState implementation for tenants. This is basically equivalent to the above
// statusServer implementation as the underlying implementation is a query to a virtual
// table.
func (t *tenantStatusServer) ClusterLocksState(
	ctx context.Context, _ *serverpb.ClusterLocksStateRequest,
) (*serverpb.ClusterLocksStateResponse, error) {
	ctx = propagateGatewayMetadata(ctx)
	ctx = t.AnnotateCtx(ctx)

	// Check permissions early to avoid fan-out to all nodes.
	if err := t.privilegeChecker.requireViewActivityOrViewActivityRedactedPermission(ctx); err != nil {
		return nil, err
	}

	hasViewActivityRedacted, err := t.privilegeChecker.hasViewActivityRedactedPermissionAndNoAdmin(ctx)
	if err != nil {
		return nil, err
	}

	locks, err := queryClusterLocks(ctx, t.sqlServer.internalExecutor, hasViewActivityRedacted)
	if err != nil {
		return nil, err
	}

	return &serverpb.ClusterLocksStateResponse{
		Locks: locks,
	}, nil
}
