// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package migrationstable

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// MarkMigrationCompleted inserts a row in system.migrations.
func MarkMigrationCompleted(ctx context.Context, ie isql.Executor, v roachpb.Version) error {
	return markMigrationCompletedInner(ctx, ie, v, false /* ignoreExisting */)
}

// MarkMigrationCompletedIdempotent is like MarkMigrationCompleted, except it
// can be called multiple times (or by multiple nodes, concurrently) without
// failing if the respective row already exists. If the row already exists, is
// not changed.
func MarkMigrationCompletedIdempotent(
	ctx context.Context, ie isql.Executor, v roachpb.Version,
) error {
	return markMigrationCompletedInner(ctx, ie, v, true /* ignoreExisting */)
}

func markMigrationCompletedInner(
	ctx context.Context, ie isql.Executor, v roachpb.Version, ignoreExisting bool,
) error {
	query := `
	INSERT
	INTO system.migrations
	(
		major,
		minor,
		patch,
		internal,
		completed_at
	)
	VALUES ($1, $2, $3, $4, $5)
`
	if ignoreExisting {
		query += "ON CONFLICT DO NOTHING"
	}

	_, err := ie.ExecEx(
		ctx,
		"migration-job-mark-job-succeeded",
		nil, /* txn */
		sessiondata.NodeUserSessionDataOverride,
		query,
		v.Major,
		v.Minor,
		v.Patch,
		v.Internal,
		timeutil.Now())
	return err
}

type StaleReadOpt bool

const (
	StaleRead      StaleReadOpt = true
	ConsistentRead              = false
)

// CheckIfMigrationCompleted queries the system.upgrades table to determine
// if the upgrade associated with this version has already been completed.
// The txn may be nil, in which case the check will be run in its own
// transaction.
//
// staleOpt dictates whether the check will run a consistent read or a stale,
// follower read. If txn is not nil, only ConsistentRead can be specified. If
// enterpriseEnabled is set and the StaleRead option is passed, then AS OF
// SYSTEM TIME with_max_staleness('1h') is used for the query.
func CheckIfMigrationCompleted(
	ctx context.Context,
	v roachpb.Version,
	txn *kv.Txn,
	ex isql.Executor,
	enterpriseEnabled bool,
	staleOpt StaleReadOpt,
) (alreadyCompleted bool, _ error) {
	if txn != nil && staleOpt == StaleRead {
		return false, errors.AssertionFailedf(
			"CheckIfMigrationCompleted: cannot ask for stale read when running in a txn")
	}
	queryFormat := `
SELECT count(*)
	FROM system.migrations
  %s
 WHERE major = $1
	 AND minor = $2
	 AND patch = $3
	 AND internal = $4
`
	var query string
	if staleOpt == StaleRead && enterpriseEnabled {
		query = fmt.Sprintf(queryFormat, "AS OF SYSTEM TIME with_max_staleness('1h')")
	} else {
		query = fmt.Sprintf(queryFormat, "")
	}

	row, err := ex.QueryRow(
		ctx,
		"migration-job-find-already-completed",
		txn,
		query,
		v.Major,
		v.Minor,
		v.Patch,
		v.Internal)
	if err != nil {
		return false, err
	}
	count := *row[0].(*tree.DInt)
	return count != 0, nil
}
