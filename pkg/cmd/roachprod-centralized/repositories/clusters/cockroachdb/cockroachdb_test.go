// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cockroachdb

import (
	"context"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/stretchr/testify/require"
)

var acquireSyncLockQuery = regexp.MustCompile(
	`UPDATE cluster_sync_state\s+SET in_progress = true, instance_id = \$1, started_at = now\(\)\s+` +
		`WHERE id = 1 AND \(in_progress = false OR instance_id = \$1\)`,
)

func TestAcquireSyncLock_AllowsSameInstanceReentry(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
		require.NoError(t, mock.ExpectationsWereMet())
	})

	repo := &CRDBClustersRepo{db: db}

	instanceID := "instance-a"
	mock.ExpectExec(acquireSyncLockQuery.String()).
		WithArgs(instanceID).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectClose()

	acquired, err := repo.AcquireSyncLock(context.Background(), logger.DefaultLogger, instanceID)
	require.NoError(t, err)
	require.True(t, acquired)
}

func TestAcquireSyncLock_PreventsLockTheft(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
		require.NoError(t, mock.ExpectationsWereMet())
	})

	repo := &CRDBClustersRepo{db: db}

	mock.ExpectExec(acquireSyncLockQuery.String()).
		WithArgs("instance-b").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectClose()

	acquired, err := repo.AcquireSyncLock(context.Background(), logger.DefaultLogger, "instance-b")
	require.NoError(t, err)
	require.False(t, acquired)
}
