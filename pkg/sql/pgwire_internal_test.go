// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// This file contains tests for pgwire that need to be in the sql package.

package sql

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// Test that abruptly closing a pgwire connection releases all leases held by
// that session.
func TestPGWireConnectionCloseReleasesLeases(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	srv, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()

	ex, err := db.Conn(ctx)
	require.NoError(t, err)

	_, err = ex.ExecContext(ctx, "CREATE DATABASE test")
	require.NoError(t, err)
	_, err = ex.ExecContext(ctx, "CREATE TABLE test.t (i INT PRIMARY KEY)")
	require.NoError(t, err)

	// Start a txn so leases are accumulated by queries.
	_, err = ex.ExecContext(ctx, "BEGIN")
	require.NoError(t, err)

	// Get a table lease.
	_, err = ex.ExecContext(ctx, "SELECT * FROM test.t")
	require.NoError(t, err)

	// Abruptly close the connection.
	require.NoError(t, ex.Raw(func(driverConn interface{}) error {
		return driverConn.(interface {
			Close() error
		}).Close()
	}))

	// Verify that there are no leases held.
	tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "test", "t")

	lm := s.LeaseManager().(*lease.Manager)

	// Looking for a table state validates that there used to be a lease on the
	// table.
	var leases int
	lm.VisitLeases(func(
		desc catalog.Descriptor, dropped bool, refCount int, expiration tree.DTimestamp,
	) (wantMore bool) {
		if desc.GetID() == tableDesc.GetID() {
			leases++
		}
		return true
	})
	if leases != 1 {
		t.Fatalf("expected one lease, found: %d", leases)
	}

	// Wait for the lease to be released.
	testutils.SucceedsSoon(t, func() error {
		var totalRefCount int
		lm.VisitLeases(func(
			desc catalog.Descriptor, dropped bool, refCount int, expiration tree.DTimestamp,
		) (wantMore bool) {
			if desc.GetID() == tableDesc.GetID() {
				totalRefCount += refCount
			}
			return true
		})
		if totalRefCount != 0 {
			return errors.Errorf(
				"expected lease to be unused, found refcount: %d", totalRefCount)
		}
		return nil
	})
}
