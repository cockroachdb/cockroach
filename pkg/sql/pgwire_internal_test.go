// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// This file contains tests for pgwire that need to be in the sql package.

package sql

import (
	"context"
	"database/sql/driver"
	"net/url"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq"
)

// Test that abruptly closing a pgwire connection releases all leases held by
// that session.
func TestPGWireConnectionCloseReleasesLeases(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, _, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)
	url, cleanupConn := sqlutils.PGUrl(t, s.ServingSQLAddr(), "SetupServer", url.User(security.RootUser))
	defer cleanupConn()
	conn, err := pq.Open(url.String())
	if err != nil {
		t.Fatal(err)
	}
	ex := conn.(driver.ExecerContext)
	if _, err := ex.ExecContext(ctx, "CREATE DATABASE test", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := ex.ExecContext(ctx, "CREATE TABLE test.t (i INT PRIMARY KEY)", nil); err != nil {
		t.Fatal(err)
	}
	// Start a txn so leases are accumulated by queries.
	if _, err := ex.ExecContext(ctx, "BEGIN", nil); err != nil {
		t.Fatal(err)
	}
	// Get a table lease.
	if _, err := ex.ExecContext(ctx, "SELECT * FROM test.t", nil); err != nil {
		t.Fatal(err)
	}
	// Abruptly close the connection.
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	// Verify that there are no leases held.
	tableDesc := catalogkv.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "test", "t")

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
