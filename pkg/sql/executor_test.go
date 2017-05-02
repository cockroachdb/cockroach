// Copyright 2017 The Cockroach Authors.
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
//
// Author: Andrei Matei (andreimatei1@gmail.com)

package sql

import (
	gosql "database/sql"
	"sync/atomic"
	"testing"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/pkg/errors"
)

// Test that the process of preparing a statement can acquire leases. We used to
// have a bug (#14473) where the transaction that the Executor creates for
// preparing a statement was not sufficiently setup for executing the internal
// SQL required for lease acquisition.
//
// The bug only happened when the transaction internally created for the prepare
// didn't perform any KV operations before attempting to execute the SQL in
// LeaseManager. We're going to replicate that here.
//
// The test will manually inject a lease acquisition in a prepare's txn. Testing
// the conditions we want for 14473 by sql statements alone is tricky because it
// requires preparing a statement that:
// a) Attempts to acquire some table leases by id, not by name. This is true
// for, e.g. edits on table referencing foreign keys.
// b) The requested lease must not be in the cache.
// c) The prepare must not execute any KV operations in its txn before
// attempting to execute internal SQL for obtaining the lease (performing any
// KV ops causes the txn to indirectly be more directly filled in). This means,
// among others, that all the table names must be resolved using the lease
// cache; therefor, some leases need to be in the cache and others not.
//
// These conditions are all doable, but the resulting test is much too fragile.
func TestPrepareCanAcquireLeases(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const query = "SELECT 1"
	leaseManagerCh := make(chan error, 1)
	// By atomically setting tableID, we're asking the LeaseStore to check that a
	// particular lease is acquired.
	var dummyTableID uint32
	var s serverutils.TestServerInterface

	// We're going to intercept prepares. When we've intercepted the one we want,
	// we're going to further intercept lease acquisitions.
	testingKnobs := base.TestingKnobs{
		SQLExecutor: &ExecutorTestingKnobs{
			BeforePrepare: func(
				ctx context.Context, stmt string, planner *planner,
			) (*PreparedStatement, error) {
				if query != stmt {
					return nil, nil
				}

				// Acquire a lease and assert that the store did in fact create a
				// new lease.
				_, err := s.LeaseManager().(*LeaseManager).Acquire(
					ctx, planner.txn, sqlbase.ID(dummyTableID), 0 /* version */)
				if err != nil {
					return nil, err
				}
				select {
				case err := <-leaseManagerCh:
					if err != nil {
						return nil, err
					}
				default:
					return nil, errors.Errorf("LeaseManager knob not called. Prepare didn't get a lease.")
				}
				// Return a success sentinel.
				return nil, errors.New("a-OK")
			},
		},
		SQLLeaseManager: &LeaseManagerTestingKnobs{
			LeaseStoreTestingKnobs: LeaseStoreTestingKnobs{
				LeaseAcquiringEvent: func(tableID sqlbase.ID, txn *client.Txn) {
					exp := sqlbase.ID(atomic.LoadUint32(&dummyTableID))
					if exp == 0 || tableID != exp {
						// Not the lease we're interested in.
						return
					}
					// The point of the test is to trigger a lease acquisition in a
					// transaction that hasn't performed any KV ops before.
					if cnt := txn.CommandCount(); cnt != 0 {
						leaseManagerCh <- errors.Errorf("expected virgin txn, got commandCount: %d", cnt)
					} else {
						leaseManagerCh <- nil
					}
				},
			},
		},
	}

	var db *gosql.DB
	var kvDB *client.DB
	s, db, kvDB = serverutils.StartServer(t, base.TestServerArgs{Knobs: testingKnobs})
	defer s.Stopper().Stop(context.TODO())

	// Create a dummy table so we have something to get a lease on.
	_, err := db.Exec("CREATE DATABASE d; CREATE TABLE d.t (i INT primary key);")
	if err != nil {
		t.Fatal(err)
	}
	tableDesc := sqlbase.GetTableDescriptor(kvDB, "d", "t")
	atomic.StoreUint32(&dummyTableID, uint32(tableDesc.ID))

	_, err = db.PrepareContext(context.TODO(), query)
	if !testutils.IsError(err, "a-OK") {
		t.Fatal(err)
	}
}
