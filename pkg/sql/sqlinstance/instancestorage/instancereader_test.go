// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package instancestorage_test

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlinstance"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlinstance/instancestorage"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness/slstorage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestReader verifies that instancereader retrieves SQL instance data correctly.
// TODO(rima): Update tests with rangefeed related tests.
func TestReader(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	tDB := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	dbName := t.Name()
	s := tc.Server(0)
	setup := func(t *testing.T) (
		*stop.Stopper, *instancestorage.Storage, *slstorage.FakeStorage, *hlc.Clock, *instancestorage.Reader,
	) {
		fmt.Printf("DB name %s\n", dbName)
		tDB.Exec(t, `CREATE DATABASE "`+dbName+`"`)
		schema := strings.Replace(systemschema.SQLInstancesTableSchema,
			`CREATE TABLE system.sql_instances`,
			`CREATE TABLE "`+dbName+`".sql_instances`, 1)
		tDB.Exec(t, schema)
		tableID := getTableID(t, tDB, dbName, "sql_instances")
		f, err := rangefeed.NewFactory(s.Stopper(), s.DB(), nil)
		if err != nil {
			t.Fatal(err)
		}
		slStorage := slstorage.NewFakeStorage()
		storage := instancestorage.NewTestingStorage(s.DB(), keys.SystemSQLCodec, tableID, slStorage)
		reader := instancestorage.NewTestingReader(storage, slStorage, f, keys.SystemSQLCodec, tableID, s.Clock(), s.Stopper())
		return s.Stopper(), storage, slStorage, s.Clock(), reader
	}

	t.Run("basic-get-instance-data", func(t *testing.T) {
		_, storage, slStorage, clock, reader := setup(t)
		require.NoError(t, reader.Start(ctx))
		const sessionID = sqlliveness.SessionID("session_id")
		const addr = "addr"
		const expiration = 10 * time.Hour
		{
			sessionExpiry := clock.Now().Add(expiration.Nanoseconds(), 0)
			id, err := storage.CreateInstance(ctx, sessionID, sessionExpiry, addr)
			if err != nil {
				t.Fatal(err)
			}
			err = slStorage.Insert(ctx, sessionID, sessionExpiry)
			if err != nil {
				t.Fatal(err)
			}
			testutils.SucceedsSoon(t, func() error {
				instanceInfo, err := reader.GetInstance(ctx, id)
				if err != nil {
					return err
				}
				if addr != instanceInfo.InstanceAddr {
					return errors.Newf("expected instance address %s != actual instance address %s", addr, instanceInfo.InstanceAddr)
				}
				return nil
			})
		}
	})
	t.Run("release-instance-get-all-instances", func(t *testing.T) {
		const expiration = time.Minute
		stopper, storage, slStorage, clock, reader := setup(t)
		defer stopper.Stop(ctx)
		// Create three instances and release one.
		instanceIDs := [...]base.SQLInstanceID{1, 2, 3}
		addresses := [...]string{"addr1", "addr2", "addr3"}
		sessionIDs := [...]sqlliveness.SessionID{"session1", "session2", "session3"}
		{
			// Set up mock data within instance and session storage.
			for index, addr := range addresses {
				sessionExpiry := clock.Now().Add(expiration.Nanoseconds(), 0)
				_, err := storage.CreateInstance(ctx, sessionIDs[index], sessionExpiry, addr)
				if err != nil {
					t.Fatal(err)
				}
				err = slStorage.Insert(ctx, sessionIDs[index], sessionExpiry)
				if err != nil {
					t.Fatal(err)
				}
			}
		}

		// Verify all instances are returned by GetAllInstances.
		{
			instances, err := reader.GetAllInstances(ctx)
			sort.SliceStable(instances, func(idx1, idx2 int) bool {
				return instances[idx1].InstanceID < instances[idx2].InstanceID
			})
			require.NoError(t, err)
			require.Equal(t, len(instanceIDs), len(instances))
			for index, instance := range instances {
				require.Equal(t, instanceIDs[index], instance.InstanceID)
				require.Equal(t, sessionIDs[index], instance.SessionID)
				require.Equal(t, addresses[index], instance.InstanceAddr)
			}
		}

		// Release an instance and verify only active instances are returned.
		{
			err := storage.ReleaseInstanceID(ctx, instanceIDs[0])
			if err != nil {
				t.Fatal(err)
			}
			instances, err := reader.GetAllInstances(ctx)
			require.NoError(t, err)
			require.Equal(t, 2, len(instances))
			sortInstances(instances)
			for index, instance := range instances {
				require.Equal(t, instanceIDs[index+1], instance.InstanceID)
				require.Equal(t, sessionIDs[index+1], instance.SessionID)
				require.Equal(t, addresses[index+1], instance.InstanceAddr)
			}
		}

		// Verify instances with expired sessions are filtered out.
		{
			err := slStorage.Delete(ctx, sessionIDs[1])
			if err != nil {
				t.Fatal(err)
			}
			var instances []sqlinstance.InstanceInfo
			instances, err = reader.GetAllInstances(ctx)
			sortInstances(instances)
			require.NoError(t, err)
			// One instance ID has been released and one is associated with an expired session.
			// So only one active instance exists at this point.
			require.Equal(t, 1, len(instances))
			require.Equal(t, instanceIDs[2], instances[0].InstanceID)
			require.Equal(t, sessionIDs[2], instances[0].SessionID)
			require.Equal(t, addresses[2], instances[0].InstanceAddr)
		}

		// When multiple instances have the same address, verify that only
		// the latest instance information is returned. This heuristic is used
		// when instance information isn't released correctly prior to SQL instance shutdown.
		{
			var instances []sqlinstance.InstanceInfo
			sessionID := sqlliveness.SessionID("session4")
			sessionExpiry := clock.Now().Add(expiration.Nanoseconds(), 0)
			id, err := storage.CreateInstance(ctx, sessionID, sessionExpiry, addresses[2])
			if err != nil {
				t.Fatal(err)
			}
			err = slStorage.Insert(ctx, sessionID, sessionExpiry)
			if err != nil {
				t.Fatal(err)
			}
			instances, err = reader.GetAllInstances(ctx)
			require.NoError(t, err)
			// Verify returned instance information is for the latest instance.
			require.Equal(t, 1, len(instances))
			require.Equal(t, id, instances[0].InstanceID)
			require.Equal(t, sessionID, instances[0].SessionID)
			require.Equal(t, addresses[2], instances[0].InstanceAddr)
		}
	})
	t.Run("release-instance-get-instance", func(t *testing.T) {
		const expiration = time.Minute
		stopper, storage, slStorage, clock, reader := setup(t)
		defer stopper.Stop(ctx)
		// Create three instances and release one.
		instanceIDs := [...]base.SQLInstanceID{1, 2, 3}
		addresses := [...]string{"addr1", "addr2", "addr3"}
		sessionIDs := [...]sqlliveness.SessionID{"session1", "session2", "session3"}
		{
			// Set up mock data within instance and session storage.
			for index, addr := range addresses {
				sessionExpiry := clock.Now().Add(expiration.Nanoseconds(), 0)
				_, err := storage.CreateInstance(ctx, sessionIDs[index], sessionExpiry, addr)
				if err != nil {
					t.Fatal(err)
				}
				err = slStorage.Insert(ctx, sessionIDs[index], sessionExpiry)
				if err != nil {
					t.Fatal(err)
				}
			}
		}

		// Verify active instance data is returned with no error.
		{
			instanceInfo, err := reader.GetInstance(ctx, instanceIDs[0])
			require.NoError(t, err)
			require.Equal(t, addresses[0], instanceInfo.InstanceAddr)
		}

		// Verify request for released instance data results in an error.
		{
			err := storage.ReleaseInstanceID(ctx, instanceIDs[0])
			if err != nil {
				t.Fatal(err)
			}
			_, err = reader.GetInstance(ctx, instanceIDs[0])
			require.Error(t, err)
			require.ErrorIs(t, err, sqlinstance.NonExistentInstanceError)
		}
		// Verify request for instance with expired session results in an error.
		{
			err := slStorage.Delete(ctx, sessionIDs[1])
			if err != nil {
				t.Fatal(err)
			}
			_, err = reader.GetInstance(ctx, instanceIDs[1])
			require.Error(t, err)
			require.ErrorIs(t, err, sqlinstance.NonExistentInstanceError)
		}
	})
}
