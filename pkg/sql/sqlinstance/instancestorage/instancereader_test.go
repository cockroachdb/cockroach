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
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
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
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestReader verifies that instancereader retrieves SQL instance data correctly.
func TestReader(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)
	s := tc.Server(0)
	tDB := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	// Enable rangefeed for the test.
	tDB.Exec(t, `SET CLUSTER SETTING kv.rangefeed.enabled = true`)
	setup := func(t *testing.T) (
		*instancestorage.Storage, *slstorage.FakeStorage, *hlc.Clock, *instancestorage.Reader,
	) {
		dbName := t.Name()
		tDB.Exec(t, `CREATE DATABASE "`+dbName+`"`)
		schema := strings.Replace(systemschema.SQLInstancesTableSchema,
			`CREATE TABLE system.sql_instances`,
			`CREATE TABLE "`+dbName+`".sql_instances`, 1)
		tDB.Exec(t, schema)
		tableID := getTableID(t, tDB, dbName, "sql_instances")
		slStorage := slstorage.NewFakeStorage()
		storage := instancestorage.NewTestingStorage(s.DB(), keys.SystemSQLCodec, tableID, slStorage)
		reader := instancestorage.NewTestingReader(storage, slStorage, s.RangeFeedFactory().(*rangefeed.Factory), keys.SystemSQLCodec, tableID, s.Clock(), s.Stopper())
		return storage, slStorage, s.Clock(), reader
	}

	t.Run("unstarted-reader", func(t *testing.T) {
		_, _, _, reader := setup(t)
		_, err := reader.GetInstance(ctx, 1)
		require.ErrorIs(t, err, sqlinstance.NotStartedError)
	})

	t.Run("basic-get-instance-data", func(t *testing.T) {
		storage, slStorage, clock, reader := setup(t)
		require.NoError(t, reader.Start(ctx))
		const sessionID = sqlliveness.SessionID("session_id")
		const addr = "addr"
		locality := roachpb.Locality{Tiers: []roachpb.Tier{{Key: "region", Value: "test"}, {Key: "az", Value: "a"}}}
		// Set a high enough expiration to ensure the session stays
		// live through the test.
		const expiration = 10 * time.Minute
		{
			sessionExpiry := clock.Now().Add(expiration.Nanoseconds(), 0)
			id, err := storage.CreateInstance(ctx, sessionID, sessionExpiry, addr, locality)
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
				if !locality.Equals(instanceInfo.Locality) {
					return errors.Newf("expected instance locality %s != actual instance locality %s", locality.String(), instanceInfo.Locality.String())
				}
				return nil
			})
		}
	})
	t.Run("release-instance-get-all-instances", func(t *testing.T) {
		// Set a high enough expiration to ensure the session stays
		// live through the test.
		const expiration = 10 * time.Minute
		storage, slStorage, clock, reader := setup(t)
		require.NoError(t, reader.Start(ctx))

		// Set up expected test data.
		instanceIDs := []base.SQLInstanceID{1, 2, 3}
		addresses := []string{"addr1", "addr2", "addr3"}
		sessionIDs := []sqlliveness.SessionID{"session1", "session2", "session3"}
		localities := []roachpb.Locality{
			{Tiers: []roachpb.Tier{{Key: "region", Value: "region1"}}},
			{Tiers: []roachpb.Tier{{Key: "region", Value: "region2"}}},
			{Tiers: []roachpb.Tier{{Key: "region", Value: "region3"}}},
		}

		testOutputFn := func(expectedIDs []base.SQLInstanceID, expectedAddresses []string, expectedSessionIDs []sqlliveness.SessionID, expectedLocalities []roachpb.Locality, actualInstances []sqlinstance.InstanceInfo) error {
			if len(expectedIDs) != len(actualInstances) {
				return errors.Newf("expected %d instances, got %d instances", len(expectedIDs), len(actualInstances))
			}
			for index, instance := range actualInstances {
				if expectedIDs[index] != instance.InstanceID {
					return errors.Newf("expected instance ID %d != actual instance ID %d", expectedIDs[index], instance.InstanceID)
				}
				if expectedAddresses[index] != instance.InstanceAddr {
					return errors.Newf("expected instance address %s != actual instance address %s", expectedAddresses[index], instance.InstanceAddr)
				}
				if expectedSessionIDs[index] != instance.SessionID {
					return errors.Newf("expected session ID %s != actual session ID %s", expectedSessionIDs[index], instance.SessionID)
				}
				if !expectedLocalities[index].Equals(instance.Locality) {
					return errors.Newf("expected instance locality %s != actual instance locality %s", expectedLocalities[index], instance.Locality)
				}
			}
			return nil
		}
		{
			// Set up mock data within instance and session storage.
			for index, addr := range addresses {
				sessionExpiry := clock.Now().Add(expiration.Nanoseconds(), 0)
				_, err := storage.CreateInstance(ctx, sessionIDs[index], sessionExpiry, addr, localities[index])
				if err != nil {
					t.Fatal(err)
				}
				err = slStorage.Insert(ctx, sessionIDs[index], sessionExpiry)
				if err != nil {
					t.Fatal(err)
				}
			}
			// Verify all instances are returned by GetAllInstances.
			testutils.SucceedsSoon(t, func() error {
				instances, err := reader.GetAllInstances(ctx)
				if err != nil {
					return err
				}
				sortInstances(instances)
				return testOutputFn(instanceIDs, addresses, sessionIDs, localities, instances)
			})
		}

		// Release an instance and verify only active instances are returned.
		{
			err := storage.ReleaseInstanceID(ctx, instanceIDs[0])
			if err != nil {
				t.Fatal(err)
			}
			testutils.SucceedsSoon(t, func() error {
				instances, err := reader.GetAllInstances(ctx)
				if err != nil {
					return err
				}
				sortInstances(instances)
				return testOutputFn(instanceIDs[1:], addresses[1:], sessionIDs[1:], localities[1:], instances)
			})
		}

		// Verify instances with expired sessions are filtered out.
		{
			err := slStorage.Delete(ctx, sessionIDs[1])
			if err != nil {
				t.Fatal(err)
			}
			testutils.SucceedsSoon(t, func() error {
				instances, err := reader.GetAllInstances(ctx)
				if err != nil {
					return err
				}
				sortInstances(instances)
				return testOutputFn(instanceIDs[2:], addresses[2:], sessionIDs[2:], localities[2:], instances)
			})
		}

		// When multiple instances have the same address, verify that only
		// the latest instance information is returned. This heuristic is used
		// when instance information isn't released correctly prior to SQL instance shutdown.
		{
			sessionID := sqlliveness.SessionID("session4")
			locality := roachpb.Locality{Tiers: []roachpb.Tier{{Key: "region", Value: "region4"}}}
			sessionExpiry := clock.Now().Add(expiration.Nanoseconds(), 0)
			id, err := storage.CreateInstance(ctx, sessionID, sessionExpiry, addresses[2], locality)
			if err != nil {
				t.Fatal(err)
			}
			err = slStorage.Insert(ctx, sessionID, sessionExpiry)
			if err != nil {
				t.Fatal(err)
			}
			testutils.SucceedsSoon(t, func() error {
				instances, err := reader.GetAllInstances(ctx)
				if err != nil {
					return err
				}
				sortInstances(instances)
				return testOutputFn([]base.SQLInstanceID{id}, []string{addresses[2]}, []sqlliveness.SessionID{sessionID}, []roachpb.Locality{locality}, instances)
			})
		}
	})
	t.Run("release-instance-get-instance", func(t *testing.T) {
		// Set a high enough expiration to ensure the session stays
		// live through the test.
		const expiration = 10 * time.Minute
		storage, slStorage, clock, reader := setup(t)
		require.NoError(t, reader.Start(ctx))
		// Create three instances and release one.
		instanceIDs := [...]base.SQLInstanceID{1, 2, 3}
		addresses := [...]string{"addr1", "addr2", "addr3"}
		sessionIDs := [...]sqlliveness.SessionID{"session1", "session2", "session3"}
		localities := [...]roachpb.Locality{
			{Tiers: []roachpb.Tier{{Key: "region", Value: "region1"}}},
			{Tiers: []roachpb.Tier{{Key: "region", Value: "region2"}}},
			{Tiers: []roachpb.Tier{{Key: "region", Value: "region3"}}},
		}
		{
			// Set up mock data within instance and session storage.
			for index, addr := range addresses {
				sessionExpiry := clock.Now().Add(expiration.Nanoseconds(), 0)
				_, err := storage.CreateInstance(ctx, sessionIDs[index], sessionExpiry, addr, localities[index])
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
			testutils.SucceedsSoon(t, func() error {
				instanceInfo, err := reader.GetInstance(ctx, instanceIDs[0])
				if err != nil {
					return err
				}
				if addresses[0] != instanceInfo.InstanceAddr {
					return errors.Newf("expected instance address %s != actual instance address %s", addresses[0], instanceInfo.InstanceAddr)
				}
				if !localities[0].Equals(instanceInfo.Locality) {
					return errors.Newf("expected instance locality %s != actual instance locality %s", localities[0], instanceInfo.Locality)
				}
				return nil
			})
		}

		// Verify request for released instance data results in an error.
		{
			err := storage.ReleaseInstanceID(ctx, instanceIDs[0])
			if err != nil {
				t.Fatal(err)
			}
			testutils.SucceedsSoon(t, func() error {
				_, err = reader.GetInstance(ctx, instanceIDs[0])
				if !errors.Is(err, sqlinstance.NonExistentInstanceError) {
					return errors.Newf("expected non existent instance error")
				}
				return nil
			})
		}
		// Verify request for instance with expired session results in an error.
		{
			err := slStorage.Delete(ctx, sessionIDs[1])
			if err != nil {
				t.Fatal(err)
			}
			testutils.SucceedsSoon(t, func() error {
				_, err = reader.GetInstance(ctx, instanceIDs[0])
				if !errors.Is(err, sqlinstance.NonExistentInstanceError) {
					return errors.Newf("expected non existent instance error")
				}
				return nil
			})
		}
	})
}
