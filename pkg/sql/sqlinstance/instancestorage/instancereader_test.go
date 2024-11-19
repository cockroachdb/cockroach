// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package instancestorage_test

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/settingswatcher"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlinstance"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlinstance/instancestorage"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness/slstorage"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness/sqllivenesstestutils"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
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
	srv, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()
	tDB := sqlutils.MakeSQLRunner(sqlDB)
	// Enable rangefeed for the test.
	for _, l := range []serverutils.ApplicationLayerInterface{s, srv.SystemLayer()} {
		kvserver.RangefeedEnabled.Override(ctx, &l.ClusterSettings().SV, true)
	}

	setup := func(t *testing.T) (
		*instancestorage.Storage, *slstorage.FakeStorage, *hlc.Clock, *instancestorage.Reader,
	) {
		dbName := t.Name()
		tDB.Exec(t, `CREATE DATABASE "`+dbName+`"`)
		schema := instancestorage.GetTableSQLForDatabase(dbName)
		tDB.Exec(t, schema)
		table := desctestutils.TestingGetPublicTableDescriptor(s.DB(), s.Codec(), dbName, "sql_instances")
		slStorage := slstorage.NewFakeStorage()
		storage := instancestorage.NewTestingStorage(s.DB(), s.Codec(), table, slStorage, s.ClusterSettings(), s.Clock(), s.RangeFeedFactory().(*rangefeed.Factory), s.SettingsWatcher().(*settingswatcher.SettingsWatcher))
		reader := instancestorage.NewTestingReader(storage, slStorage, s.AppStopper(), s.DB())
		return storage, slStorage, s.Clock(), reader
	}

	t.Run("unstarted-reader", func(t *testing.T) {
		_, _, _, reader := setup(t)
		_, err := reader.GetInstance(ctx, 1)
		require.ErrorIs(t, err, sqlinstance.NonExistentInstanceError)
	})
	t.Run("read-without-waiting", func(t *testing.T) {
		storage, slStorage, clock, reader := setup(t)
		session := makeSession()
		const rpcAddr = "rpcAddr"
		const sqlAddr = "sqlAddr"
		binaryVersion := roachpb.Version{Major: 23, Minor: 2}
		locality := roachpb.Locality{Tiers: []roachpb.Tier{{Key: "region", Value: "test"}, {Key: "az", Value: "a"}}}
		// Set a high enough expiration to ensure the session stays
		// live through the test.
		const expiration = 10 * time.Minute
		session.StartTS = clock.Now()
		session.ExpTS = session.StartTS.Add(expiration.Nanoseconds(), 0)
		instance, err := storage.CreateInstance(ctx, session, rpcAddr, sqlAddr, locality, binaryVersion)
		require.NoError(t, err)
		err = slStorage.Insert(ctx, session.ID(), session.Expiration())
		require.NoError(t, err)
		reader.Start(ctx, instance)

		// Attempt to get instance without waiting.
		instanceInfo, err := reader.GetInstance(ctx, instance.InstanceID)
		require.NoError(t, err)
		require.Equal(t, rpcAddr, instanceInfo.InstanceRPCAddr)
		require.Equal(t, sqlAddr, instanceInfo.InstanceSQLAddr)
		require.Equal(t, locality, instanceInfo.Locality)
		require.Equal(t, binaryVersion, instanceInfo.BinaryVersion)
	})
	t.Run("basic-get-instance-data", func(t *testing.T) {
		storage, slStorage, clock, reader := setup(t)
		reader.Start(ctx, sqlinstance.InstanceInfo{})
		require.NoError(t, reader.WaitForStarted(ctx))
		session := makeSession()
		const rpcAddr = "rpcAddr"
		const sqlAddr = "sqlAddr"
		binaryVersion := roachpb.Version{Major: 25, Minor: 3}
		locality := roachpb.Locality{Tiers: []roachpb.Tier{{Key: "region", Value: "test"}, {Key: "az", Value: "a"}}}
		// Set a high enough expiration to ensure the session stays
		// live through the test.
		const expiration = 10 * time.Minute
		{
			session.StartTS = clock.Now()
			session.ExpTS = session.StartTS.Add(expiration.Nanoseconds(), 0)
			instance, err := storage.CreateInstance(ctx, session, rpcAddr, sqlAddr, locality, binaryVersion)
			if err != nil {
				t.Fatal(err)
			}
			err = slStorage.Insert(ctx, session.ID(), session.Expiration())
			if err != nil {
				t.Fatal(err)
			}
			testutils.SucceedsSoon(t, func() error {
				instanceInfo, err := reader.GetInstance(ctx, instance.InstanceID)
				if err != nil {
					return err
				}
				if rpcAddr != instanceInfo.InstanceRPCAddr {
					return errors.Newf("expected instance address %s != actual instance address %s", rpcAddr, instanceInfo.InstanceRPCAddr)
				}
				if sqlAddr != instanceInfo.InstanceSQLAddr {
					return errors.Newf("expected instance sql address %s != actual sql instance address %s", sqlAddr, instanceInfo.InstanceSQLAddr)
				}
				if !locality.Equals(instanceInfo.Locality) {
					return errors.Newf("expected instance locality %s != actual instance locality %s", locality, instanceInfo.Locality)
				}
				if binaryVersion != instanceInfo.BinaryVersion {
					return errors.Newf("expected binary version %s != actual binary version %s", binaryVersion, instanceInfo.BinaryVersion)
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
		reader.Start(ctx, sqlinstance.InstanceInfo{})
		require.NoError(t, reader.WaitForStarted(ctx))

		// Set up expected test data.
		instanceIDs := []base.SQLInstanceID{1, 2, 3}
		rpcAddresses := []string{"addr1", "addr2", "addr3"}
		sqlAddresses := []string{"addr4", "addr5", "addr6"}
		sessions := []*sqllivenesstestutils.FakeSession{makeSession(), makeSession(), makeSession()}
		localities := []roachpb.Locality{
			{Tiers: []roachpb.Tier{{Key: "region", Value: "region1"}}},
			{Tiers: []roachpb.Tier{{Key: "region", Value: "region2"}}},
			{Tiers: []roachpb.Tier{{Key: "region", Value: "region3"}}},
		}
		binaryVersions := []roachpb.Version{
			{Major: 22, Minor: 2}, {Major: 23, Minor: 1}, {Major: 23, Minor: 2},
		}

		type expectations struct {
			instanceIDs    []base.SQLInstanceID
			rpcAddresses   []string
			sqlAddresses   []string
			sessionIDs     []sqlliveness.SessionID
			localities     []roachpb.Locality
			binaryVersions []roachpb.Version
		}

		testOutputFn := func(exp expectations, actualInstances []sqlinstance.InstanceInfo) error {
			if len(exp.instanceIDs) != len(actualInstances) {
				return errors.Newf("expected %d instances, got %d instances", len(exp.instanceIDs), len(actualInstances))
			}
			for index, instance := range actualInstances {
				if exp.instanceIDs[index] != instance.InstanceID {
					return errors.Newf("expected instance ID %d != actual instance ID %d", exp.instanceIDs[index], instance.InstanceID)
				}
				if exp.rpcAddresses[index] != instance.InstanceRPCAddr {
					return errors.Newf("expected instance address %s != actual instance address %s", exp.rpcAddresses[index], instance.InstanceRPCAddr)
				}
				if exp.sqlAddresses[index] != instance.InstanceSQLAddr {
					return errors.Newf("expected instance address %s != actual instance address %s", exp.sqlAddresses[index], instance.InstanceSQLAddr)
				}

				if exp.sessionIDs[index] != instance.SessionID {
					return errors.Newf("expected session ID %s != actual session ID %s", exp.sessionIDs[index], instance.SessionID)
				}
				if !exp.localities[index].Equals(instance.Locality) {
					return errors.Newf("expected instance locality %s != actual instance locality %s", exp.localities[index], instance.Locality)
				}
				if exp.binaryVersions[index] != instance.BinaryVersion {
					return errors.Newf("expected binary version %s != actual binary version %s", exp.binaryVersions[index], instance.BinaryVersion)
				}
			}
			return nil
		}

		getInstancesUsingTxn := func(t *testing.T) ([]sqlinstance.InstanceInfo, error) {
			var instancesUsingTxn []sqlinstance.InstanceInfo
			if err := s.DB().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
				var err error
				instancesUsingTxn, err = reader.GetAllInstancesUsingTxn(ctx, txn)
				return err
			}); err != nil {
				return nil, err
			}
			return instancesUsingTxn, nil
		}

		verifyInstancesWithGetter := func(t *testing.T, name string, exp expectations, getInstances func() ([]sqlinstance.InstanceInfo, error)) error {
			instances, err := getInstances()
			if err != nil {
				return errors.Wrapf(err, "%s", name)
			}
			instancestorage.SortInstances(instances)
			return errors.Wrapf(testOutputFn(exp, instances), "%s", name)
		}
		verifyInstances := func(t *testing.T, exp expectations) error {
			if err := verifyInstancesWithGetter(t, "reader", exp, func() ([]sqlinstance.InstanceInfo, error) {
				return reader.GetAllInstances(ctx)
			}); err != nil {
				return err
			}
			return verifyInstancesWithGetter(t, "txn", exp, func() ([]sqlinstance.InstanceInfo, error) {
				return getInstancesUsingTxn(t)
			})
		}

		expectationsFromOffset := func(offset int) expectations {
			sessionIDs := make([]sqlliveness.SessionID, 0, len(sessions[offset:]))
			for _, session := range sessions[offset:] {
				sessionIDs = append(sessionIDs, session.ID())
			}
			return expectations{
				instanceIDs:    instanceIDs[offset:],
				rpcAddresses:   rpcAddresses[offset:],
				sqlAddresses:   sqlAddresses[offset:],
				sessionIDs:     sessionIDs,
				localities:     localities[offset:],
				binaryVersions: binaryVersions[offset:],
			}
		}

		{
			// Set up mock data within instance and session storage.
			for index, rpcAddr := range rpcAddresses {
				sessions[index].StartTS = clock.Now()
				sessions[index].ExpTS = sessions[index].StartTS.Add(expiration.Nanoseconds(), 0)
				_, err := storage.CreateInstance(ctx, sessions[index], rpcAddr, sqlAddresses[index], localities[index], binaryVersions[index])
				if err != nil {
					t.Fatal(err)
				}
				err = slStorage.Insert(ctx, sessions[index].SessionID, sessions[index].Expiration())
				if err != nil {
					t.Fatal(err)
				}
			}
			// Verify all instances are returned by GetAllInstances.
			testutils.SucceedsSoon(t, func() error {
				return verifyInstances(t, expectationsFromOffset(0))
			})
		}

		// Release an instance and verify only active instances are returned.
		{
			err := storage.ReleaseInstance(ctx, sessions[0].ID(), instanceIDs[0])
			if err != nil {
				t.Fatal(err)
			}
			testutils.SucceedsSoon(t, func() error {
				return verifyInstances(t, expectationsFromOffset(1))
			})
		}

		// Verify instances with expired sessions are filtered out.
		{
			err := slStorage.Delete(ctx, sessions[1].ID())
			if err != nil {
				t.Fatal(err)
			}
			testutils.SucceedsSoon(t, func() error {
				return verifyInstances(t, expectationsFromOffset(2))
			})
		}

		// When multiple instances have the same address, verify that only
		// the latest instance information is returned. This heuristic is used
		// when instance information isn't released correctly prior to SQL instance shutdown.
		{
			session := makeSession()
			locality := roachpb.Locality{Tiers: []roachpb.Tier{{Key: "region", Value: "region4"}}}
			session.StartTS = clock.Now()
			session.ExpTS = session.StartTS.Add(expiration.Nanoseconds(), 0)
			instance, err := storage.CreateInstance(ctx, session, rpcAddresses[2], sqlAddresses[2], locality, binaryVersions[2])
			if err != nil {
				t.Fatal(err)
			}
			err = slStorage.Insert(ctx, session.ID(), session.Expiration())
			if err != nil {
				t.Fatal(err)
			}
			testutils.SucceedsSoon(t, func() error {
				return verifyInstances(t, expectations{
					[]base.SQLInstanceID{instance.InstanceID}, /* instanceIDs */
					[]string{rpcAddresses[2]},                 /* rpcAddresses */
					[]string{sqlAddresses[2]},                 /* sqlAddresses */
					[]sqlliveness.SessionID{session.ID()},     /* sessions */
					[]roachpb.Locality{locality},              /* localities */
					[]roachpb.Version{binaryVersions[2]},      /* binaryVersions */
				})
			})
		}
	})
	t.Run("release-instance-get-instance", func(t *testing.T) {
		// Set a high enough expiration to ensure the session stays
		// live through the test.
		const expiration = 10 * time.Minute
		storage, slStorage, clock, reader := setup(t)
		reader.Start(ctx, sqlinstance.InstanceInfo{})
		require.NoError(t, reader.WaitForStarted(ctx))
		// Create three instances and release one.
		instanceIDs := [...]base.SQLInstanceID{1, 2, 3}
		rpcAddresses := [...]string{"addr1", "addr2", "addr3"}
		sqlAddresses := [...]string{"addr4", "addr5", "addr6"}
		sessions := [...]*sqllivenesstestutils.FakeSession{makeSession(), makeSession(), makeSession()}
		localities := [...]roachpb.Locality{
			{Tiers: []roachpb.Tier{{Key: "region", Value: "region1"}}},
			{Tiers: []roachpb.Tier{{Key: "region", Value: "region2"}}},
			{Tiers: []roachpb.Tier{{Key: "region", Value: "region3"}}},
		}
		binaryVersions := []roachpb.Version{
			{Major: 22, Minor: 2}, {Major: 23, Minor: 1}, {Major: 23, Minor: 2},
		}
		{
			// Set up mock data within instance and session storage.
			for index, rpcAddr := range rpcAddresses {
				sessions[index].StartTS = clock.Now()
				sessions[index].ExpTS = sessions[index].StartTS.Add(expiration.Nanoseconds(), 0)
				_, err := storage.CreateInstance(ctx, sessions[index], rpcAddr, sqlAddresses[index], localities[index], binaryVersions[index])
				if err != nil {
					t.Fatal(err)
				}
				err = slStorage.Insert(ctx, sessions[index].ID(), sessions[index].Expiration())
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
				if rpcAddresses[0] != instanceInfo.InstanceRPCAddr {
					return errors.Newf("expected instance address %s != actual instance address %s", rpcAddresses[0], instanceInfo.InstanceRPCAddr)
				}
				if sqlAddresses[0] != instanceInfo.InstanceSQLAddr {
					return errors.Newf("expected instance address %s != actual instance address %s", sqlAddresses[0], instanceInfo.InstanceSQLAddr)
				}
				if !localities[0].Equals(instanceInfo.Locality) {
					return errors.Newf("expected instance locality %s != actual instance locality %s", localities[0], instanceInfo.Locality)
				}
				if binaryVersions[0] != instanceInfo.BinaryVersion {
					return errors.Newf("expected binary version %s != actual binary version %s", binaryVersions[0], instanceInfo.BinaryVersion)
				}
				return nil
			})
		}

		// Verify request for released instance data results in an error.
		{
			err := storage.ReleaseInstance(ctx, sessions[0].ID(), instanceIDs[0])
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
			err := slStorage.Delete(ctx, sessions[1].ID())
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
