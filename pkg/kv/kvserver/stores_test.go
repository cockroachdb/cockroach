// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"
	"path"
	"reflect"
	"strconv"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/logstore"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/disk"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func newStores(ambientCtx log.AmbientContext, clock *hlc.Clock) *Stores {
	return NewStores(ambientCtx, clock)
}

func TestStoresAddStore(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ls := newStores(log.MakeTestingAmbientCtxWithNewTracer(), hlc.NewClockForTesting(nil))
	store := Store{
		Ident: &roachpb.StoreIdent{StoreID: 123},
	}
	ls.AddStore(&store)
	if !ls.HasStore(store.Ident.StoreID) {
		t.Errorf("expected local sender to contain storeID=%d", store.Ident.StoreID)
	}
	if ls.HasStore(store.Ident.StoreID + 1) {
		t.Errorf("expected local sender to not contain storeID=%d", store.Ident.StoreID+1)
	}
}

func TestStoresRemoveStore(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ls := newStores(log.MakeTestingAmbientCtxWithNewTracer(), hlc.NewClockForTesting(nil))

	storeID := roachpb.StoreID(89)

	ls.AddStore(&Store{Ident: &roachpb.StoreIdent{StoreID: storeID}})

	ls.RemoveStore(&Store{Ident: &roachpb.StoreIdent{StoreID: storeID}})

	if ls.HasStore(storeID) {
		t.Errorf("expted local sender to remove storeID=%d", storeID)
	}
}

func TestStoresGetStoreCount(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ls := newStores(log.MakeTestingAmbientCtxWithNewTracer(), hlc.NewClockForTesting(nil))
	if ls.GetStoreCount() != 0 {
		t.Errorf("expected 0 stores in new local sender")
	}

	expectedCount := 10
	for i := 0; i < expectedCount; i++ {
		ls.AddStore(&Store{Ident: &roachpb.StoreIdent{StoreID: roachpb.StoreID(i)}})
	}
	if count := ls.GetStoreCount(); count != expectedCount {
		t.Errorf("expected store count to be %d but was %d", expectedCount, count)
	}
}

func TestStoresVisitStores(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ls := newStores(log.MakeTestingAmbientCtxWithNewTracer(), hlc.NewClockForTesting(nil))
	numStores := 10
	for i := 0; i < numStores; i++ {
		ls.AddStore(&Store{Ident: &roachpb.StoreIdent{StoreID: roachpb.StoreID(i)}})
	}

	visit := make([]bool, numStores)
	err := ls.VisitStores(func(s *Store) error { visit[s.Ident.StoreID] = true; return nil })
	if err != nil {
		t.Errorf("unexpected error on visit: %s", err.Error())
	}

	for i, visited := range visit {
		if !visited {
			t.Errorf("store %d was not visited", i)
		}
	}

	errBoom := errors.New("boom")
	if err := ls.VisitStores(func(s *Store) error {
		return errBoom
	}); !errors.Is(err, errBoom) {
		t.Errorf("got unexpected error %v", err)
	}
}

func TestStoresGetReplicaForRangeID(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	clock := hlc.NewClockForTesting(nil)

	ls := newStores(log.MakeTestingAmbientCtxWithNewTracer(), clock)
	numStores := 10
	for i := 1; i <= numStores; i++ {
		storeID := roachpb.StoreID(i)
		rangeID := roachpb.RangeID(i)
		replicaID := roachpb.ReplicaID(1)

		memEngine := storage.NewDefaultInMemForTesting()
		stopper.AddCloser(memEngine)

		cfg := TestStoreConfig(clock)
		cfg.Transport = NewDummyRaftTransport(cfg.AmbientCtx, cfg.Settings, cfg.Clock)

		store := NewStore(ctx, cfg, memEngine, &roachpb.NodeDescriptor{NodeID: 1})
		// Fake-set an ident. This is usually read from the engine on store.Start()
		// which we're not even going to call.
		store.Ident = &roachpb.StoreIdent{StoreID: storeID}
		ls.AddStore(store)

		desc := &roachpb.RangeDescriptor{
			RangeID:  rangeID,
			StartKey: roachpb.RKey("a"),
			EndKey:   roachpb.RKey("b"),
			InternalReplicas: []roachpb.ReplicaDescriptor{
				{
					StoreID:   storeID,
					ReplicaID: replicaID,
					NodeID:    1,
				},
			},
		}

		require.NoError(t,
			logstore.NewStateLoader(desc.RangeID).SetRaftReplicaID(ctx, store.TODOEngine(), replicaID))
		replica, err := loadInitializedReplicaForTesting(ctx, store, desc, replicaID)
		if err != nil {
			t.Fatalf("unexpected error when creating replica: %+v", err)
		}
		err2 := store.AddReplica(replica)
		if err2 != nil {
			t.Fatalf("unexpected error when adding replica: %v", err2)
		}
	}

	// Test the case where the replica we're looking for exists.
	rangeID1 := roachpb.RangeID(5)
	replica1, _, err1 := ls.GetReplicaForRangeID(ctx, rangeID1)
	if replica1 == nil {
		t.Fatal("expected replica to be found; was nil")
	}
	if err1 != nil {
		t.Fatalf("expected err to be nil; was %v", err1)
	}
	if replica1.RangeID != rangeID1 {
		t.Fatalf("expected replica's range id to be %v; got %v", rangeID1, replica1.RangeID)
	}

	// Test the case where the replica we're looking for doesn't exist.
	rangeID2 := roachpb.RangeID(1000)
	replica2, _, err2 := ls.GetReplicaForRangeID(ctx, rangeID2)
	if replica2 != nil {
		t.Fatalf("expected replica to be nil; was %v", replica2)
	}
	expectedError := kvpb.NewRangeNotFoundError(rangeID2, 0)
	if err2.Error() != expectedError.Error() {
		t.Fatalf("expected err to be %v; was %v", expectedError, err2)
	}
}

func TestStoresGetStore(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ls := newStores(log.MakeTestingAmbientCtxWithNewTracer(), hlc.NewClockForTesting(nil))
	store := Store{Ident: &roachpb.StoreIdent{StoreID: 1}}
	replica := roachpb.ReplicaDescriptor{StoreID: store.Ident.StoreID}
	s, pErr := ls.GetStore(replica.StoreID)
	if s != nil || pErr == nil {
		t.Errorf("expected no stores in new local sender")
	}

	ls.AddStore(&store)
	s, pErr = ls.GetStore(replica.StoreID)
	if s == nil {
		t.Errorf("expected store")
	} else if s.Ident.StoreID != store.Ident.StoreID {
		t.Errorf("expected storeID to be %d but was %d",
			s.Ident.StoreID, store.Ident.StoreID)
	} else if pErr != nil {
		t.Errorf("expected no error, instead had err=%s", pErr)
	}
}

var storeIDAlloc roachpb.StoreID

// createStores creates a slice of count stores.
func createStores(count int) (*timeutil.ManualTime, []*Store, *Stores, *stop.Stopper) {
	stopper := stop.NewStopper()
	manual := timeutil.NewManualTime(timeutil.Unix(0, 123))
	cfg := TestStoreConfig(hlc.NewClockForTesting(manual))
	ls := newStores(log.MakeTestingAmbientCtxWithNewTracer(), cfg.Clock)

	// Create two stores with ranges we care about.
	stores := []*Store{}
	for i := 0; i < count; i++ {
		cfg.Transport = NewDummyRaftTransport(cfg.AmbientCtx, cfg.Settings, cfg.Clock)
		eng := storage.NewDefaultInMemForTesting()
		stopper.AddCloser(eng)
		s := NewStore(context.Background(), cfg, eng, &roachpb.NodeDescriptor{NodeID: 1})
		storeIDAlloc++
		s.Ident = &roachpb.StoreIdent{StoreID: storeIDAlloc}
		stores = append(stores, s)
	}

	return manual, stores, ls, stopper
}

// TestStoresGossipStorage verifies reading and writing of bootstrap info.
func TestStoresGossipStorage(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	manual, stores, ls, stopper := createStores(2)
	defer stopper.Stop(context.Background())
	ls.AddStore(stores[0])

	// Verify initial read is empty.
	var bi gossip.BootstrapInfo
	if err := ls.ReadBootstrapInfo(&bi); err != nil {
		t.Fatal(err)
	}
	if len(bi.Addresses) != 0 {
		t.Errorf("expected empty bootstrap info: %+v", bi)
	}

	// Add a fake address and write.
	manual.Advance(1)
	bi.Addresses = append(bi.Addresses, util.MakeUnresolvedAddr("tcp", "127.0.0.1:8001"))
	if err := ls.WriteBootstrapInfo(&bi); err != nil {
		t.Fatal(err)
	}

	// Verify on read.
	manual.Advance(1)
	var newBI gossip.BootstrapInfo
	if err := ls.ReadBootstrapInfo(&newBI); err != nil {
		t.Fatal(err)
	}
	if len(newBI.Addresses) != 1 {
		t.Errorf("expected single bootstrap info address: %+v", newBI)
	}

	// Add another store and verify it has bootstrap info written.
	ls.AddStore(stores[1])

	// Create a new stores object to verify read.
	ls2 := newStores(log.MakeTestingAmbientCtxWithNewTracer(), ls.clock)
	ls2.AddStore(stores[1])
	var verifyBI gossip.BootstrapInfo
	if err := ls2.ReadBootstrapInfo(&verifyBI); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(bi, verifyBI) {
		t.Errorf("bootstrap info %+v not equal to expected %+v", verifyBI, bi)
	}
}

// TestStoresGossipStorageReadLatest verifies that the latest
// bootstrap info from multiple stores is returned on Read.
func TestStoresGossipStorageReadLatest(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	manual, stores, ls, stopper := createStores(2)
	defer stopper.Stop(context.Background())
	ls.AddStore(stores[0])

	// Add a fake address and write.
	var bi gossip.BootstrapInfo
	bi.Addresses = append(bi.Addresses, util.MakeUnresolvedAddr("tcp", "127.0.0.1:8001"))
	if err := ls.WriteBootstrapInfo(&bi); err != nil {
		t.Fatal(err)
	}

	// Now remove store 0 and add store 1.
	ls.RemoveStore(stores[0])
	ls.AddStore(stores[1])

	// Increment clock, add another address and write.
	manual.Advance(1)
	bi.Addresses = append(bi.Addresses, util.MakeUnresolvedAddr("tcp", "127.0.0.1:8002"))
	if err := ls.WriteBootstrapInfo(&bi); err != nil {
		t.Fatal(err)
	}

	// Create a new stores object to freshly read. Should get latest
	// version from store 1.
	manual.Advance(1)
	ls2 := newStores(log.MakeTestingAmbientCtxWithNewTracer(), ls.clock)
	ls2.AddStore(stores[0])
	ls2.AddStore(stores[1])
	var verifyBI gossip.BootstrapInfo
	if err := ls2.ReadBootstrapInfo(&verifyBI); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(bi, verifyBI) {
		t.Errorf("bootstrap info %+v not equal to expected %+v", verifyBI, bi)
	}

	// Verify that stores[0], which had old info, was updated with
	// latest bootstrap info during the read.
	ls3 := newStores(log.MakeTestingAmbientCtxWithNewTracer(), ls.clock)
	ls3.AddStore(stores[0])
	verifyBI.Reset()
	if err := ls3.ReadBootstrapInfo(&verifyBI); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(bi, verifyBI) {
		t.Errorf("bootstrap info %+v not equal to expected %+v", verifyBI, bi)
	}
}

func TestRegisterDiskMonitors(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	dir, dirCleanupFn := testutils.TempDir(t)
	defer dirCleanupFn()

	_, stores, ls, stopper := createStores(2)
	defer stopper.Stop(context.Background())
	defer ls.CloseDiskMonitors()

	ls.AddStore(stores[0])
	ls.AddStore(stores[1])

	defaultFS := vfs.Default
	diskManager := disk.NewMonitorManager(defaultFS)
	diskMonitors := make(map[roachpb.StoreID]DiskStatsMonitor, len(stores))
	for i, store := range stores {
		storePath := path.Join(dir, strconv.Itoa(i))
		_, err := defaultFS.Create(storePath, fs.UnspecifiedWriteCategory)
		require.NoError(t, err)
		require.Nil(t, store.diskMonitor)

		monitor, err := diskManager.Monitor(storePath)
		require.NoError(t, err)
		defer monitor.Close()
		diskMonitors[store.StoreID()] = monitor
	}

	err := ls.RegisterDiskMonitors(diskMonitors)
	require.NoError(t, err)
	for _, store := range stores {
		require.NotNil(t, store.diskMonitor)
	}
}
