// Copyright 2016 The Cockroach Authors.
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

package migrations

import (
	"bytes"
	"fmt"
	"os"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/pkg/errors"
)

var (
	noopMigration1 = migrationDescriptor{
		name:   "noop 1",
		workFn: func(_ context.Context, _ runner) error { return nil },
	}
	noopMigration2 = migrationDescriptor{
		name:   "noop 2",
		workFn: func(_ context.Context, _ runner) error { return nil },
	}
	addOneDescriptorMigration = migrationDescriptor{
		name:           "add one descriptor",
		workFn:         func(_ context.Context, _ runner) error { return nil },
		newDescriptors: 1,
		newRanges:      1,
	}
	addTwoDescriptorsMigration = migrationDescriptor{
		name:           "add two descriptors",
		workFn:         func(_ context.Context, _ runner) error { return nil },
		newDescriptors: 2,
		newRanges:      2,
	}
	addRangelessDescMigration = migrationDescriptor{
		name:           "add rangeless descriptor",
		workFn:         func(_ context.Context, _ runner) error { return nil },
		newDescriptors: 2,
		newRanges:      1,
	}
	errorMigration = migrationDescriptor{
		name:   "error",
		workFn: func(_ context.Context, _ runner) error { return errors.New("errorMigration") },
	}
	panicMigration = migrationDescriptor{
		name:   "panic",
		workFn: func(_ context.Context, _ runner) error { panic("panicMigration") },
	}
)

type fakeLeaseManager struct {
	extendErr          error
	releaseErr         error
	leaseTimeRemaining time.Duration
}

func (f *fakeLeaseManager) AcquireLease(
	ctx context.Context, key roachpb.Key,
) (*client.Lease, error) {
	return &client.Lease{}, nil
}

func (f *fakeLeaseManager) ExtendLease(ctx context.Context, l *client.Lease) error {
	return f.extendErr
}

func (f *fakeLeaseManager) ReleaseLease(ctx context.Context, l *client.Lease) error {
	return f.releaseErr
}

func (f *fakeLeaseManager) TimeRemaining(l *client.Lease) time.Duration {
	// Default to a reasonable amount of time left if the field wasn't set.
	if f.leaseTimeRemaining == 0 {
		return leaseRefreshInterval * 2
	}
	return f.leaseTimeRemaining
}

type fakeDB struct {
	kvs     map[string][]byte
	scanErr error
	putErr  error
}

func (f *fakeDB) Scan(
	ctx context.Context, begin, end interface{}, maxRows int64,
) ([]client.KeyValue, error) {
	if f.scanErr != nil {
		return nil, f.scanErr
	}
	if !bytes.Equal(begin.(roachpb.Key), keys.MigrationPrefix) {
		return nil, errors.Errorf("expected begin key %q, got %q", keys.MigrationPrefix, begin)
	}
	if !bytes.Equal(end.(roachpb.Key), keys.MigrationKeyMax) {
		return nil, errors.Errorf("expected end key %q, got %q", keys.MigrationKeyMax, end)
	}
	var results []client.KeyValue
	for k, v := range f.kvs {
		results = append(results, client.KeyValue{
			Key:   []byte(k),
			Value: &roachpb.Value{RawBytes: v},
		})
	}
	return results, nil
}

func (f *fakeDB) Put(ctx context.Context, key, value interface{}) error {
	if f.putErr != nil {
		return f.putErr
	}
	f.kvs[string(key.(roachpb.Key))] = []byte(value.(string))
	return nil
}

func (f *fakeDB) Txn(context.Context, func(context.Context, *client.Txn) error) error {
	return errors.New("unimplemented")
}

func TestEnsureMigrations(t *testing.T) {
	defer leaktest.AfterTest(t)()
	db := &fakeDB{}
	mgr := Manager{
		stopper:      stop.NewStopper(),
		leaseManager: &fakeLeaseManager{},
		db:           db,
	}
	defer mgr.stopper.Stop(context.TODO())

	fnGotCalled := false
	fnGotCalledDescriptor := migrationDescriptor{
		name: "got-called-verifier",
		workFn: func(context.Context, runner) error {
			fnGotCalled = true
			return nil
		},
	}
	testCases := []struct {
		preCompleted                                    []migrationDescriptor
		migrations                                      []migrationDescriptor
		expectedErr                                     string
		expectedPreDescriptors, expectedPostDescriptors int
		expectedPreRanges, expectedPostRanges           int
	}{
		{
			nil,
			nil,
			"",
			0, 0, 0, 0,
		},
		{
			nil,
			[]migrationDescriptor{noopMigration1},
			"",
			0, 0, 0, 0,
		},
		{
			[]migrationDescriptor{noopMigration1},
			[]migrationDescriptor{noopMigration1},
			"",
			0, 0, 0, 0,
		},
		{
			[]migrationDescriptor{},
			[]migrationDescriptor{noopMigration1, noopMigration2},
			"",
			0, 0, 0, 0,
		},
		{
			[]migrationDescriptor{noopMigration1},
			[]migrationDescriptor{noopMigration1, noopMigration2},
			"",
			0, 0, 0, 0,
		},
		{
			[]migrationDescriptor{noopMigration1, noopMigration2, panicMigration},
			[]migrationDescriptor{noopMigration1, noopMigration2, panicMigration},
			"",
			0, 0, 0, 0,
		},
		{
			[]migrationDescriptor{noopMigration1, noopMigration2},
			[]migrationDescriptor{noopMigration1, noopMigration2, fnGotCalledDescriptor},
			"",
			0, 0, 0, 0,
		},
		{
			[]migrationDescriptor{noopMigration1, noopMigration2},
			[]migrationDescriptor{noopMigration1, noopMigration2, errorMigration},
			fmt.Sprintf("failed to run migration %q", errorMigration.name),
			0, 0, 0, 0,
		},
		{
			[]migrationDescriptor{noopMigration1},
			[]migrationDescriptor{noopMigration1, addOneDescriptorMigration},
			"",
			0, 1, 0, 1,
		},
		{
			[]migrationDescriptor{addOneDescriptorMigration},
			[]migrationDescriptor{addOneDescriptorMigration, addTwoDescriptorsMigration},
			"",
			1, 3, 1, 3,
		},
		{
			[]migrationDescriptor{addOneDescriptorMigration},
			[]migrationDescriptor{addOneDescriptorMigration, addRangelessDescMigration},
			"",
			1, 3, 1, 2,
		},
		{
			[]migrationDescriptor{},
			[]migrationDescriptor{noopMigration1, addOneDescriptorMigration, noopMigration2, addTwoDescriptorsMigration},
			"",
			0, 3, 0, 3,
		},
	}
	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			db.kvs = make(map[string][]byte)
			for _, name := range tc.preCompleted {
				db.kvs[string(migrationKey(name))] = []byte{}
			}
			backwardCompatibleMigrations = tc.migrations

			preDescriptors, preRanges, err := AdditionalInitialDescriptors(context.Background(), mgr.db)
			if err != nil {
				t.Fatal(err)
			}
			if preDescriptors != tc.expectedPreDescriptors {
				t.Errorf("expected %d additional initial descriptors before migration, got %d",
					tc.expectedPreDescriptors, preDescriptors)
			}
			if preRanges != tc.expectedPreRanges {
				t.Errorf("expected %d additional initial ranges before migration, got %d",
					tc.expectedPreRanges, preRanges)
			}

			err = mgr.EnsureMigrations(context.Background())
			if !testutils.IsError(err, tc.expectedErr) {
				t.Errorf("expected error %q, got error %v", tc.expectedErr, err)
			}
			if err != nil {
				return
			}

			postDescriptors, postRanges, err := AdditionalInitialDescriptors(context.Background(), mgr.db)
			if err != nil {
				t.Fatal(err)
			}
			if postDescriptors != tc.expectedPostDescriptors {
				t.Errorf("expected %d additional initial descriptors after migration, got %d",
					tc.expectedPostDescriptors, postDescriptors)
			}
			if postRanges != tc.expectedPostRanges {
				t.Errorf("expected %d additional initial ranges after migration, got %d",
					tc.expectedPostRanges, postRanges)
			}

			for _, migration := range tc.migrations {
				if _, ok := db.kvs[string(migrationKey(migration))]; !ok {
					t.Errorf("expected key %s to be written, but it wasn't", migrationKey(migration))
				}
			}
			if len(db.kvs) != len(tc.migrations) {
				t.Errorf("expected %d key to be written, but %d were",
					len(tc.migrations), len(db.kvs))
			}
		})
	}
	if !fnGotCalled {
		t.Errorf("expected fnGotCalledDescriptor to be run by the migration coordinator, but it wasn't")
	}
}

func TestDBErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()
	db := &fakeDB{}
	mgr := Manager{
		stopper:      stop.NewStopper(),
		leaseManager: &fakeLeaseManager{},
		db:           db,
	}
	defer mgr.stopper.Stop(context.TODO())

	migration := noopMigration1
	backwardCompatibleMigrations = []migrationDescriptor{migration}
	testCases := []struct {
		scanErr     error
		putErr      error
		expectedErr string
	}{
		{
			nil,
			nil,
			"",
		},
		{
			fmt.Errorf("context deadline exceeded"),
			nil,
			"failed to get list of completed migrations.*context deadline exceeded",
		},
		{
			nil,
			fmt.Errorf("context deadline exceeded"),
			"failed to persist record of completing migration.*context deadline exceeded",
		},
	}
	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			db.scanErr = tc.scanErr
			db.putErr = tc.putErr
			db.kvs = make(map[string][]byte)
			err := mgr.EnsureMigrations(context.Background())
			if !testutils.IsError(err, tc.expectedErr) {
				t.Errorf("expected error %q, got error %v", tc.expectedErr, err)
			}
			if err != nil {
				return
			}
			if _, ok := db.kvs[string(migrationKey(migration))]; !ok {
				t.Errorf("expected key %s to be written, but it wasn't", migrationKey(migration))
			}
			if len(db.kvs) != len(backwardCompatibleMigrations) {
				t.Errorf("expected %d key to be written, but %d were",
					len(backwardCompatibleMigrations), len(db.kvs))
			}
		})
	}
}

// ExtendLease and ReleaseLease errors should not, by themselves, cause the
// migration process to fail. Not being able to acquire the lease should, but
// we don't test that here due to the added code that would be needed to change
// its retry settings to allow for testing it in a reasonable amount of time.
func TestLeaseErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()
	db := &fakeDB{kvs: make(map[string][]byte)}
	mgr := Manager{
		stopper: stop.NewStopper(),
		leaseManager: &fakeLeaseManager{
			extendErr:  fmt.Errorf("context deadline exceeded"),
			releaseErr: fmt.Errorf("context deadline exceeded"),
		},
		db: db,
	}
	defer mgr.stopper.Stop(context.TODO())

	migration := noopMigration1
	backwardCompatibleMigrations = []migrationDescriptor{migration}
	if err := mgr.EnsureMigrations(context.Background()); err != nil {
		t.Error(err)
	}
	if _, ok := db.kvs[string(migrationKey(migration))]; !ok {
		t.Errorf("expected key %s to be written, but it wasn't", migrationKey(migration))
	}
	if len(db.kvs) != len(backwardCompatibleMigrations) {
		t.Errorf("expected %d key to be written, but %d were",
			len(backwardCompatibleMigrations), len(db.kvs))
	}
}

// The lease not having enough time left on it to finish migrations should
// cause the process to exit via a call to log.Fatal.
func TestLeaseExpiration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	db := &fakeDB{kvs: make(map[string][]byte)}
	mgr := Manager{
		stopper:      stop.NewStopper(),
		leaseManager: &fakeLeaseManager{leaseTimeRemaining: time.Nanosecond},
		db:           db,
	}
	defer mgr.stopper.Stop(context.TODO())

	oldLeaseRefreshInterval := leaseRefreshInterval
	leaseRefreshInterval = time.Microsecond
	defer func() { leaseRefreshInterval = oldLeaseRefreshInterval }()

	exitCalled := make(chan bool)
	log.SetExitFunc(func(int) { exitCalled <- true })
	defer log.SetExitFunc(os.Exit)

	waitForExitMigration := migrationDescriptor{
		name: "wait for exit to be called",
		workFn: func(context.Context, runner) error {
			select {
			case <-exitCalled:
				return nil
			case <-time.After(10 * time.Second):
				return fmt.Errorf("timed out waiting for exit to be called")
			}
		},
	}
	backwardCompatibleMigrations = []migrationDescriptor{waitForExitMigration}
	if err := mgr.EnsureMigrations(context.Background()); err != nil {
		t.Error(err)
	}
}
