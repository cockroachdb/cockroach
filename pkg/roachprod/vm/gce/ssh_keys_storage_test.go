// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package gce

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

type fakeSSHKeysObjectStore struct {
	readContents []byte
	readErr      error
	writeErr     error
	closeErr     error

	readBucket  string
	readObject  string
	writeBucket string
	writeObject string
	written     []byte
	closed      bool
}

func (f *fakeSSHKeysObjectStore) ReadObject(
	_ context.Context, bucket, object string,
) ([]byte, error) {
	f.readBucket = bucket
	f.readObject = object
	return f.readContents, f.readErr
}

func (f *fakeSSHKeysObjectStore) WriteObject(
	_ context.Context, bucket, object string, contents []byte,
) error {
	f.writeBucket = bucket
	f.writeObject = object
	f.written = append([]byte(nil), contents...)
	return f.writeErr
}

func (f *fakeSSHKeysObjectStore) Close() error {
	f.closed = true
	return f.closeErr
}

func withSSHKeysObjectStore(t *testing.T, store sshKeysObjectStore, err error) {
	t.Helper()
	old := newSSHKeysObjectStore
	t.Cleanup(func() {
		newSSHKeysObjectStore = old
	})
	newSSHKeysObjectStore = func(context.Context) (sshKeysObjectStore, error) {
		return store, err
	}
}

func TestSSHKeysBucketForProject(t *testing.T) {
	for _, tc := range []struct {
		project string
		bucket  string
		ok      bool
	}{
		{
			project: DefaultProjectID,
			bucket:  "roachprod-ssh-keys-" + DefaultProjectID,
			ok:      true,
		},
		{
			project: StagingProjectID,
			bucket:  "roachprod-ssh-keys-" + StagingProjectID,
			ok:      true,
		},
		{project: "custom-infra-project"},
		{project: ""},
	} {
		t.Run(tc.project, func(t *testing.T) {
			bucket, ok := SSHKeysBucketForProject(tc.project)
			require.Equal(t, tc.ok, ok)
			require.Equal(t, tc.bucket, bucket)
		})
	}
}

func TestGetUserAuthorizedKeysFromGCS(t *testing.T) {
	store := &fakeSSHKeysObjectStore{readContents: []byte(
		"zoe:" + testAuthorizedKeyTwo + "\nanna:" + testAuthorizedKeyOne + "\n",
	)}
	withSSHKeysObjectStore(t, store, nil)

	keys, issues, err := GetUserAuthorizedKeysFromGCS(context.Background(), DefaultProjectID)

	require.NoError(t, err)
	require.Empty(t, issues)
	require.Equal(t, []string{"anna", "zoe"}, []string{keys[0].User, keys[1].User})
	require.Equal(t, "roachprod-ssh-keys-"+DefaultProjectID, store.readBucket)
	require.Equal(t, sshKeysObjectName, store.readObject)
	require.True(t, store.closed)
}

func TestGetUserAuthorizedKeysFromGCSErrors(t *testing.T) {
	t.Run("unsupported project", func(t *testing.T) {
		_, _, err := GetUserAuthorizedKeysFromGCS(context.Background(), "custom-project")
		require.ErrorContains(t, err, "not configured")
	})

	t.Run("read", func(t *testing.T) {
		store := &fakeSSHKeysObjectStore{readErr: errors.New("read failed")}
		withSSHKeysObjectStore(t, store, nil)

		_, _, err := GetUserAuthorizedKeysFromGCS(context.Background(), StagingProjectID)

		require.ErrorContains(t, err, "read failed")
		require.ErrorContains(t, err, "gs://roachprod-ssh-keys-"+StagingProjectID+"/ssh-keys")
		require.True(t, store.closed)
	})
}

func TestProviderGetUserAuthorizedKeysUsesGCS(t *testing.T) {
	store := &fakeSSHKeysObjectStore{
		readContents: []byte("anna:" + testAuthorizedKeyOne + "\n"),
	}
	withSSHKeysObjectStore(t, store, nil)
	provider := &Provider{
		infraProject: "custom-infra-project", metadataProject: DefaultProjectID,
	}

	keys, err := provider.GetUserAuthorizedKeys()

	require.NoError(t, err)
	require.Len(t, keys, 1)
	require.Equal(t, "anna", keys[0].User)
	require.Equal(t, "roachprod-ssh-keys-"+DefaultProjectID, store.readBucket)
}

func TestSetUserAuthorizedKeysUsesGCS(t *testing.T) {
	keys, issues, err := ParseUserAuthorizedKeys(
		"anna:" + testAuthorizedKeyOne + "\nzoe:" + testAuthorizedKeyTwo + "\n",
	)
	require.NoError(t, err)
	require.Empty(t, issues)
	store := &fakeSSHKeysObjectStore{}
	withSSHKeysObjectStore(t, store, nil)

	oldProvider, hadProvider := vm.Providers[ProviderName]
	vm.Providers[ProviderName] = &Provider{
		infraProject: DefaultProjectID, metadataProject: StagingProjectID,
	}
	t.Cleanup(func() {
		if hadProvider {
			vm.Providers[ProviderName] = oldProvider
		} else {
			delete(vm.Providers, ProviderName)
		}
	})

	require.NoError(t, SetUserAuthorizedKeys(keys))
	require.Equal(t, "roachprod-ssh-keys-"+StagingProjectID, store.writeBucket)
	require.Equal(t, sshKeysObjectName, store.writeObject)
	require.Equal(t, keys.AsStorageFile(), store.written)
	require.True(t, store.closed)
}
