// Copyright 2019 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package engineccl

import (
	"context"
	"crypto/rand"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl/engineccl/enginepbccl"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/kr/pretty"
	"github.com/stretchr/testify/require"
)

var testData = []byte("Call me Ishmael. Some years ago—never mind how long precisely—" +
	"having little or no money in my purse, and nothing particular to interest me " +
	"on shore, I thought I would sail about a little and see the watery part of the world.")

func generateKey(encType enginepbccl.EncryptionType) (*enginepbccl.SecretKey, error) {
	key := &enginepbccl.SecretKey{}
	key.Info = &enginepbccl.KeyInfo{}
	key.Info.EncryptionType = encType
	var keyLength int
	switch encType {
	case enginepbccl.EncryptionType_AES128_CTR:
		keyLength = 16
	case enginepbccl.EncryptionType_AES192_CTR:
		keyLength = 24
	case enginepbccl.EncryptionType_AES256_CTR:
		keyLength = 32
	}
	key.Key = make([]byte, keyLength)
	_, err := rand.Read(key.Key)
	return key, err
}

func TestFileCipherStream(t *testing.T) {
	defer leaktest.AfterTest(t)()

	encTypes := []enginepbccl.EncryptionType{enginepbccl.EncryptionType_AES128_CTR,
		enginepbccl.EncryptionType_AES192_CTR, enginepbccl.EncryptionType_AES256_CTR}
	for _, encType := range encTypes {
		key, err := generateKey(encType)
		require.NoError(t, err)
		var counter uint32 = 5
		nonce := make([]byte, ctrNonceSize)
		_, err = rand.Read(nonce)
		require.NoError(t, err)
		bcs, err := newCTRBlockCipherStream(key, nonce, counter)
		require.NoError(t, err)
		fcs := fileCipherStream{bcs: bcs}

		var data []byte
		data = append(data, testData...)

		// Using some arbitrary file offsets, and for each of these offsets cycle through the
		// full block size so that we have tested all partial blocks at the beginning and end
		// of a sequence.
		for _, fOffset := range []int64{5, 23, 435, 2000} {
			for i := 0; i < ctrBlockSize; i++ {
				offset := fOffset + int64(i)
				fcs.Encrypt(offset, data)
				if diff := pretty.Diff(data, testData); diff == nil {
					t.Fatal("encryption was a noop")
				}
				fcs.Decrypt(offset, data)
				if diff := pretty.Diff(data, testData); diff != nil {
					t.Fatalf("%s\n%s", strings.Join(diff, "\n"), data)
				}
			}
		}
	}
}

type testKeyManager struct {
	keys     map[string]*enginepbccl.SecretKey
	activeID string
}

func (m *testKeyManager) ActiveKey(ctx context.Context) (*enginepbccl.SecretKey, error) {
	key, _ := m.GetKey(m.activeID)
	return key, nil
}
func (m *testKeyManager) GetKey(id string) (*enginepbccl.SecretKey, error) {
	key, found := m.keys[id]
	if !found {
		return nil, fmt.Errorf("")
	}
	return key, nil
}

func TestFileCipherStreamCreator(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Key manager with a "foo" active key.
	km := testKeyManager{}
	km.activeID = "foo"
	key, err := generateKey(enginepbccl.EncryptionType_AES192_CTR)
	key.Info.KeyId = "foo"
	require.NoError(t, err)
	km.keys = make(map[string]*enginepbccl.SecretKey)
	km.keys["foo"] = key
	fcs := &FileCipherStreamCreator{envType: enginepb.EnvType_Data, keyManager: &km}

	// Existing stream that uses "foo" key.
	nonce := make([]byte, 12)
	encSettings := &enginepbccl.EncryptionSettings{
		EncryptionType: enginepbccl.EncryptionType_AES192_CTR, KeyId: "foo", Nonce: nonce}
	fs1, err := fcs.CreateExisting(encSettings)
	require.NoError(t, err)
	data := append([]byte{}, testData...)
	fs1.Encrypt(5, data)
	encData := append([]byte{}, data...) // remember the encrypted data.

	// Create another stream that uses "foo" key with the same nonce and counter (i.e., same file)
	// and decrypt and compare.
	fs2, err := fcs.CreateExisting(encSettings)
	require.NoError(t, err)
	fs2.Decrypt(5, data)
	if diff := pretty.Diff(data, testData); diff != nil {
		t.Fatalf("%s\n%s", strings.Join(diff, "\n"), data)
	}

	// Encryption/decryption is noop.
	encSettings.EncryptionType = enginepbccl.EncryptionType_Plaintext
	fs3, err := fcs.CreateExisting(encSettings)
	require.NoError(t, err)
	fs3.Encrypt(5, data)
	if diff := pretty.Diff(data, testData); diff != nil {
		t.Fatalf("%s\n%s", strings.Join(diff, "\n"), data)
	}
	fs3.Decrypt(5, data)
	if diff := pretty.Diff(data, testData); diff != nil {
		t.Fatalf("%s\n%s", strings.Join(diff, "\n"), data)
	}

	// Create a new stream that uses the "foo" key. A different IV and nonce should be chosen so the
	// encrypted state will not be the same as the previous stream.
	encSettings, fs4, err := fcs.CreateNew(context.Background())
	require.Equal(t, "foo", encSettings.KeyId)
	require.Equal(t, enginepbccl.EncryptionType_AES192_CTR, encSettings.EncryptionType)
	require.NoError(t, err)
	fs4.Encrypt(5, data)
	if diff := pretty.Diff(data, testData); diff == nil {
		t.Fatalf("encryption was a noop")
	}
	if diff := pretty.Diff(data, encData); diff == nil {
		t.Fatalf("unexpected equality")
	}
	fs4.Decrypt(5, data)
	if diff := pretty.Diff(data, testData); diff != nil {
		t.Fatalf("%s\n%s", strings.Join(diff, "\n"), data)
	}

	// Make the active key = nil, so encryption/decryption is a noop.
	km.activeID = "bar"
	encSettings, fs5, err := fcs.CreateNew(context.Background())
	require.Equal(t, "", encSettings.KeyId)
	require.Equal(t, enginepbccl.EncryptionType_Plaintext, encSettings.EncryptionType)
	require.NoError(t, err)
	fs5.Encrypt(5, data)
	if diff := pretty.Diff(data, testData); diff != nil {
		t.Fatalf("%s\n%s", strings.Join(diff, "\n"), data)
	}
}
