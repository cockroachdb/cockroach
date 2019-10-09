package engineccl

import (
	"crypto/rand"
	"fmt"
	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl/engineccl/enginepbccl"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/kr/pretty"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

var kData []byte = []byte("Call me Ishmael. Some years ago—never mind how long precisely—" +
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
	encTypes := []enginepbccl.EncryptionType{enginepbccl.EncryptionType_AES128_CTR,
		enginepbccl.EncryptionType_AES192_CTR, enginepbccl.EncryptionType_AES256_CTR}
	for _, encType := range encTypes {
		key, err := generateKey(encType)
		require.NoError(t, err)
		var counter uint32 = 5
		nonce := make([]byte, kCTRNonceSize)
		_, err = rand.Read(nonce)
		require.NoError(t, err)
		bcs, err := newCTRBlockCipherStream(key, nonce, counter)
		fcs := fileCipherStream{bcs: bcs}

		var data []byte
		data = append(data, kData...)

		for _, offset := range []int64{5, 23, 435, 2000} {
			fcs.Encrypt(offset, data)
			if diff := pretty.Diff(data, kData); diff == nil {
				t.Fatal("encryption was a noop")
			}
			fcs.Decrypt(offset, data)
			if diff := pretty.Diff(data, kData); diff != nil {
				t.Fatalf("%s\n%s", strings.Join(diff, "\n"), data)
			}
		}
	}
}

type TestKeyManager struct {
	keys     map[string]*enginepbccl.SecretKey
	activeId string
}

func (m *TestKeyManager) ActiveKey() (*enginepbccl.SecretKey, error) {
	key, _ := m.GetKey(m.activeId)
	return key, nil
}
func (m *TestKeyManager) GetKey(id string) (*enginepbccl.SecretKey, error) {
	key, found := m.keys[id]
	if !found {
		return nil, fmt.Errorf("")
	}
	return key, nil
}

func TestFileCipherStreamCreator(t *testing.T) {
	km := TestKeyManager{}
	km.activeId = "foo"
	key, err := generateKey(enginepbccl.EncryptionType_AES192_CTR)
	require.NoError(t, err)
	km.keys = make(map[string]*enginepbccl.SecretKey)
	km.keys["foo"] = key
	fcs := &FileCipherStreamCreator{envType: enginepb.EnvType_Data, keyManager: &km}
	nonce := make([]byte, 12)
	encSettings := &enginepbccl.EncryptionSettings{
		EncryptionType: enginepbccl.EncryptionType_AES192_CTR, KeyId: "foo", Nonce: nonce}

	fs1, err := fcs.CreateExisting(encSettings)
	require.NoError(t, err)
	data := append([]byte{}, kData...)
	fs1.Encrypt(5, data)
	encData := append([]byte{}, data...)

	fs2, err := fcs.CreateExisting(encSettings)
	require.NoError(t, err)
	fs2.Decrypt(5, data)
	if diff := pretty.Diff(data, kData); diff != nil {
		t.Fatalf("%s\n%s", strings.Join(diff, "\n"), data)
	}

	encSettings.EncryptionType = enginepbccl.EncryptionType_Plaintext
	fs3, err := fcs.CreateExisting(encSettings)
	require.NoError(t, err)
	fs3.Encrypt(5, data)
	if diff := pretty.Diff(data, kData); diff != nil {
		t.Fatalf("%s\n%s", strings.Join(diff, "\n"), data)
	}
	fs3.Decrypt(5, data)
	if diff := pretty.Diff(data, kData); diff != nil {
		t.Fatalf("%s\n%s", strings.Join(diff, "\n"), data)
	}

	encSettings, fs4, err := fcs.CreateNew()
	require.NoError(t, err)
	fs4.Encrypt(5, data)
	if diff := pretty.Diff(data, kData); diff == nil {
		t.Fatalf("encryption was a noop")
	}
	if diff := pretty.Diff(data, encData); diff == nil {
		t.Fatalf("unexpected equality")
	}
	fs4.Decrypt(5, data)
	if diff := pretty.Diff(data, kData); diff != nil {
		t.Fatalf("%s\n%s", strings.Join(diff, "\n"), data)
	}

	// Make the active key = nil
	km.activeId = "bar"
	encSettings, fs5, err := fcs.CreateNew()
	require.NoError(t, err)
	fs5.Encrypt(5, data)
	if diff := pretty.Diff(data, kData); diff != nil {
		t.Fatalf("%s\n%s", strings.Join(diff, "\n"), data)
	}
}
