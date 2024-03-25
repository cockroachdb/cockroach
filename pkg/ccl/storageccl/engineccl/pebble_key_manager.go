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
	"encoding/hex"
	"fmt"
	"io"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl/engineccl/enginepbccl"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/cockroachdb/pebble/vfs/atomicfs"
	"github.com/gogo/protobuf/proto"
	"github.com/lestrrat-go/jwx/jwa"
	"github.com/lestrrat-go/jwx/jwk"
)

const (
	// The key filename for the StoreKeyManager when using plaintext.
	storeFileNamePlain = "plain"
	// The key id used in KeyInfo for a plaintext "key".
	plainKeyID = "plain"
	// The length of a real key id.
	keyIDLength = 32
	// The filename used for writing the data keys by the DataKeyManager.
	keyRegistryFilename = "COCKROACHDB_DATA_KEYS"
	// The name of the marker used to record the active data keys
	// registry.
	keysRegistryMarkerName = "datakeys"
)

// registryFormat is an enum describing the format of the data keys
// registry file. The enum value is encoded into the data keys registry
// filename. Currently, there's only one value for the enum. Future work
// may convert the data keys registry to a new format (see #70140).
type registryFormat string

const (
	// registryFormatMonolith is the existing format of the data keys
	// registry file used by the DataKeyManager. The format is a single
	// serialized enginepbccl.DataKeysRegistry protocol buffer.
	registryFormatMonolith registryFormat = "monolith"
)

// PebbleKeyManager manages encryption keys. There are two implementations. See encrypted_fs.go for
// high-level context.
type PebbleKeyManager interface {
	// ActiveKey returns the currently active key. If plaintext should be used it can return nil or
	// a key with encryption_type = Plaintext.
	ActiveKey(ctx context.Context) (*enginepbccl.SecretKey, error)

	// GetKey gets the key for the given id. Returns an error if the key was not found.
	GetKey(id string) (*enginepbccl.SecretKey, error)
}

var _ PebbleKeyManager = &StoreKeyManager{}
var _ PebbleKeyManager = &DataKeyManager{}

// Overridden for testing.
var kmTimeNow = time.Now

// StoreKeyManager manages the user-provided keys. Implements PebbleKeyManager.
type StoreKeyManager struct {
	// Initialize the following before calling Load().
	fs                vfs.FS
	activeKeyFilename string
	oldKeyFilename    string

	// Implementation. Both are not nil after a successful call to Load().
	activeKey *enginepbccl.SecretKey
	oldKey    *enginepbccl.SecretKey
}

// Load must be called before calling other functions.
func (m *StoreKeyManager) Load(ctx context.Context) error {
	var err error
	m.activeKey, err = LoadKeyFromFile(m.fs, m.activeKeyFilename)
	if err != nil {
		return err
	}
	m.oldKey, err = LoadKeyFromFile(m.fs, m.oldKeyFilename)
	if err != nil {
		return err
	}
	log.Infof(ctx, "loaded active store key: %s, old store key: %s",
		proto.CompactTextString(m.activeKey.Info), proto.CompactTextString(m.oldKey.Info))
	return nil
}

// ActiveKey implements PebbleKeyManager.ActiveKey.
func (m *StoreKeyManager) ActiveKey(ctx context.Context) (*enginepbccl.SecretKey, error) {
	return m.activeKey, nil
}

// GetKey implements PebbleKeyManager.GetKey.
func (m *StoreKeyManager) GetKey(id string) (*enginepbccl.SecretKey, error) {
	if m.activeKey.Info.KeyId == id {
		return m.activeKey, nil
	}
	if m.oldKey.Info.KeyId == id {
		return m.oldKey, nil
	}
	return nil, fmt.Errorf("store key ID %s was not found", id)
}

// LoadKeyFromFile reads a secret key from the given file.
func LoadKeyFromFile(fs vfs.FS, filename string) (*enginepbccl.SecretKey, error) {
	now := kmTimeNow().Unix()
	key := &enginepbccl.SecretKey{}
	key.Info = &enginepbccl.KeyInfo{}
	if filename == storeFileNamePlain {
		key.Info.EncryptionType = enginepbccl.EncryptionType_Plaintext
		key.Info.KeyId = plainKeyID
		key.Info.CreationTime = now
		key.Info.Source = storeFileNamePlain
		return key, nil
	}

	f, err := fs.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	b, err := io.ReadAll(f)
	if err != nil {
		return nil, err
	}

	// We support two file formats:
	// - Old-style keys are just raw random data with no delimiters; the only
	//   format validity requirement is that the file has the right length.
	// - New-style keys are in JWK format (we support both JWK (single-key)
	//   and JWKS (set of keys)) formats, although we currently require
	//   that JWKS sets contain exactly one key).
	// Since random generation of 48+ bytes will not produce a valid json object,
	// if the file parses as JWK, assume that was the intended format.
	if keySet, jwkErr := jwk.Parse(b); jwkErr == nil {
		jwKey, ok := keySet.Get(0)
		if !ok {
			return nil, fmt.Errorf("JWKS file contains no keys")
		}
		if keySet.Len() != 1 {
			return nil, fmt.Errorf("expected exactly 1 key in JWKS file, found %d", keySet.Len())
		}
		if jwKey.KeyType() != jwa.OctetSeq {
			return nil, fmt.Errorf("expected kty=oct, found %s", jwKey.KeyType())
		}
		key.Info.EncryptionType, err = enginepbccl.EncryptionTypeFromJWKAlgorithm(jwKey.Algorithm())
		if err != nil {
			return nil, err
		}
		key.Info.KeyId = jwKey.KeyID()
		symKey, ok := jwKey.(jwk.SymmetricKey)
		if !ok {
			return nil, fmt.Errorf("error converting jwk.Key to SymmetricKey")
		}
		key.Key = symKey.Octets()
	} else {
		// keyIDLength bytes for the ID, followed by the key.
		keyLength := len(b) - keyIDLength
		switch keyLength {
		case 16:
			key.Info.EncryptionType = enginepbccl.EncryptionType_AES128_CTR
		case 24:
			key.Info.EncryptionType = enginepbccl.EncryptionType_AES192_CTR
		case 32:
			key.Info.EncryptionType = enginepbccl.EncryptionType_AES256_CTR
		default:
			return nil, errors.Wrapf(jwkErr, "could not parse store key. "+
				"Key length %d is not valid for old-style key. Parse error for new-style key", keyLength)
		}
		key.Key = b[keyIDLength:]
		// Hex encoding to make it human readable.
		key.Info.KeyId = hex.EncodeToString(b[:keyIDLength])
	}
	key.Info.CreationTime = now
	key.Info.Source = filename

	return key, nil
}

// DataKeyManager manages data keys. Implements PebbleKeyManager. Key rotation does not begin until
// SetActiveStoreKeyInfo() is called.
type DataKeyManager struct {
	// Initialize the following before calling Load().
	fs             vfs.FS
	dbDir          string
	rotationPeriod int64 // seconds
	readOnly       bool

	// Implementation.

	mu struct {
		syncutil.Mutex
		// Non-nil after Load()
		keyRegistry *enginepbccl.DataKeysRegistry
		// rotationEnabled => non-nil
		activeKey *enginepbccl.SecretKey
		// Transitions to true when SetActiveStoreKeyInfo() is called for the
		// first time.
		rotationEnabled bool
		// marker is an atomic file marker used to denote which of the
		// data keys registry files is the current one. When we rotate
		// files, the marker is atomically moved to the new file. It's
		// guaranteed to be non-nil after Load.
		marker *atomicfs.Marker
		// filename is the filename of the currently active registry.
		filename string
	}
}

func makeRegistryProto() *enginepbccl.DataKeysRegistry {
	return &enginepbccl.DataKeysRegistry{
		StoreKeys: make(map[string]*enginepbccl.KeyInfo),
		DataKeys:  make(map[string]*enginepbccl.SecretKey),
	}
}

// Close releases all of the manager's held resources.
func (m *DataKeyManager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.mu.marker.Close()
}

// Load must be called before calling other methods.
func (m *DataKeyManager) Load(ctx context.Context) error {
	marker, filename, err := atomicfs.LocateMarker(m.fs, m.dbDir, keysRegistryMarkerName)
	if err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	m.mu.marker = marker
	if oserror.IsNotExist(err) {
		// First run.
		m.mu.keyRegistry = makeRegistryProto()
		return nil
	}

	// Load the existing state from the file named by `filename`.
	m.mu.filename = filename
	m.mu.keyRegistry = makeRegistryProto()
	if filename != "" {
		f, err := m.fs.Open(m.fs.PathJoin(m.dbDir, filename))
		if err != nil {
			return err
		}
		defer f.Close()
		b, err := io.ReadAll(f)
		if err != nil {
			return err
		}
		if err = protoutil.Unmarshal(b, m.mu.keyRegistry); err != nil {
			return err
		}
		if err = validateRegistry(m.mu.keyRegistry); err != nil {
			return err
		}
	}
	// Else there is no DataKeysRegistry file yet.

	if m.mu.keyRegistry.ActiveDataKeyId != "" {
		key, found := m.mu.keyRegistry.DataKeys[m.mu.keyRegistry.ActiveDataKeyId]
		if !found {
			// This should have resulted in an error in validateRegistry()
			panic("unexpected inconsistent DataKeysRegistry")
		}
		m.mu.activeKey = key
		log.Infof(ctx, "loaded active data key: %s", m.mu.activeKey.Info.String())
	} else {
		log.Infof(ctx, "no active data key yet")
	}
	return nil
}

// ActiveKey implements PebbleKeyManager.ActiveKey.
//
// TODO(sbhola): do rotation via a background activity instead of in this function so that we don't
// slow down creation of files.
func (m *DataKeyManager) ActiveKey(ctx context.Context) (*enginepbccl.SecretKey, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.mu.rotationEnabled {
		now := kmTimeNow().Unix()
		if now-m.mu.activeKey.Info.CreationTime > m.rotationPeriod {
			keyRegistry := makeRegistryProto()
			proto.Merge(keyRegistry, m.mu.keyRegistry)
			if err := m.rotateDataKeyAndWrite(ctx, keyRegistry); err != nil {
				return nil, err
			}
		}
	}
	return m.mu.activeKey, nil
}

// GetKey implements PebbleKeyManager.GetKey.
func (m *DataKeyManager) GetKey(id string) (*enginepbccl.SecretKey, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key, found := m.mu.keyRegistry.DataKeys[id]
	if !found {
		return nil, fmt.Errorf("key %s is not found", id)
	}
	return key, nil
}

// SetActiveStoreKeyInfo sets the current active store key. Even though there may be a valid
// ActiveStoreKeyId in the DataKeysRegistry loaded from file, key rotation does not start until
// the first call to the following function. Each call to this function will rotate the active
// data key under the following conditions:
// - there is no active data key.
// - the active store key has changed.
//
// This function should not be called for a read only store.
func (m *DataKeyManager) SetActiveStoreKeyInfo(
	ctx context.Context, storeKeyInfo *enginepbccl.KeyInfo,
) error {
	if m.readOnly {
		return errors.New("read only")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	// Enable data key rotation regardless of what case we go into.
	m.mu.rotationEnabled = true
	prevActiveStoreKey, found := m.mu.keyRegistry.StoreKeys[m.mu.keyRegistry.ActiveStoreKeyId]
	if found && prevActiveStoreKey.KeyId == storeKeyInfo.KeyId && m.mu.activeKey != nil {
		// The active store key has not changed and we already have an active data key,
		// so no need to do anything.
		return nil
	}
	// For keys other than plaintext, make sure the user is not reusing inactive keys.
	if storeKeyInfo.EncryptionType != enginepbccl.EncryptionType_Plaintext {
		if _, found := m.mu.keyRegistry.StoreKeys[storeKeyInfo.KeyId]; found {
			return fmt.Errorf("new active store key ID %s already exists as an inactive key -- this"+
				"is really dangerous", storeKeyInfo.KeyId)
		}
	}

	// The keyRegistry proto that will replace the current one.
	keyRegistry := makeRegistryProto()
	proto.Merge(keyRegistry, m.mu.keyRegistry)
	keyRegistry.StoreKeys[storeKeyInfo.KeyId] = storeKeyInfo
	keyRegistry.ActiveStoreKeyId = storeKeyInfo.KeyId
	if storeKeyInfo.EncryptionType == enginepbccl.EncryptionType_Plaintext {
		// Mark all data keys as exposed.
		for _, key := range keyRegistry.DataKeys {
			key.Info.WasExposed = true
		}
	}
	if err := m.rotateDataKeyAndWrite(ctx, keyRegistry); err != nil {
		return err
	}
	m.mu.rotationEnabled = true
	return nil
}

func (m *DataKeyManager) getScrubbedRegistry() *enginepbccl.DataKeysRegistry {
	m.mu.Lock()
	defer m.mu.Unlock()
	r := makeRegistryProto()
	proto.Merge(r, m.mu.keyRegistry)
	for _, v := range r.DataKeys {
		v.Key = nil
	}
	return r
}

func validateRegistry(keyRegistry *enginepbccl.DataKeysRegistry) error {
	if keyRegistry.ActiveStoreKeyId != "" && keyRegistry.StoreKeys[keyRegistry.ActiveStoreKeyId] == nil {
		return fmt.Errorf("active store key %s not found", keyRegistry.ActiveStoreKeyId)
	}
	if keyRegistry.ActiveDataKeyId != "" && keyRegistry.DataKeys[keyRegistry.ActiveDataKeyId] == nil {
		return fmt.Errorf("active data key %s not found", keyRegistry.ActiveDataKeyId)
	}
	return nil
}

// Generates a new data key and adds it to the keyRegistry proto and sets it as the active key.
func generateAndSetNewDataKey(
	ctx context.Context, keyRegistry *enginepbccl.DataKeysRegistry,
) (*enginepbccl.SecretKey, error) {
	activeStoreKey := keyRegistry.StoreKeys[keyRegistry.ActiveStoreKeyId]
	if activeStoreKey == nil {
		panic("expected registry with active store key")
	}
	key := &enginepbccl.SecretKey{}
	key.Info = &enginepbccl.KeyInfo{}
	key.Info.EncryptionType = activeStoreKey.EncryptionType
	key.Info.CreationTime = kmTimeNow().Unix()
	key.Info.Source = "data key manager"
	key.Info.ParentKeyId = activeStoreKey.KeyId

	if activeStoreKey.EncryptionType == enginepbccl.EncryptionType_Plaintext {
		key.Info.KeyId = plainKeyID
		key.Info.WasExposed = true
	} else {
		var keyLength int
		switch activeStoreKey.EncryptionType {
		case enginepbccl.EncryptionType_AES128_CTR:
			keyLength = 16
		case enginepbccl.EncryptionType_AES192_CTR:
			keyLength = 24
		case enginepbccl.EncryptionType_AES256_CTR:
			keyLength = 32
		default:
			return nil, fmt.Errorf("unknown encryption type %d for key ID %s",
				activeStoreKey.EncryptionType, activeStoreKey.KeyId)
		}
		key.Key = make([]byte, keyLength)
		n, err := rand.Read(key.Key)
		if err != nil {
			return nil, err
		}
		if n != keyLength {
			log.Fatalf(ctx, "rand.Read returned no error but fewer bytes %d than promised %d", n, keyLength)
		}
		keyID := make([]byte, keyIDLength)
		if n, err = rand.Read(keyID); err != nil {
			return nil, err
		}
		if n != keyIDLength {
			log.Fatalf(ctx, "rand.Read returned no error but fewer bytes %d than promised %d", n, keyIDLength)
		}
		// Hex encoding to make it human readable.
		key.Info.KeyId = hex.EncodeToString(keyID)
		key.Info.WasExposed = false
	}
	keyRegistry.DataKeys[key.Info.KeyId] = key
	keyRegistry.ActiveDataKeyId = key.Info.KeyId
	return key, nil
}

// REQUIRES: m.mu is held.
func (m *DataKeyManager) rotateDataKeyAndWrite(
	ctx context.Context, keyRegistry *enginepbccl.DataKeysRegistry,
) (err error) {
	defer func() {
		if err != nil {
			log.Infof(ctx, "error while attempting to rotate data key: %s", err)
		} else {
			log.Infof(ctx, "rotated to new active data key: %s", proto.CompactTextString(m.mu.activeKey.Info))
		}
	}()

	var newKey *enginepbccl.SecretKey
	if newKey, err = generateAndSetNewDataKey(ctx, keyRegistry); err != nil {
		return
	}
	if err = validateRegistry(keyRegistry); err != nil {
		return
	}
	bytes, err := protoutil.Marshal(keyRegistry)
	if err != nil {
		return err
	}

	// Write the current registry state to a new file and sync it.
	// The new file's filename incorporates the marker's iteration
	// number to ensure we're not overwriting the existing registry.
	filename := fmt.Sprintf("%s_%06d_%s", keyRegistryFilename, m.mu.marker.NextIter(), registryFormatMonolith)
	f, err := m.fs.Create(m.fs.PathJoin(m.dbDir, filename), fs.EncryptionRegistryWriteCategory)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.Write(bytes); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	// Move the marker to the new file. Once the marker is moved,
	// the new file is active. This call to marker.Move will also sync
	// the directory on behalf of both the marker and the registry
	// itself.
	if err := m.mu.marker.Move(filename); err != nil {
		return err
	}

	prevFilename := m.mu.filename
	m.mu.filename = filename
	m.mu.keyRegistry = keyRegistry
	m.mu.activeKey = newKey

	// Remove the previous data registry file.
	if prevFilename != "" {
		path := m.fs.PathJoin(m.dbDir, prevFilename)
		if err := m.fs.Remove(path); err != nil && !oserror.IsNotExist(err) {
			return err
		}
	}
	return err
}
