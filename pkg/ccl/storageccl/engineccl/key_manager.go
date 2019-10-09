package engineccl

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl/engineccl/enginepbccl"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/gogo/protobuf/proto"
	"io/ioutil"
	"os"
	"sync"
	"time"
)

const (
	// The key filename for the StoreKeyManager when using plaintext.
	kStoreFileNamePlain = "plain"
	// The key id used in KeyInfo for a plaintext "key".
	kPlainKeyId = "plain"
	// The length of a real key id.
	kKeyIdLength = 32
	// The filename used for writing the data keys by the DataKeyManager.
	kKeyRegistryFilename = "COCKROACHDB_DATA_KEYS"
)

// Manages encryption keys. There are two implementations. See encrypted_fs.go for high-level
// context.
type KeyManager interface {
	// Returns the currently active key. If plaintext should be used it can return nil or a key
	// with encryption_type = Plaintext.
	ActiveKey() (*enginepbccl.SecretKey, error)

	// Gets the key for the given id. Returns an error if the key was not found.
	GetKey(id string) (*enginepbccl.SecretKey, error)
}

// Manages the user-provided keys. Implements KeyManager.
type StoreKeyManager struct {
	// Initialize the following before calling Load().
	fs                vfs.FS
	activeKeyFilename string
	oldKeyFilename    string

	// Implementation. Both are not nil after a successful call to Load().
	activeKey *enginepbccl.SecretKey
	oldKey    *enginepbccl.SecretKey
}

func (m *StoreKeyManager) Load() error {
	var err error
	m.activeKey, err = loadKeyFromFile(m.fs, m.activeKeyFilename)
	if err != nil {
		return err
	}
	m.oldKey, err = loadKeyFromFile(m.fs, m.oldKeyFilename)
	if err != nil {
		return err
	}
	return nil
}

func (m *StoreKeyManager) ActiveKey() (*enginepbccl.SecretKey, error) {
	return m.activeKey, nil
}

func (m *StoreKeyManager) GetKey(id string) (*enginepbccl.SecretKey, error) {
	if m.activeKey.Info.KeyId == id {
		return m.activeKey, nil
	}
	if m.oldKey.Info.KeyId == id {
		return m.oldKey, nil
	}
	return nil, fmt.Errorf("store key with id: %s was not found", id)
}

func loadKeyFromFile(fs vfs.FS, filename string) (*enginepbccl.SecretKey, error) {
	now := time.Now().Unix()
	key := &enginepbccl.SecretKey{}
	if filename == kStoreFileNamePlain {
		key.Info.EncryptionType = enginepbccl.EncryptionType_Plaintext
		key.Info.KeyId = kPlainKeyId
		key.Info.CreationTime = now
		key.Info.Source = kStoreFileNamePlain
		return key, nil
	}

	f, err := fs.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	b, err := ioutil.ReadAll(f)
	if err != nil {
		return nil, err
	}
	// kKeyIdLength bytes for the ID, followed by the key.
	keyLength := len(b) - kKeyIdLength
	switch keyLength {
	case 16:
		key.Info.EncryptionType = enginepbccl.EncryptionType_AES128_CTR
	case 24:
		key.Info.EncryptionType = enginepbccl.EncryptionType_AES192_CTR
	case 32:
		key.Info.EncryptionType = enginepbccl.EncryptionType_AES192_CTR
	default:
		return nil, fmt.Errorf("store key of unsupported length: %d", keyLength)
	}
	key.Key = b[kKeyIdLength:]
	// hex encoding to make it human readable.
	key.Info.KeyId = hex.EncodeToString(b[:kKeyIdLength])
	key.Info.CreationTime = now
	key.Info.Source = filename

	return key, nil
}

// Manages data keys. Implements KeyManager.
type DataKeyManager struct {
	// Initialize the following before calling Load().
	fs             vfs.FS
	dbDir          string
	rotationPeriod int64 // seconds

	// Implementation.

	// Initialized in Load()
	registryFilename string

	mu sync.Mutex

	// Non-nil after Load()
	keyRegistry *enginepbccl.DataKeysRegistry // guarded by mu
	// rotationEnabled => non-nil
	activeKey *enginepbccl.SecretKey // guarded by mu
	// Transitions to true when SetActiveStoreKeyInfo() is called for the
	// first time.
	rotationEnabled bool // guarded by mu
}

func (m *DataKeyManager) Load() error {
	m.registryFilename = m.fs.PathJoin(m.dbDir, kKeyRegistryFilename)
	_, err := m.fs.Stat(m.registryFilename)
	if err == os.ErrNotExist {
		// TODO: can we rely on ErrNotExist being the only not-existence error?
		// First run.
		m.keyRegistry = &enginepbccl.DataKeysRegistry{}
		return nil
	} else if err != nil {
		return err
	}

	f, err := m.fs.Open(m.registryFilename)
	if err != nil {
		return err
	}
	defer f.Close()
	b, err := ioutil.ReadAll(f)
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.keyRegistry = &enginepbccl.DataKeysRegistry{}
	if err = m.keyRegistry.Unmarshal(b); err != nil {
		return err
	}
	if err = validateRegistry(m.keyRegistry); err != nil {
		return err
	}
	if m.keyRegistry.ActiveDataKeyId != "" {
		key, found := m.keyRegistry.DataKeys[m.keyRegistry.ActiveDataKeyId]
		if !found {
			// This should have resulted in an error in validateRegistry()
			panic("unexpected inconsistent DataKeysRegistry")
		}
		m.activeKey = key
		fmt.Printf("loaded active data key: %s", m.activeKey.Info.String())
	} else {
		fmt.Printf("no active data key yet")
	}
	return nil
}

func (m *DataKeyManager) ActiveKey() (*enginepbccl.SecretKey, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.rotationEnabled {
		now := time.Now().Unix()
		if now-m.activeKey.Info.CreationTime > m.rotationPeriod {
			keyRegistry := &enginepbccl.DataKeysRegistry{}
			proto.Merge(keyRegistry, m.keyRegistry)
			if err := m.rotateDataKeyAndWrite(keyRegistry); err != nil {
				return nil, err
			}
		}
	}
	return m.activeKey, nil
}

func (m *DataKeyManager) GetKey(id string) (*enginepbccl.SecretKey, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.keyRegistry == nil {
		return nil, fmt.Errorf("no keys in KeyRegistry")
	}
	key, found := m.keyRegistry.DataKeys[id]
	if !found {
		return nil, fmt.Errorf("key %s is not found", id)
	}
	return key, nil
}

// Even though there may be a valid active_store_key_id in the DataKeysRegistry loaded from
// file, key rotation does not start until the first call to the following function. Each
// call to this function will rotate the active data key under the following conditions:
// - there is no active data key.
// - the active store key has changed.
//
// This function should not be called for a read only store.
func (m *DataKeyManager) SetActiveStoreKeyInfo(storeKeyInfo *enginepbccl.KeyInfo) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	prevActiveStorekey, found := m.keyRegistry.StoreKeys[m.keyRegistry.ActiveStoreKeyId]
	if found && prevActiveStorekey.KeyId == storeKeyInfo.KeyId && m.activeKey != nil {
		// The active store key has not changed and we already have an active data key,
		// so no need to do anything.
	}
	// For keys other than plaintext, make sure the user is not reusing inactive keys.
	if storeKeyInfo.EncryptionType != enginepbccl.EncryptionType_Plaintext {
		if _, found := m.keyRegistry.StoreKeys[storeKeyInfo.KeyId]; found {
			return fmt.Errorf("new active store key ID %s already exists as an inactive key",
				storeKeyInfo.KeyId)
		}
	}

	// The keyRegistry proto that will replace the current one.
	keyRegistry := &enginepbccl.DataKeysRegistry{}
	proto.Merge(keyRegistry, m.keyRegistry)
	keyRegistry.StoreKeys[storeKeyInfo.KeyId] = storeKeyInfo
	keyRegistry.ActiveStoreKeyId = storeKeyInfo.KeyId
	if storeKeyInfo.EncryptionType == enginepbccl.EncryptionType_Plaintext {
		// Mark all data keys as exposed.
		for _, key := range keyRegistry.DataKeys {
			key.Info.WasExposed = true
		}
	}
	if err := m.rotateDataKeyAndWrite(keyRegistry); err != nil {
		return err
	}
	m.rotationEnabled = true
	return nil
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
func generateAndSetNewDateKey(
	keyRegistry *enginepbccl.DataKeysRegistry) (*enginepbccl.SecretKey, error) {
	activeStoreKey := keyRegistry.StoreKeys[keyRegistry.ActiveStoreKeyId]
	if activeStoreKey == nil {
		panic("expected registry with active store key")
	}
	key := &enginepbccl.SecretKey{}
	key.Info.EncryptionType = activeStoreKey.EncryptionType
	key.Info.CreationTime = time.Now().Unix()
	key.Info.Source = "data key manager"
	key.Info.ParentKeyId = activeStoreKey.KeyId

	if activeStoreKey.EncryptionType == enginepbccl.EncryptionType_Plaintext {
		key.Info.KeyId = kPlainKeyId
		key.Info.WasExposed = true
		return key, nil
	}

	var keyLength int
	switch activeStoreKey.EncryptionType {
	case enginepbccl.EncryptionType_AES128_CTR:
		keyLength = 16
	case enginepbccl.EncryptionType_AES192_CTR:
		keyLength = 24
	case enginepbccl.EncryptionType_AES256_CTR:
		keyLength = 32
	default:
		return nil, fmt.Errorf("unknown encryption type %d for key ID %s", activeStoreKey.EncryptionType, activeStoreKey.KeyId)
	}
	key.Key = make([]byte, keyLength)
	_, err := rand.Read(key.Key)
	if err != nil {
		return nil, err
	}
	keyId := make([]byte, kKeyIdLength)
	if _, err = rand.Read(keyId); err != nil {
		return nil, err
	}
	// Hex encoding to make it human readable.
	key.Info.KeyId = hex.EncodeToString(keyId)
	key.Info.WasExposed = false
	keyRegistry.DataKeys[key.Info.KeyId] = key
	keyRegistry.ActiveDataKeyId = key.Info.KeyId
	return key, nil
}

func (m *DataKeyManager) rotateDataKeyAndWrite(keyRegistry *enginepbccl.DataKeysRegistry) error {
	var newKey *enginepbccl.SecretKey
	var err error
	if newKey, err = generateAndSetNewDateKey(keyRegistry); err != nil {
		return err
	}
	if err := validateRegistry(keyRegistry); err != nil {
		return err
	}
	bytes, err := keyRegistry.Marshal()
	if err != nil {
		return err
	}
	if err = engine.SafeWriteToFile(m.fs, m.registryFilename, bytes); err != nil {
		return err
	}
	m.activeKey = newKey
	return nil
}
