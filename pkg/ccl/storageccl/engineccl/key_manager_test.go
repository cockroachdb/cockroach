package engineccl

import (
	"bytes"
	"fmt"
	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl/engineccl/enginepbccl"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
	"io"
	"strconv"
	"strings"
	"testing"
	"time"
)

func checkPlainKey(t *testing.T, key *enginepbccl.SecretKey) {
	require.NotNil(t, key)
	require.Equal(t, enginepbccl.EncryptionType_Plaintext, key.Info.EncryptionType)
	require.Equal(t, "plain", key.Info.Source)
	require.Equal(t, "plain", key.Info.KeyId)
}

func checkKey(t *testing.T, key *enginepbccl.SecretKey, id string, keyBytes []byte,
	source string, encType enginepbccl.EncryptionType) {
	require.NotNil(t, key)
	require.Equal(t, encType, key.Info.EncryptionType)
	require.Equal(t, source, key.Info.Source)
	require.Equal(t, id, key.Info.KeyId)
	require.Equal(t, keyBytes, key.Key)
}

func writeToFile(t *testing.T, fs vfs.FS, filename string, b []byte) {
	f, err := fs.Create(filename)
	require.NoError(t, err)
	breader := bytes.NewReader(b)
	_, err = io.Copy(f, breader)
	require.NoError(t, err)
	err = f.Close()
	require.NoError(t, err)
}

func TestStoreKeyManager(t *testing.T) {
	mem := vfs.NewMem()
	{
		skm := &StoreKeyManager{fs: mem, activeKeyFilename: "plain", oldKeyFilename: "plain"}
		skm.Load()
		key, err := skm.ActiveKey()
		require.NoError(t, err)
		checkPlainKey(t, key)
		key, err = skm.GetKey("plain")
		require.NoError(t, err)
		checkPlainKey(t, key)
		key, err = skm.GetKey("x")
		require.Error(t, err)
	}
	{
		encTypes := []enginepbccl.EncryptionType{enginepbccl.EncryptionType_AES128_CTR,
			enginepbccl.EncryptionType_AES192_CTR, enginepbccl.EncryptionType_AES256_CTR}
		for _, encType := range encTypes {
			var keyLength int
			switch encType {
			case enginepbccl.EncryptionType_AES128_CTR:
				keyLength = 16
			case enginepbccl.EncryptionType_AES192_CTR:
				keyLength = 24
			case enginepbccl.EncryptionType_AES256_CTR:
				keyLength = 32
			}
			var b []byte
			var expectedId []byte
			for i := 0; i < kKeyIdLength; i++ {
				b = append(b, 'a')
				// Hex encoding of "a"
				expectedId = append(expectedId, '6', '1')
			}
			var expectedKey []byte
			for i := 0; i < keyLength; i++ {
				b = append(b, 'b')
				expectedKey = append(expectedKey, 'b')
			}

			writeToFile(t, mem, "file1", b)
			{
				skm := &StoreKeyManager{fs: mem, activeKeyFilename: "file1", oldKeyFilename: "plain"}
				skm.Load()
				key, err := skm.GetKey("plain")
				require.NoError(t, err)
				checkPlainKey(t, key)
				key, err = skm.GetKey(string(expectedId))
				require.NoError(t, err)
				checkKey(t, key, string(expectedId), expectedKey, "file1", encType)
				key, err = skm.ActiveKey()
				require.NoError(t, err)
				checkKey(t, key, string(expectedId), expectedKey, "file1", encType)
			}
		}
	}
}

func setActiveStoreKeyInProto(dkr *enginepbccl.DataKeysRegistry, id string) {
	dkr.StoreKeys[id] = &enginepbccl.KeyInfo{EncryptionType: enginepbccl.EncryptionType_AES128_CTR,
		KeyId: id}
	dkr.ActiveStoreKeyId = id
}

func setActiveDataKeyInProto(dkr *enginepbccl.DataKeysRegistry, id string) {
	dkr.DataKeys[id] = &enginepbccl.SecretKey{
		Info: &enginepbccl.KeyInfo{EncryptionType: enginepbccl.EncryptionType_AES192_CTR, KeyId: id},
		Key:  []byte("some key"),
	}
	dkr.ActiveDataKeyId = id
}

func setActiveStoreKey(dkm *DataKeyManager, id string, kind enginepbccl.EncryptionType) string {
	err := dkm.SetActiveStoreKeyInfo(&enginepbccl.KeyInfo{
		EncryptionType: kind,
		KeyId:          id,
	})
	if err != nil {
		return err.Error()
	}
	return ""
}

func keyPresence(key *enginepbccl.SecretKey) string {
	if key != nil {
		return "present\n"
	}
	return "none\n"
}

func CompareKeys(last, curr *enginepbccl.SecretKey) string {
	if (last == nil && curr == nil) || (last != nil && curr == nil) || (last == nil && curr != nil) ||
		(last.Info.KeyId == curr.Info.KeyId) {
		return "same\n"
	}
	return "different\n"
}

func TestDataKeyManager(t *testing.T) {
	mem := vfs.NewMem()

	var dkm *DataKeyManager
	var keyMap map[string]*enginepbccl.SecretKey
	var lastActiveDataKey *enginepbccl.SecretKey

	var unixTime int64
	dkmTimeNow = func() time.Time {
		return time.Unix(unixTime, 0)
	}

	datadriven.RunTest(t, "testdata/data_key_manager",
		func(d *datadriven.TestData) string {
			switch d.Cmd {
			case "init":
				data := strings.Split(d.Input, "\n")
				if len(data) < 2 {
					return "insufficient arguments to init"
				}
				data[0] = strings.TrimSpace(data[0])
				data[1] = strings.TrimSpace(data[1])
				period, err := strconv.Atoi(data[1])
				if err != nil {
					return err.Error()
				}
				keyMap = make(map[string]*enginepbccl.SecretKey)
				lastActiveDataKey = nil
				mem.MkdirAll(data[0], 0755)
				dkm = &DataKeyManager{fs: mem, dbDir: data[0], rotationPeriod: int64(period)}
				dkr := &enginepbccl.DataKeysRegistry{
					StoreKeys: make(map[string]*enginepbccl.KeyInfo),
					DataKeys:  make(map[string]*enginepbccl.SecretKey),
				}
				for i := 2; i < len(data); i++ {
					keyInfo := strings.Split(data[i], " ")
					if len(keyInfo) != 2 {
						return "insufficient parameters: " + data[i]
					}
					keyInfo[0] = strings.TrimSpace(keyInfo[0])
					keyInfo[1] = strings.TrimSpace(keyInfo[1])
					switch keyInfo[0] {
					case "active-store-key":
						setActiveStoreKeyInProto(dkr, keyInfo[1])
					case "active-data-key":
						setActiveDataKeyInProto(dkr, keyInfo[1])
					default:
						return fmt.Sprintf("unknown command: %s", keyInfo[1])
					}
				}
				if len(data) > 2 {
					bytes, err := dkr.Marshal()
					if err != nil {
						return err.Error()
					}
					writeToFile(t, mem, mem.PathJoin(data[0], kKeyRegistryFilename), bytes)
				}
				return ""
			case "load":
				if err := dkm.Load(); err != nil {
					return err.Error()
				}
				return ""
			case "set-active-store-key":
				var id string
				d.ScanArgs(t, "id", &id)
				return setActiveStoreKey(dkm, id, enginepbccl.EncryptionType_AES128_CTR)
			case "set-active-store-key-plain":
				var id string
				d.ScanArgs(t, "id", &id)
				return setActiveStoreKey(dkm, d.CmdArgs[0].Vals[0], enginepbccl.EncryptionType_Plaintext)
			case "check-exposed":
				var val bool
				d.ScanArgs(t, "val", &val)
				for _, key := range dkm.keyRegistry.DataKeys {
					if key.Info.WasExposed != val {
						return fmt.Sprintf("key: %s, WasExposed: actual: %t, expected: %t",
							key.Info.KeyId, key.Info.WasExposed, val)
					}
				}
				return ""
			case "get-active-data-key":
				key, err := dkm.ActiveKey()
				if err != nil {
					return err.Error()
				}
				lastActiveDataKey = key
				return keyPresence(key)
			case "get-active-store-key":
				id := dkm.keyRegistry.ActiveStoreKeyId
				if id == "" {
					return "none\n"
				}
				return id + "\n"
			case "get-store-key":
				var id string
				d.ScanArgs(t, "id", &id)
				if dkm.keyRegistry.StoreKeys != nil && dkm.keyRegistry.StoreKeys[id] != nil {
					return "present\n"
				}
				return "absent\n"
			case "record-active-data-key":
				key, err := dkm.ActiveKey()
				if err != nil {
					return err.Error()
				}
				if key != nil {
					keyMap[key.Info.KeyId] = key
				}
				return ""
			case "compare-active-data-key":
				key, err := dkm.ActiveKey()
				if err != nil {
					return err.Error()
				}
				rv := CompareKeys(lastActiveDataKey, key)
				lastActiveDataKey = key
				return rv
			case "check-all-recorded-data-keys":
				actual := fmt.Sprint(dkm.keyRegistry.DataKeys)
				expected := fmt.Sprint(keyMap)
				require.Equal(t, expected, actual)
				return ""
			case "wait":
				data := strings.Split(d.Input, "\n")
				if len(data) != 1 {
					return "incorrect arguments to wait"
				}
				interval, err := strconv.Atoi(strings.TrimSpace(data[0]))
				if err != nil {
					return err.Error()
				}
				unixTime += int64(interval)
				return ""
			default:
				return fmt.Sprintf("unknown command: %s\n", d.Cmd)
			}
			return "should not reach here"
		})
}
