// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package main

import (
	"context"
	"crypto/aes"
	"encoding/binary"
	"encoding/hex"
	"flag"
	"fmt"
	"io/ioutil"
	"path/filepath"

	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl/engineccl/enginepbccl"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

const fileRegistryPath = "COCKROACHDB_REGISTRY"
const keyRegistryPath = "COCKROACHDB_DATA_KEYS"
const currentPath = "CURRENT"
const optionsPathGlob = "OPTIONS-*"

var dbDir = flag.String("db-dir", "", "path to the db directory")
var storeKeyPath = flag.String("store-key", "", "path to the active store key")

type fileEntry struct {
	envType  enginepb.EnvType
	settings enginepbccl.EncryptionSettings
}

func (f fileEntry) String() string {
	ret := fmt.Sprintf(" env type: %d, %s\n",
		f.envType, f.settings.EncryptionType)
	if f.settings.EncryptionType != enginepbccl.EncryptionType_Plaintext {
		ret += fmt.Sprintf("  keyID: %s\n  nonce: % x\n  counter: %d\n",
			f.settings.KeyId,
			f.settings.Nonce,
			f.settings.Counter)
	}
	return ret
}

type keyEntry struct {
	encryptionType enginepbccl.EncryptionType
	rawKey         []byte
}

func (k keyEntry) String() string {
	return fmt.Sprintf("%s len: %d", k.encryptionType, len(k.rawKey))
}

var fileRegistry = map[string]fileEntry{}
var keyRegistry = map[string]keyEntry{}

func loadFileRegistry() {
	data, err := ioutil.ReadFile(filepath.Join(*dbDir, fileRegistryPath))
	if err != nil {
		log.Fatalf(context.Background(), "could not read %s: %v", fileRegistryPath, err)
	}

	var reg enginepb.FileRegistry
	if err := protoutil.Unmarshal(data, &reg); err != nil {
		log.Fatalf(context.Background(), "could not unmarshal %s: %v", fileRegistryPath, err)
	}

	log.Infof(context.Background(), "file registry version: %s", reg.Version)
	log.Infof(context.Background(), "file registry contains %d entries", len(reg.Files))
	for name, entry := range reg.Files {
		var encSettings enginepbccl.EncryptionSettings
		settings := entry.EncryptionSettings
		if err := protoutil.Unmarshal(settings, &encSettings); err != nil {
			log.Fatalf(context.Background(), "could not unmarshal encryption setting for %s: %v", name, err)
		}

		fileRegistry[name] = fileEntry{entry.EnvType, encSettings}

		log.Infof(context.Background(), "  %-30s level: %-8s type: %-12s keyID: %s", name, entry.EnvType, encSettings.EncryptionType, encSettings.KeyId[:8])
	}
}

func loadStoreKey() {
	if len(*storeKeyPath) == 0 || *storeKeyPath == "plain" {
		log.Infof(context.Background(), "no store key specified")
		return
	}

	data, err := ioutil.ReadFile(*storeKeyPath)
	if err != nil {
		log.Fatalf(context.Background(), "could not read %s: %v", *storeKeyPath, err)
	}

	var k keyEntry
	switch len(data) {
	case 48:
		k.encryptionType = enginepbccl.EncryptionType_AES128_CTR
	case 56:
		k.encryptionType = enginepbccl.EncryptionType_AES192_CTR
	case 64:
		k.encryptionType = enginepbccl.EncryptionType_AES256_CTR
	default:
		log.Fatalf(context.Background(), "wrong key length %d, want 32 bytes + AES length", len(data))
	}

	// Hexadecimal representation of the first 32 bytes.
	id := hex.EncodeToString(data[0:32])
	// Raw key is the rest.
	k.rawKey = data[32:]

	keyRegistry[id] = k

	log.Infof(context.Background(), "store key: %s", k)
}

func loadKeyRegistry() {
	data, err := readFile(keyRegistryPath)
	if err != nil {
		log.Fatalf(context.Background(), "could not read %s: %v", keyRegistryPath, err)
	}

	var reg enginepbccl.DataKeysRegistry
	if err := protoutil.Unmarshal(data, &reg); err != nil {
		log.Fatalf(context.Background(), "could not unmarshal %s: %v", keyRegistryPath, err)
	}

	log.Infof(context.Background(), "key registry contains %d store keys(s) and %d data key(s)",
		len(reg.StoreKeys), len(reg.DataKeys))
	for _, e := range reg.StoreKeys {
		log.Infof(context.Background(), "  store key: type: %-12s %v", e.EncryptionType, e)
	}
	for _, e := range reg.DataKeys {
		log.Infof(context.Background(), "  data  key: type: %-12s %v", e.Info.EncryptionType, e.Info)
	}
	for k, e := range reg.DataKeys {
		keyRegistry[k] = keyEntry{e.Info.EncryptionType, e.Key}
	}
}

func loadCurrent() {
	data, err := readFile(currentPath)
	if err != nil {
		log.Fatalf(context.Background(), "could not read %s: %v", currentPath, err)
	}

	log.Infof(context.Background(), "current: %s", string(data))
}

func loadOptions() {
	absGlob := filepath.Join(*dbDir, optionsPathGlob)
	paths, err := filepath.Glob(absGlob)
	if err != nil {
		log.Fatalf(context.Background(), "problem finding files matching %s: %v", absGlob, err)
	}

	for _, f := range paths {
		fname := filepath.Base(f)
		data, err := readFile(fname)
		if err != nil {
			log.Fatalf(context.Background(), "could not read %s: %v", fname, err)
		}
		log.Infof(context.Background(), "options file: %s starts with: %s", fname, string(data[:100]))
	}
}

func readFile(filename string) ([]byte, error) {
	if len(filename) == 0 {
		return nil, errors.Errorf("filename is empty")
	}

	absPath := filename
	if filename[0] != '/' {
		absPath = filepath.Join(*dbDir, filename)
	}

	data, err := ioutil.ReadFile(absPath)
	if err != nil {
		return nil, errors.Errorf("could not read %s: %v", absPath, err)
	}

	reg, ok := fileRegistry[filename]
	if !ok || reg.settings.EncryptionType == enginepbccl.EncryptionType_Plaintext {
		// Plaintext: do nothing.
		log.Infof(context.Background(), "reading plaintext %s", absPath)
		return data, nil
	}

	// Encrypted: find the key.
	key, ok := keyRegistry[reg.settings.KeyId]
	if !ok {
		return nil, errors.Errorf("could not find key %s for file %s", reg.settings.KeyId, absPath)
	}
	log.Infof(context.Background(), "decrypting %s with %s key %s...", filename, reg.settings.EncryptionType, reg.settings.KeyId[:8])

	cipher, err := aes.NewCipher(key.rawKey)
	if err != nil {
		return nil, errors.Errorf("could not build AES cipher for file %s: %v", absPath, err)
	}

	size := len(data)
	counter := reg.settings.Counter
	nonce := reg.settings.Nonce
	if len(nonce) != 12 {
		log.Fatalf(context.Background(), "nonce has wrong length: %d, expected 12", len(nonce))
	}

	iv := make([]byte, aes.BlockSize)
	for offset := 0; offset < size; offset += aes.BlockSize {
		// Put nonce at beginning of IV.
		copy(iv, nonce)
		// Write counter to end of IV in network byte order.
		binary.BigEndian.PutUint32(iv[12:], counter)
		// Increment counter for next block.
		counter++

		// Encrypt IV (AES CTR mode is always encrypt).
		cipher.Encrypt(iv, iv)

		// XOR data with decrypted IV. We may have a partial block at the end of 'data'.
		for i := 0; i < aes.BlockSize; i++ {
			pos := offset + i
			if pos >= size {
				// Partial block.
				break
			}
			data[pos] = data[pos] ^ iv[i]
		}
	}

	return data, nil
}

func main() {
	flag.Parse()
	loadStoreKey()
	loadFileRegistry()
	loadKeyRegistry()
	loadCurrent()
	loadOptions()
}
