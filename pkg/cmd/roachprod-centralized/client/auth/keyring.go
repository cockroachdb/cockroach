// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package auth

import (
	"encoding/json"
	"os"
	"path/filepath"

	"github.com/99designs/keyring"
	"github.com/cockroachdb/errors"
)

// KeyringStore provides secure storage for authentication tokens using the OS keyring.
type KeyringStore struct {
	ring keyring.Keyring
}

// NewKeyringStore creates a new keyring store for storing authentication tokens.
// It tries multiple backends in order of preference:
// - macOS Keychain
// - Windows Credential Manager
// - Linux Secret Service (GNOME/KDE)
// - Encrypted file backend (fallback)
func NewKeyringStore() (*KeyringStore, error) {
	// Determine the file backend directory
	homeDir, err := os.UserHomeDir()
	if err != nil {
		homeDir = "."
	}
	fileDir := filepath.Join(homeDir, "."+KeyringServiceName)

	ring, err := keyring.Open(keyring.Config{
		ServiceName: KeyringServiceName,
		// Backends will be tried in order until one succeeds.
		AllowedBackends: []keyring.BackendType{
			keyring.KeychainBackend,      // macOS
			keyring.WinCredBackend,       // Windows
			keyring.SecretServiceBackend, // Linux with GNOME/KDE
			keyring.FileBackend,          // Encrypted file fallback
		},
		FileDir:          fileDir,
		FilePasswordFunc: keyring.TerminalPrompt,
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to open keyring")
	}

	return &KeyringStore{ring: ring}, nil
}

// StoreToken stores an authentication token in the keyring.
func (k *KeyringStore) StoreToken(token StoredToken) error {
	data, err := json.Marshal(token)
	if err != nil {
		return errors.Wrap(err, "failed to marshal token")
	}

	err = k.ring.Set(keyring.Item{
		Key:  KeyringTokenKey,
		Data: data,
	})
	if err != nil {
		return errors.Wrap(err, "failed to store token in keyring")
	}

	return nil
}

// GetToken retrieves the stored authentication token from the keyring.
// Returns ErrNoToken if no token is stored.
func (k *KeyringStore) GetToken() (*StoredToken, error) {
	item, err := k.ring.Get(KeyringTokenKey)
	if err != nil {
		if errors.Is(err, keyring.ErrKeyNotFound) {
			return nil, ErrNoToken
		}
		return nil, errors.Wrap(err, "failed to get token from keyring")
	}

	var token StoredToken
	if err := json.Unmarshal(item.Data, &token); err != nil {
		return nil, errors.Wrap(err, "failed to unmarshal token")
	}

	return &token, nil
}

// ClearToken removes the stored authentication token from the keyring.
func (k *KeyringStore) ClearToken() error {
	err := k.ring.Remove(KeyringTokenKey)
	if err != nil {
		// Ignore "key not found" errors - clearing a non-existent token is fine
		if errors.Is(err, keyring.ErrKeyNotFound) {
			return nil
		}
		return errors.Wrap(err, "failed to clear token from keyring")
	}
	return nil
}

// HasToken returns true if a token is stored in the keyring.
func (k *KeyringStore) HasToken() bool {
	_, err := k.ring.Get(KeyringTokenKey)
	return err == nil
}
