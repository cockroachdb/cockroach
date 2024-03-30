// Copyright 2024 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package cliccl

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"

	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl/engineccl/enginepbccl"
	"github.com/cockroachdb/cockroach/pkg/cli"
	"github.com/cockroachdb/errors"
	"github.com/lestrrat-go/jwx/jwk"
	"github.com/spf13/cobra"
)

var aesSizeFlag int
var overwriteKeyFlag bool
var keyVersionFlag int

func genEncryptionKey(
	encryptionKeyPath string, aesSize int, overwriteKey bool, keyVersion int,
) error {
	// Check encryptionKeySize is suitable for the encryption algorithm.
	if aesSize != 128 && aesSize != 192 && aesSize != 256 {
		return fmt.Errorf("store key size should be 128, 192, or 256 bits, got %d", aesSize)
	}

	keyID := make([]byte, 32)
	if _, err := rand.Read(keyID); err != nil {
		return fmt.Errorf("failed to create random key ID")
	}
	key := make([]byte, aesSize/8)
	if _, err := rand.Read(key); err != nil {
		return fmt.Errorf("failed to create key with size %d bytes", aesSize/8)
	}

	var b []byte
	switch keyVersion {
	case 1:
		b = append(b, keyID...)
		b = append(b, key...)
	case 2:
		var et enginepbccl.EncryptionType
		switch aesSize {
		case 128:
			et = enginepbccl.EncryptionType_AES_128_CTR_V2
		case 192:
			et = enginepbccl.EncryptionType_AES_192_CTR_V2
		case 256:
			et = enginepbccl.EncryptionType_AES_256_CTR_V2
		default:
			// Redundant since we checked this at the start of the function too.
			return fmt.Errorf("store key size should be 128, 192, or 256 bits, got %d", aesSize)
		}

		symKey := jwk.NewSymmetricKey()
		if err := symKey.FromRaw(key); err != nil {
			return errors.Wrap(err, "error setting key bytes")
		}
		if err := symKey.Set(jwk.KeyIDKey, hex.EncodeToString(keyID)); err != nil {
			return errors.Wrap(err, "error setting key id")
		}
		alg, err := et.JWKAlgorithm()
		if err != nil {
			return err
		}
		if err := symKey.Set(jwk.AlgorithmKey, alg); err != nil {
			return errors.Wrap(err, "error setting algorithm")
		}

		keySet := jwk.NewSet()
		keySet.Add(symKey)

		b, err = json.Marshal(keySet)
		if err != nil {
			return errors.Wrap(err, "error writing key to json: %s")
		}
	default:
		return fmt.Errorf("unsupported version %d", keyVersion)
	}

	// Write key to the file with owner read/write permission.
	openMode := os.O_WRONLY | os.O_CREATE | os.O_TRUNC
	if !overwriteKey {
		openMode |= os.O_EXCL
	}

	f, err := os.OpenFile(encryptionKeyPath, openMode, 0600)
	if err != nil {
		return err
	}
	_, err = f.Write(b)
	if err1 := f.Close(); err == nil {
		err = err1
	}
	return err
}

// genEncryptionKeyCmd is a command to generate a store key for Encryption At
// Rest.
var genEncryptionKeyCmd = &cobra.Command{
	Use:   "encryption-key <key-file>",
	Short: "generate store key for encryption at rest",
	Long: `Generate store key for encryption at rest.

Generates a key suitable for use as a store key for Encryption At Rest.
The resulting key file will be 32 bytes (random key ID) + key_size in bytes.
`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		encryptionKeyPath := args[0]

		err := genEncryptionKey(encryptionKeyPath, aesSizeFlag, overwriteKeyFlag, keyVersionFlag)

		if err != nil {
			return err
		}

		fmt.Printf("successfully created AES-%d key: %s\n", aesSizeFlag, encryptionKeyPath)
		return nil
	},
}

func init() {
	cli.GenCmd.AddCommand(genEncryptionKeyCmd)

	genEncryptionKeyCmd.PersistentFlags().IntVarP(&aesSizeFlag, "size", "s", 128,
		"AES key size for encryption at rest (one of: 128, 192, 256)")
	genEncryptionKeyCmd.PersistentFlags().BoolVar(&overwriteKeyFlag, "overwrite", false,
		"Overwrite key if it exists")
	genEncryptionKeyCmd.PersistentFlags().IntVar(&keyVersionFlag, "version", 1,
		"Encryption format version (1 or 2)")
}
