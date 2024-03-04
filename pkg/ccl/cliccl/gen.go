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
	"fmt"
	"io"
	"os"

	"github.com/cockroachdb/cockroach/pkg/cli"
	"github.com/spf13/cobra"
)

var aesSize int
var overwriteKey bool

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

		// Check encryptionKeySize is suitable for the encryption algorithm.
		if aesSize != 128 && aesSize != 192 && aesSize != 256 {
			return fmt.Errorf("store key size should be 128, 192, or 256 bits, got %d", aesSize)
		}

		// 32 bytes are reserved for key ID.
		kSize := aesSize/8 + 32
		b := make([]byte, kSize)
		if _, err := rand.Read(b); err != nil {
			return fmt.Errorf("failed to create key with size %d bytes", kSize)
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
		n, err := f.Write(b)
		if err == nil && n < len(b) {
			err = io.ErrShortWrite
		}
		if err1 := f.Close(); err == nil {
			err = err1
		}

		if err != nil {
			return err
		}

		fmt.Printf("successfully created AES-%d key: %s\n", aesSize, encryptionKeyPath)
		return nil
	},
}

func init() {
	cli.GenCmd.AddCommand(genEncryptionKeyCmd)

	genEncryptionKeyCmd.PersistentFlags().IntVarP(&aesSize, "size", "s", 128,
		"AES key size for encryption at rest (one of: 128, 192, 256)")
	genEncryptionKeyCmd.PersistentFlags().BoolVar(&overwriteKey, "overwrite", false,
		"Overwrite key if it exists")
}
