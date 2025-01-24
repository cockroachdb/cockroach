// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl/engineccl"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGenEncryptionKey(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	dir := t.TempDir()

	for _, keyVersion := range []int{1, 2} {
		for _, keySize := range []int{128, 192, 256} {
			t.Run(fmt.Sprintf("version=%d/size=%d", keyVersion, keySize), func(t *testing.T) {
				keyName := fmt.Sprintf("aes-%d-v%d.key", keySize, keyVersion)
				keyPath := filepath.Join(dir, keyName)

				err := genEncryptionKey(keyPath, keySize, false, keyVersion)
				require.NoError(t, err)

				if keyVersion == 1 {
					info, err := os.Stat(keyPath)
					require.NoError(t, err)
					// 32-byte id plus the key.
					assert.EqualValues(t, 32+(keySize/8), info.Size())
				}

				key, err := engineccl.LoadKeyFromFile(vfs.Default, keyPath)
				require.NoError(t, err)
				assert.EqualValues(t, keySize/8, len(key.Key))
				// Key ID is hex encoded on load so it's 64 bytes here but 32 in the file size.
				assert.EqualValues(t, 64, len(key.Info.KeyId))

				err = genEncryptionKey(keyPath, keySize, false, keyVersion)
				require.ErrorContains(t, err, fmt.Sprintf("%s: file exists", keyName))

				err = genEncryptionKey(keyPath, keySize, true /* overwrite */, keyVersion)
				require.NoError(t, err)
			})
		}
	}
}
