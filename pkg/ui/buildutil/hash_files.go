// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package buildutil

import (
	"crypto/sha1"
	"encoding/hex"
	"io"
	"io/fs"

	"github.com/cockroachdb/errors"
)

// HashFilesInDir recursively computes the SHA1 hash of every file in fsys
// starting at root, and stores the computed hashes in dest["/path/to/file"]
// (*including* a leading "/").
func HashFilesInDir(dest *map[string]string, fsys fs.FS) error {
	if dest == nil {
		return errors.New("Unable to hash files without a hash destination")
	}

	hash := sha1.New()
	fileHashes := *dest

	return fs.WalkDir(fsys, ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if d.IsDir() {
			return nil
		}

		hash.Reset()

		file, err := fsys.Open(path)
		if err != nil {
			return err
		}
		defer func() { _ = file.Close() }()

		// Copy file contents into the hash algorithm
		if _, err := io.Copy(hash, file); err != nil {
			return err
		}

		// Store the computed hash
		fileHashes["/"+path] = hex.EncodeToString(hash.Sum(nil))
		return nil
	})
}
