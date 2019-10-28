// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package blobs

import (
	"io"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
)

// TODO(georgiah): Use this in LocalClient
func writeFileLocally(filename string, content io.ReadSeeker) error {
	tmpFile, err := ioutil.TempFile(os.TempDir(), filepath.Base(filename))
	if err != nil {
		return errors.Wrap(err, "creating tmp file")
	}
	// TODO(georgiah): cleanup intermediate state on failure
	defer tmpFile.Close()
	_, err = io.Copy(tmpFile, content)
	if err != nil {
		return errors.Wrapf(err, "writing to local external tmp file %q", tmpFile.Name())
	}
	if err := tmpFile.Sync(); err != nil {
		return errors.Wrapf(err, "syncing to local external tmp file %q", tmpFile.Name())
	}
	if err = os.MkdirAll(filepath.Dir(filename), 0755); err != nil {
		return errors.Wrap(err, "creating local external storage path")
	}
	return errors.Wrapf(os.Rename(tmpFile.Name(), filename), "renaming to local export file %q", filename)
}
