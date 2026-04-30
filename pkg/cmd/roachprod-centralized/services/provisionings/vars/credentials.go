// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vars

import (
	"maps"
	"os"
	"path/filepath"

	envmodels "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/environments"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/environments/types"
	"github.com/cockroachdb/errors"
)

// credentialsDir is the subdirectory inside the working directory where
// credential files are written.
const credentialsDir = ".credentials"

// PrepareCredentialFiles writes resolved secret_file variable values to
// temporary files inside workingDir. It returns a new copy of envVars
// where secret_file entries are replaced with the file path, and a
// cleanup function that removes the credential files from disk. The
// caller should defer the cleanup function to minimize the time secrets
// are on disk.
//
// This is required for env vars like GOOGLE_APPLICATION_CREDENTIALS
// where the consuming tool (e.g. Google Cloud client libraries) expects
// a file path, not the credential content itself. Without this, the
// client library tries to open() the raw credential content as a
// filename, which fails with "file name too long" and leaks the secret
// in the error message.
//
// The files are written into a ".credentials" subdirectory of workingDir
// with mode 0600.
func PrepareCredentialFiles(
	workingDir string, envVars map[string]string, resolved types.ResolvedEnvironment,
) (updatedEnvVars map[string]string, cleanup func() error, err error) {
	dir := filepath.Join(workingDir, credentialsDir)
	result := maps.Clone(envVars)
	var written bool

	for _, v := range resolved.Variables {
		if v.Type != envmodels.VarTypeSecretFile {
			continue
		}
		if _, present := result[v.Key]; !present {
			continue
		}
		path, writeErr := writeCredentialFile(dir, v.Key, v.Value)
		if writeErr != nil {
			return nil, nil, errors.Wrapf(writeErr, "prepare credential file for %s", v.Key)
		}
		result[v.Key] = path
		written = true
	}

	if !written {
		return result, func() error { return nil }, nil
	}

	return result, func() error {
		return os.RemoveAll(dir)
	}, nil
}

// writeCredentialFile writes content to dir/<key> with mode 0600 and
// returns the absolute path. The directory is created with mode 0700
// if it does not exist.
func writeCredentialFile(dir, key, content string) (string, error) {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return "", errors.Wrap(err, "create credentials directory")
	}
	path := filepath.Join(dir, key)
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		return "", errors.Wrap(err, "write credential file")
	}
	return path, nil
}
