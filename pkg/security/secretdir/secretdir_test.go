// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package secretdir

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReader(t *testing.T) {
	t.Run("disabled when secret directory is empty", func(t *testing.T) {
		r, err := NewReader("")
		require.NoError(t, err)
		require.Nil(t, r)

		_, err = r.ReadFile("/anything")
		require.ErrorContains(t, err, "--secret-directory is not configured")
	})

	t.Run("reads file under configured directory", func(t *testing.T) {
		dir := t.TempDir()
		absPath := filepath.Join(dir, "oidc")
		require.NoError(t, os.WriteFile(absPath, []byte("shh"), 0o600))

		r, err := NewReader(dir)
		require.NoError(t, err)

		got, err := r.ReadFile(absPath)
		require.NoError(t, err)
		require.Equal(t, []byte("shh"), got)
	})

	t.Run("rejects relative paths", func(t *testing.T) {
		r, err := NewReader(t.TempDir())
		require.NoError(t, err)

		_, err = r.ReadFile("oidc")
		require.ErrorContains(t, err, "must be absolute")
	})

	t.Run("rejects paths outside the secret directory", func(t *testing.T) {
		r, err := NewReader(t.TempDir())
		require.NoError(t, err)

		_, err = r.ReadFile("/etc/passwd")
		require.ErrorContains(t, err, "escapes --secret-directory")
	})

	t.Run("rejects paths escaping via .. segments", func(t *testing.T) {
		dir := t.TempDir()
		r, err := NewReader(dir)
		require.NoError(t, err)

		_, err = r.ReadFile(filepath.Join(dir, "..", "escape"))
		require.ErrorContains(t, err, "escapes --secret-directory")
	})

	t.Run("propagates missing-file error", func(t *testing.T) {
		dir := t.TempDir()
		r, err := NewReader(dir)
		require.NoError(t, err)

		_, err = r.ReadFile(filepath.Join(dir, "nope"))
		require.ErrorIs(t, err, os.ErrNotExist)
	})
}
