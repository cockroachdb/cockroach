// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package externalcreds

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDirResolve(t *testing.T) {
	t.Run("zero Dir rejects resolution", func(t *testing.T) {
		var d Dir
		require.False(t, d.IsSet())
		_, err := d.Resolve("oidc")
		require.ErrorContains(t, err, "external credentials directory is not configured")
	})

	t.Run("NewDir with empty path returns zero Dir", func(t *testing.T) {
		d, err := NewDir("")
		require.NoError(t, err)
		require.False(t, d.IsSet())
	})

	t.Run("root directory", func(t *testing.T) {
		d, err := NewDir("/")
		require.NoError(t, err)
		p, err := d.Resolve("test.py")
		require.NoError(t, err)
		require.Equal(t, "/test.py", string(p))
	})

	t.Run("NewDir cleans and absolutizes the path", func(t *testing.T) {
		base := t.TempDir()
		d, err := NewDir(filepath.Join(base, "sub", ".."))
		require.NoError(t, err)
		require.Equal(t, Dir(base), d)
	})

	t.Run("resolves relative path under directory", func(t *testing.T) {
		base := t.TempDir()
		d, err := NewDir(base)
		require.NoError(t, err)

		p, err := d.Resolve("oidc")
		require.NoError(t, err)
		require.Equal(t, SecretPath(filepath.Join(base, "oidc")), p)
	})

	t.Run("rejects absolute paths", func(t *testing.T) {
		d, err := NewDir(t.TempDir())
		require.NoError(t, err)

		_, err = d.Resolve("/etc/passwd")
		require.ErrorContains(t, err, "must be relative to the external credentials directory")
	})

	t.Run("rejects paths escaping via .. segments", func(t *testing.T) {
		d, err := NewDir(t.TempDir())
		require.NoError(t, err)

		_, err = d.Resolve("../escape")
		require.ErrorContains(t, err, "escapes external credentials directory")
	})
}

func TestSecretPathRead(t *testing.T) {
	t.Run("reads file contents", func(t *testing.T) {
		base := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(base, "oidc"), []byte("shh"), 0o600))

		d, err := NewDir(base)
		require.NoError(t, err)
		p, err := d.Resolve("oidc")
		require.NoError(t, err)

		got, err := p.Read()
		require.NoError(t, err)
		require.Equal(t, []byte("shh"), got)
	})

	t.Run("file path is a symlink that points outside the directory", func(t *testing.T) {
		base := t.TempDir()
		outside := filepath.Join(t.TempDir(), "real")
		require.NoError(t, os.WriteFile(outside, []byte("shh"), 0o600))
		symlinkPath := filepath.Join(base, "link")
		require.NoError(t, os.Symlink(outside, symlinkPath))

		d, err := NewDir(base)
		require.NoError(t, err)
		p, err := d.Resolve("link")
		require.NoError(t, err)

		got, err := p.Read()
		require.NoError(t, err)
		require.Equal(t, []byte("shh"), got)
	})

	t.Run("propagates missing-file error", func(t *testing.T) {
		d, err := NewDir(t.TempDir())
		require.NoError(t, err)
		p, err := d.Resolve("nope")
		require.NoError(t, err)

		_, err = p.Read()
		require.ErrorIs(t, err, os.ErrNotExist)
	})
}
