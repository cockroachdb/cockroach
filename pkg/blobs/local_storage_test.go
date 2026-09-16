// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package blobs

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDirectoryNormalization(t *testing.T) {
	l, err := NewLocalStorage("././.")
	if err != nil {
		t.Fatal(err)
	}
	expected, err := filepath.Abs(".")
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, expected, l.externalIODir)
}

func TestPrependExternalIODir(t *testing.T) {
	externalIODir := filepath.Join(t.TempDir(), "backups")
	l, err := NewLocalStorage(externalIODir)
	if err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name        string
		path        string
		expectedErr string
	}{
		{
			name: "inside",
			path: "test/file.csv",
		},
		{
			name:        "parent",
			path:        "../file.csv",
			expectedErr: "outside of external-io-dir is not allowed",
		},
		{
			name:        "sibling with the io-dir as a name prefix",
			path:        "../backups-archive/file.csv",
			expectedErr: "outside of external-io-dir is not allowed",
		},
		{
			name:        "rooted sibling with the io-dir as a name prefix",
			path:        "/../backups-archive/file.csv",
			expectedErr: "outside of external-io-dir is not allowed",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := l.prependExternalIODir(tc.path)
			if tc.expectedErr != "" {
				require.ErrorContains(t, err, tc.expectedErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
