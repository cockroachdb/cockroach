// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package fileutil

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIsContained(t *testing.T) {
	const base = "/var/secrets"

	tests := []struct {
		name     string
		path     string
		expected bool
	}{
		{name: "equal to base", path: base, expected: true},
		{name: "direct descendant", path: "/var/secrets/file.txt", expected: true},
		{name: "nested descendant", path: "/var/secrets/sub/dir/file.txt", expected: true},
		{name: "sibling with shared prefix", path: "/var/secretsX/file.txt", expected: false},
		{name: "unrelated path", path: "/etc/passwd", expected: false},
		{name: "parent directory", path: "/var", expected: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, IsContained(base, tc.path))
		})
	}
}
