// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package pgwire

import (
	"context"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/hba"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/identmap"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestHbaEnhancedMapper(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	t.Run("NoMapOption_UsesProvidedIdentities", func(t *testing.T) {
		// HBA entry with no "map" option
		hbaConf := "host all all all cert"
		parsed, err := hba.ParseAndNormalize(hbaConf)
		require.NoError(t, err)
		require.Len(t, parsed.Entries, 1)

		identConf := identmap.Empty()
		mapper := HbaEnhancedMapper(&parsed.Entries[0], identConf)

		// Should use UseProvidedIdentities behavior
		mappings, err := mapper(ctx, []string{"user1", "user2"})
		require.NoError(t, err)
		require.Len(t, mappings, 2)
		require.Equal(t, "user1", mappings[0].SystemIdentity)
		require.Equal(t, "user1", mappings[0].MappedUser.Normalized())
		require.Equal(t, "user2", mappings[1].SystemIdentity)
		require.Equal(t, "user2", mappings[1].MappedUser.Normalized())
	})

	t.Run("WithMapOption_UsesIdentityMap", func(t *testing.T) {
		// HBA entry with "map" option
		hbaConf := "host all all all cert map=testmap"
		parsed, err := hba.ParseAndNormalize(hbaConf)
		require.NoError(t, err)
		require.Len(t, parsed.Entries, 1)

		// Create identity map with a mapping rule
		identMapData := `
testmap      /^(.*)@example.com$           \1
`
		identConf, err := identmap.From(strings.NewReader(identMapData))
		require.NoError(t, err)

		mapper := HbaEnhancedMapper(&parsed.Entries[0], identConf)

		// Should delegate to identity map configuration
		mappings, err := mapper(ctx, []string{"alice@example.com", "bob@example.com"})
		require.NoError(t, err)
		require.Len(t, mappings, 2)
		require.Equal(t, "alice@example.com", mappings[0].SystemIdentity)
		require.Equal(t, "alice", mappings[0].MappedUser.Normalized())
		require.Equal(t, "bob@example.com", mappings[1].SystemIdentity)
		require.Equal(t, "bob", mappings[1].MappedUser.Normalized())
	})

	t.Run("RejectsRootUser", func(t *testing.T) {
		// HBA entry with map option
		hbaConf := "host all all all cert map=testmap"
		parsed, err := hba.ParseAndNormalize(hbaConf)
		require.NoError(t, err)

		// Create identity map that maps to root
		identMapData := `
testmap      admin@example.com             root
`
		identConf, err := identmap.From(strings.NewReader(identMapData))
		require.NoError(t, err)

		mapper := HbaEnhancedMapper(&parsed.Entries[0], identConf)

		// Should return error for root user
		mappings, err := mapper(ctx, []string{"admin@example.com"})
		require.Error(t, err)
		require.Contains(t, err.Error(), "system identity \"admin@example.com\" mapped to reserved database role \"root\"")
		require.Nil(t, mappings)
	})

	t.Run("RejectsReservedUser_Node", func(t *testing.T) {
		// HBA entry with map option
		hbaConf := "host all all all cert map=testmap"
		parsed, err := hba.ParseAndNormalize(hbaConf)
		require.NoError(t, err)

		// Create identity map that maps to node (reserved)
		identMapData := `
testmap      system@example.com            node
`
		identConf, err := identmap.From(strings.NewReader(identMapData))
		require.NoError(t, err)

		mapper := HbaEnhancedMapper(&parsed.Entries[0], identConf)

		// Should return error for reserved user "node"
		mappings, err := mapper(ctx, []string{"system@example.com"})
		require.Error(t, err)
		require.Contains(t, err.Error(), "system identity \"system@example.com\" mapped to reserved database role \"node\"")
		require.Nil(t, mappings)
	})

	t.Run("MixedValidAndInvalidUsers", func(t *testing.T) {
		// HBA entry with map option
		hbaConf := "host all all all cert map=testmap"
		parsed, err := hba.ParseAndNormalize(hbaConf)
		require.NoError(t, err)

		// Create identity map with mix of valid and root mapping
		identMapData := `
testmap      alice@example.com             alice
testmap      admin@example.com             root
testmap      bob@example.com               bob
`
		identConf, err := identmap.From(strings.NewReader(identMapData))
		require.NoError(t, err)

		mapper := HbaEnhancedMapper(&parsed.Entries[0], identConf)

		// Should fail with error about root mapping
		mappings, err := mapper(ctx, []string{"alice@example.com", "admin@example.com", "bob@example.com"})
		require.Error(t, err)
		require.Contains(t, err.Error(), "system identity \"admin@example.com\" mapped to reserved database role \"root\"")
		require.Nil(t, mappings)
	})

	t.Run("EmptyIdentitiesList_WithMapOption", func(t *testing.T) {
		// HBA entry with map option
		hbaConf := "host all all all cert map=testmap"
		parsed, err := hba.ParseAndNormalize(hbaConf)
		require.NoError(t, err)

		// Create identity map
		identMapData := `
testmap      /^(.*)@example.com$           \1
`
		identConf, err := identmap.From(strings.NewReader(identMapData))
		require.NoError(t, err)

		mapper := HbaEnhancedMapper(&parsed.Entries[0], identConf)

		// Should return empty mappings without error
		mappings, err := mapper(ctx, []string{})
		require.NoError(t, err)
		require.Len(t, mappings, 0)
	})
}
