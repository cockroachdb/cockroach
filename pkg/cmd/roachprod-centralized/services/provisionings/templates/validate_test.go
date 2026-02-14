// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package templates

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateModuleReferences_AllValid(t *testing.T) {
	dir := t.TempDir()

	// Template references a local module that exists.
	tmplDir := filepath.Join(dir, "my-template")
	require.NoError(t, os.MkdirAll(filepath.Join(tmplDir, "modules", "vpc"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(tmplDir, "main.tf"), []byte(
		"module \"vpc\" {\n  source = \"./modules/vpc\"\n}\n",
	), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(tmplDir, "modules", "vpc", "main.tf"), []byte(
		"resource \"null_resource\" \"vpc\" {}\n",
	), 0o644))

	warnings := ValidateModuleReferences(tmplDir)
	assert.Empty(t, warnings)
}

func TestValidateModuleReferences_BrokenReference(t *testing.T) {
	dir := t.TempDir()

	// Template references a module that doesn't exist.
	tmplDir := filepath.Join(dir, "my-template")
	require.NoError(t, os.MkdirAll(tmplDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(tmplDir, "main.tf"), []byte(
		"module \"missing\" {\n  source = \"./modules/missing\"\n}\n",
	), 0o644))

	warnings := ValidateModuleReferences(tmplDir)
	require.Len(t, warnings, 1)
	assert.Contains(t, warnings[0], "missing")
	assert.Contains(t, warnings[0], "does not exist")
}

func TestValidateModuleReferences_NestedBrokenReference(t *testing.T) {
	// Simulates the foot-gun: template symlinks module-a from shared-modules,
	// but module-a references ../module-b which doesn't exist at the logical
	// path in the template tree.
	dir := t.TempDir()

	// Create shared-modules with two sibling modules.
	sharedA := filepath.Join(dir, "shared-modules", "module-a")
	require.NoError(t, os.MkdirAll(sharedA, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(sharedA, "main.tf"), []byte(
		"module \"b\" {\n  source = \"../module-b\"\n}\n",
	), 0o644))

	sharedB := filepath.Join(dir, "shared-modules", "module-b")
	require.NoError(t, os.MkdirAll(sharedB, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(sharedB, "main.tf"), []byte(
		"resource \"null_resource\" \"b\" {}\n",
	), 0o644))

	// Template symlinks module-a but NOT module-b.
	tmplDir := filepath.Join(dir, "my-template")
	require.NoError(t, os.MkdirAll(filepath.Join(tmplDir, "modules"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(tmplDir, "main.tf"), []byte(
		"module \"a\" {\n  source = \"./modules/module-a\"\n}\n",
	), 0o644))
	require.NoError(t, os.Symlink(sharedA, filepath.Join(tmplDir, "modules", "module-a")))

	warnings := ValidateModuleReferences(tmplDir)
	require.Len(t, warnings, 1)
	assert.Contains(t, warnings[0], "module-b")
	assert.Contains(t, warnings[0], "does not exist")
}

func TestValidateModuleReferences_NestedAllSymlinked(t *testing.T) {
	// Both sibling modules are symlinked — no warnings.
	dir := t.TempDir()

	sharedA := filepath.Join(dir, "shared-modules", "module-a")
	require.NoError(t, os.MkdirAll(sharedA, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(sharedA, "main.tf"), []byte(
		"module \"b\" {\n  source = \"../module-b\"\n}\n",
	), 0o644))

	sharedB := filepath.Join(dir, "shared-modules", "module-b")
	require.NoError(t, os.MkdirAll(sharedB, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(sharedB, "main.tf"), []byte(
		"resource \"null_resource\" \"b\" {}\n",
	), 0o644))

	tmplDir := filepath.Join(dir, "my-template")
	require.NoError(t, os.MkdirAll(filepath.Join(tmplDir, "modules"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(tmplDir, "main.tf"), []byte(
		"module \"a\" {\n  source = \"./modules/module-a\"\n}\n",
	), 0o644))
	require.NoError(t, os.Symlink(sharedA, filepath.Join(tmplDir, "modules", "module-a")))
	require.NoError(t, os.Symlink(sharedB, filepath.Join(tmplDir, "modules", "module-b")))

	warnings := ValidateModuleReferences(tmplDir)
	assert.Empty(t, warnings)
}

func TestValidateModuleReferences_IgnoresRegistrySources(t *testing.T) {
	dir := t.TempDir()

	tmplDir := filepath.Join(dir, "my-template")
	require.NoError(t, os.MkdirAll(tmplDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(tmplDir, "main.tf"), []byte(
		"module \"vpc\" {\n  source = \"hashicorp/consul/aws\"\n}\n"+
			"module \"s3\" {\n  source = \"github.com/example/module\"\n}\n",
	), 0o644))

	warnings := ValidateModuleReferences(tmplDir)
	assert.Empty(t, warnings)
}

func TestValidateModuleReferences_NoModules(t *testing.T) {
	dir := t.TempDir()

	tmplDir := filepath.Join(dir, "my-template")
	require.NoError(t, os.MkdirAll(tmplDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(tmplDir, "main.tf"), []byte(
		"resource \"null_resource\" \"example\" {}\n",
	), 0o644))

	warnings := ValidateModuleReferences(tmplDir)
	assert.Empty(t, warnings)
}
