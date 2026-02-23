// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package templates

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/provisionings"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTestTemplatesDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()

	// Template with template.yaml.
	tmpl1Dir := filepath.Join(dir, "my-template")
	require.NoError(t, os.MkdirAll(tmpl1Dir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(tmpl1Dir, "template.yaml"), []byte(
		"name: my-template\ndescription: A test template\n",
	), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(tmpl1Dir, "main.tf"), []byte(
		`variable "identifier" { type = string }`+"\n",
	), 0o644))

	// Template with template.yml.
	tmpl2Dir := filepath.Join(dir, "second-template")
	require.NoError(t, os.MkdirAll(tmpl2Dir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(tmpl2Dir, "template.yml"), []byte(
		"name: second-template\ndescription: Another test\n",
	), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(tmpl2Dir, "variables.tf"), []byte(
		"variable \"region\" {\n  type = string\n  default = \"us-east-1\"\n}\n",
	), 0o644))

	// Directory without marker file (should be ignored).
	sharedDir := filepath.Join(dir, "shared-modules")
	require.NoError(t, os.MkdirAll(sharedDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(sharedDir, "main.tf"), []byte(
		`resource "null_resource" "shared" {}`+"\n",
	), 0o644))

	return dir
}

func TestListTemplates(t *testing.T) {
	dir := setupTestTemplatesDir(t)
	mgr := NewManager(dir)

	templates, err := mgr.ListTemplates()
	require.NoError(t, err)
	require.Len(t, templates, 2)

	names := make(map[string]bool)
	for _, tmpl := range templates {
		names[tmpl.Name] = true
	}
	assert.True(t, names["my-template"], "my-template should be discovered")
	assert.True(t, names["second-template"], "second-template should be discovered")
}

func TestListTemplatesIgnoresNonMarked(t *testing.T) {
	dir := setupTestTemplatesDir(t)
	mgr := NewManager(dir)

	templates, err := mgr.ListTemplates()
	require.NoError(t, err)

	for _, tmpl := range templates {
		assert.NotEqual(t, "shared-modules", tmpl.Name,
			"shared-modules should not be listed as a template")
	}
}

func TestGetTemplate(t *testing.T) {
	dir := setupTestTemplatesDir(t)
	mgr := NewManager(dir)

	tmpl, err := mgr.GetTemplate("my-template")
	require.NoError(t, err)
	assert.Equal(t, "my-template", tmpl.Name)
	assert.Equal(t, "A test template", tmpl.Description)
	assert.Contains(t, tmpl.Variables, "identifier")
}

func TestGetTemplateNotFound(t *testing.T) {
	dir := setupTestTemplatesDir(t)
	mgr := NewManager(dir)

	_, err := mgr.GetTemplate("nonexistent")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestGetTemplateNoMarker(t *testing.T) {
	dir := setupTestTemplatesDir(t)
	mgr := NewManager(dir)

	_, err := mgr.GetTemplate("shared-modules")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestGetTemplateByMetadataName(t *testing.T) {
	dir := setupTestTemplatesDir(t)
	mgr := NewManager(dir)

	// "my-template" is both the dir name and the metadata name, so it works.
	tmpl, err := mgr.GetTemplate("my-template")
	require.NoError(t, err)
	assert.Equal(t, "my-template", tmpl.Name)
	assert.Equal(t, "my-template", tmpl.DirName)

	// "second-template" is both the dir name and the metadata name.
	tmpl, err = mgr.GetTemplate("second-template")
	require.NoError(t, err)
	assert.Equal(t, "second-template", tmpl.Name)
	assert.Equal(t, "second-template", tmpl.DirName)
}

func TestSnapshotTemplate(t *testing.T) {
	dir := setupTestTemplatesDir(t)
	mgr := NewManager(dir)

	archive, checksum, err := mgr.SnapshotTemplate("my-template")
	require.NoError(t, err)
	assert.NotEmpty(t, archive)
	assert.NotEmpty(t, checksum)
	assert.Len(t, checksum, 64, "SHA256 checksum should be 64 hex chars")
}

func TestSnapshotTemplateIdempotentChecksum(t *testing.T) {
	dir := setupTestTemplatesDir(t)
	mgr := NewManager(dir)

	_, checksum1, err := mgr.SnapshotTemplate("my-template")
	require.NoError(t, err)

	_, checksum2, err := mgr.SnapshotTemplate("my-template")
	require.NoError(t, err)

	assert.Equal(t, checksum1, checksum2, "repeated snapshots should produce same checksum")
}

func TestSnapshotRoundTrip(t *testing.T) {
	dir := setupTestTemplatesDir(t)
	mgr := NewManager(dir)

	// Snapshot.
	archive, _, err := mgr.SnapshotTemplate("second-template")
	require.NoError(t, err)

	// Extract.
	extractDir := filepath.Join(t.TempDir(), "extracted")
	err = ExtractSnapshot(archive, extractDir)
	require.NoError(t, err)

	// Parse from extracted files — should get the same variables.
	extractedVars, err := ParseTemplateVariables(extractDir)
	require.NoError(t, err)

	assert.Contains(t, extractedVars, "region")
	assert.Equal(t, "string", extractedVars["region"].Type)
	assert.Equal(t, "us-east-1", extractedVars["region"].Value)
}

func TestExtractSnapshotIdempotent(t *testing.T) {
	dir := setupTestTemplatesDir(t)
	mgr := NewManager(dir)

	archive, _, err := mgr.SnapshotTemplate("my-template")
	require.NoError(t, err)

	extractDir := filepath.Join(t.TempDir(), "extracted")

	// First extraction.
	err = ExtractSnapshot(archive, extractDir)
	require.NoError(t, err)

	// Second extraction should be a no-op (directory already has contents).
	err = ExtractSnapshot(archive, extractDir)
	require.NoError(t, err)
}

func TestSnapshotTemplateWithSymlink(t *testing.T) {
	dir := t.TempDir()

	// Create a shared module.
	sharedDir := filepath.Join(dir, "shared-modules", "my-module")
	require.NoError(t, os.MkdirAll(sharedDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(sharedDir, "main.tf"), []byte(
		`resource "null_resource" "shared" {}`+"\n",
	), 0o644))

	// Create a template that symlinks to the shared module.
	tmplDir := filepath.Join(dir, "my-template")
	require.NoError(t, os.MkdirAll(filepath.Join(tmplDir, "modules"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(tmplDir, "template.yaml"), []byte(
		"name: my-template\ndescription: Template with symlink\n",
	), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(tmplDir, "main.tf"), []byte(
		"module \"shared\" {\n  source = \"./modules/my-module\"\n}\n",
	), 0o644))
	require.NoError(t, os.Symlink(sharedDir, filepath.Join(tmplDir, "modules", "my-module")))

	mgr := NewManager(dir)
	archive, _, err := mgr.SnapshotTemplate("my-template")
	require.NoError(t, err)
	assert.NotEmpty(t, archive)

	// Extract and verify the resolved files are present.
	extractDir := filepath.Join(t.TempDir(), "extracted")
	err = ExtractSnapshot(archive, extractDir)
	require.NoError(t, err)

	// The symlink should have been resolved — check the file exists.
	resolvedPath := filepath.Join(extractDir, "modules", "my-module", "main.tf")
	_, err = os.Stat(resolvedPath)
	assert.NoError(t, err, "resolved symlink file should exist in extracted archive")
}

func TestSnapshotTemplateRejectsOutsideSymlink(t *testing.T) {
	dir := t.TempDir()

	// Create a directory outside the templates root.
	outsideDir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(outsideDir, "secret.tf"), []byte(
		`resource "null_resource" "bad" {}`+"\n",
	), 0o644))

	// Create a template with a symlink pointing outside.
	tmplDir := filepath.Join(dir, "bad-template")
	require.NoError(t, os.MkdirAll(tmplDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(tmplDir, "template.yaml"), []byte(
		"name: bad-template\ndescription: Bad symlink\n",
	), 0o644))
	require.NoError(t, os.Symlink(outsideDir, filepath.Join(tmplDir, "modules")))

	mgr := NewManager(dir)
	_, _, err := mgr.SnapshotTemplate("bad-template")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "outside templates directory")
}

func TestSnapshotIncludesMarkerFile(t *testing.T) {
	dir := setupTestTemplatesDir(t)
	mgr := NewManager(dir)

	archive, _, err := mgr.SnapshotTemplate("my-template")
	require.NoError(t, err)

	extractDir := filepath.Join(t.TempDir(), "extracted")
	err = ExtractSnapshot(archive, extractDir)
	require.NoError(t, err)

	// template.yaml should be in the extracted archive (needed for hooks).
	_, err = os.Stat(filepath.Join(extractDir, "template.yaml"))
	assert.NoError(t, err, "template.yaml should be in the snapshot")
}

func TestParseMetadataFromDir(t *testing.T) {
	dir := setupTestTemplatesDir(t)
	tmplDir := filepath.Join(dir, "my-template")

	meta, err := ParseMetadataFromDir(tmplDir)
	require.NoError(t, err)
	assert.Equal(t, "my-template", meta.Name)
	assert.Equal(t, "A test template", meta.Description)
}

func TestParseMetadataFromDir_NoMarker(t *testing.T) {
	dir := t.TempDir()
	_, err := ParseMetadataFromDir(dir)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no template.yaml")
}

func TestParseMetadataFromDir_WithHooks(t *testing.T) {
	dir := t.TempDir()
	tmplDir := filepath.Join(dir, "hooked")
	require.NoError(t, os.MkdirAll(tmplDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(tmplDir, "template.yaml"), []byte(`
name: hooked
description: Template with hooks
ssh:
  private_key_var: ssh_key
  machines: .instances[].public_ip
  user: ubuntu
hooks:
  - name: wait-ready
    type: run-command
    ssh: true
    command: test -f /.ready
    optional: false
    retry:
      interval: "10s"
      timeout: "5m"
    triggers: [post_apply]
`), 0o644))

	meta, err := ParseMetadataFromDir(tmplDir)
	require.NoError(t, err)
	assert.Equal(t, "hooked", meta.Name)
	require.NotNil(t, meta.SSH)
	assert.Equal(t, "ssh_key", meta.SSH.PrivateKeyVar)
	assert.Equal(t, ".instances[].public_ip", meta.SSH.Machines)
	assert.Equal(t, "ubuntu", meta.SSH.User)
	require.Len(t, meta.Hooks, 1)
	assert.Equal(t, "wait-ready", meta.Hooks[0].Name)
	assert.Equal(t, "run-command", meta.Hooks[0].Type)
	assert.True(t, meta.Hooks[0].SSH)
	assert.Equal(t, "test -f /.ready", meta.Hooks[0].Command)
	assert.False(t, meta.Hooks[0].Optional)
	require.NotNil(t, meta.Hooks[0].Retry)
	assert.Equal(t, "10s", meta.Hooks[0].Retry.Interval)
	assert.Equal(t, "5m", meta.Hooks[0].Retry.Timeout)
	require.Len(t, meta.Hooks[0].Triggers, 1)
	assert.True(t, meta.Hooks[0].HasTrigger(provisionings.TriggerPostApply))
}

func TestParseMetadataFromSnapshot(t *testing.T) {
	dir := t.TempDir()
	tmplDir := filepath.Join(dir, "snap-tmpl")
	require.NoError(t, os.MkdirAll(tmplDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(tmplDir, "template.yaml"), []byte(`
name: snap-tmpl
description: Snapshot metadata test
ssh:
  private_key_var: my_key
  machines: .vms[].ip
  user: root
hooks:
  - name: init-setup
    type: run-command
    ssh: true
    command: echo hello
    triggers: [post_apply]
`), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(tmplDir, "main.tf"), []byte(
		`variable "identifier" { type = string }`,
	), 0o644))

	mgr := NewManager(dir)
	archive, _, err := mgr.SnapshotTemplate("snap-tmpl")
	require.NoError(t, err)

	meta, err := ParseMetadataFromSnapshot(archive)
	require.NoError(t, err)
	assert.Equal(t, "snap-tmpl", meta.Name)
	require.NotNil(t, meta.SSH)
	assert.Equal(t, "my_key", meta.SSH.PrivateKeyVar)
	require.Len(t, meta.Hooks, 1)
	assert.Equal(t, "init-setup", meta.Hooks[0].Name)
}

func TestParseMetadataFromSnapshot_NoMarker(t *testing.T) {
	dir := t.TempDir()
	tmplDir := filepath.Join(dir, "no-marker")
	require.NoError(t, os.MkdirAll(tmplDir, 0o755))
	// Create a minimal template.yaml for snapshot (required by GetTemplate),
	// then remove it from the archive by creating a custom archive without it.
	require.NoError(t, os.WriteFile(filepath.Join(tmplDir, "template.yaml"), []byte(
		"name: no-marker\n",
	), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(tmplDir, "main.tf"), []byte(
		`variable "identifier" { type = string }`,
	), 0o644))

	// Build a custom archive with only main.tf (no template.yaml).
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	data := []byte(`variable "identifier" { type = string }`)
	require.NoError(t, tw.WriteHeader(&tar.Header{
		Name: "main.tf", Size: int64(len(data)), Mode: 0o644,
	}))
	_, err := tw.Write(data)
	require.NoError(t, err)
	require.NoError(t, tw.Close())
	require.NoError(t, gz.Close())

	_, err = ParseMetadataFromSnapshot(buf.Bytes())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no template.yaml")
}

func TestParseMetadataFromDir_NoHooks(t *testing.T) {
	dir := setupTestTemplatesDir(t)
	tmplDir := filepath.Join(dir, "my-template")

	meta, err := ParseMetadataFromDir(tmplDir)
	require.NoError(t, err)
	assert.Nil(t, meta.SSH)
	assert.Empty(t, meta.Hooks)
}

func TestWriteBackendTF(t *testing.T) {
	dir := t.TempDir()
	content := NewLocalBackend().GenerateTF("")
	err := WriteBackendTF(dir, content)
	require.NoError(t, err)

	written, err := os.ReadFile(filepath.Join(dir, "backend.tf"))
	require.NoError(t, err)
	assert.Equal(t, content, string(written))
}

func TestListTFFiles(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "main.tf"), []byte(""), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "variables.tf"), []byte(""), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "override.tf"), []byte(""), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "custom_override.tf"), []byte(""), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, ".hidden.tf"), []byte(""), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "backup.tf~"), []byte(""), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "README.md"), []byte(""), 0o644))

	files, err := listTFFiles(dir)
	require.NoError(t, err)

	// Should include main.tf, variables.tf (primary), then override.tf, custom_override.tf.
	// Should exclude hidden, backup, and non-.tf files.
	require.Len(t, files, 4)

	// Override files should be last.
	assert.Contains(t, files[len(files)-1], "override.tf")
}

func TestGetAsInterface(t *testing.T) {
	tests := []struct {
		name     string
		opt      provisionings.TemplateOption
		expected interface{}
	}{{
		name:     "string",
		opt:      provisionings.TemplateOption{Type: "string", Value: "hello"},
		expected: "hello",
	}, {
		name:     "number",
		opt:      provisionings.TemplateOption{Type: "number", Value: float64(42)},
		expected: float64(42),
	}, {
		name:     "bool",
		opt:      provisionings.TemplateOption{Type: "bool", Value: true},
		expected: true,
	}, {
		name: "list",
		opt: provisionings.TemplateOption{
			Type: "list",
			Value: []provisionings.TemplateOption{
				{Type: "string", Value: "a"},
				{Type: "string", Value: "b"},
			},
		},
		expected: []interface{}{"a", "b"},
	}, {
		name: "object",
		opt: provisionings.TemplateOption{
			Type: "object",
			Value: map[string]provisionings.TemplateOption{
				"key": {Type: "string", Value: "val"},
			},
		},
		expected: map[string]interface{}{"key": "val"},
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.opt.GetAsInterface()
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestToStringMap(t *testing.T) {
	opts := map[string]provisionings.TemplateOption{
		"region": {Type: "string", Value: "us-east-1"},
		"count":  {Type: "number", Value: float64(3)},
		"debug":  {Type: "bool", Value: true},
		"empty":  {Type: "string", Value: nil},
		"tags": {
			Type: "object",
			Value: map[string]provisionings.TemplateOption{
				"env": {Type: "string", Value: "prod"},
			},
		},
	}

	result, err := provisionings.ToStringMap(opts)
	require.NoError(t, err)

	assert.Equal(t, "us-east-1", result["region"])
	assert.Equal(t, "3", result["count"])
	assert.Equal(t, "true", result["debug"])
	assert.NotContains(t, result, "empty", "nil values should be skipped")
	assert.Equal(t, `{"env":"prod"}`, result["tags"])
}
