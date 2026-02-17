// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package templates

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/provisionings"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// writeTFFile writes an HCL file to a temporary directory and returns the dir.
func writeTFFile(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	err := os.WriteFile(filepath.Join(dir, "variables.tf"), []byte(content), 0o644)
	require.NoError(t, err)
	return dir
}

func TestParseStringVariable(t *testing.T) {
	dir := writeTFFile(t, `
variable "region" {
  type    = string
  default = "us-east-1"
  description = "The AWS region"
}
`)
	vars, err := ParseTemplateVariables(dir)
	require.NoError(t, err)

	opt, ok := vars["region"]
	require.True(t, ok, "region variable not found")
	assert.Equal(t, "string", opt.Type)
	assert.Equal(t, "us-east-1", opt.Value)
	assert.False(t, opt.Required)
	assert.Equal(t, "The AWS region", opt.Description)
}

func TestParseNumberVariable(t *testing.T) {
	dir := writeTFFile(t, `
variable "count" {
  type    = number
  default = 3
}
`)
	vars, err := ParseTemplateVariables(dir)
	require.NoError(t, err)

	opt := vars["count"]
	assert.Equal(t, "number", opt.Type)
	assert.Equal(t, float64(3), opt.Value)
	assert.False(t, opt.Required)
}

func TestParseBoolVariable(t *testing.T) {
	dir := writeTFFile(t, `
variable "enable_logging" {
  type    = bool
  default = true
  sensitive = true
}
`)
	vars, err := ParseTemplateVariables(dir)
	require.NoError(t, err)

	opt := vars["enable_logging"]
	assert.Equal(t, "bool", opt.Type)
	assert.Equal(t, true, opt.Value)
	assert.False(t, opt.Required)
	assert.True(t, opt.Sensitive)
}

func TestParseRequiredVariable(t *testing.T) {
	dir := writeTFFile(t, `
variable "identifier" {
  type = string
}
`)
	vars, err := ParseTemplateVariables(dir)
	require.NoError(t, err)

	opt := vars["identifier"]
	assert.Equal(t, "string", opt.Type)
	assert.Nil(t, opt.Value)
	assert.True(t, opt.Required)
}

func TestParseListStringVariable(t *testing.T) {
	dir := writeTFFile(t, `
variable "subnets" {
  type    = list(string)
  default = ["subnet-a", "subnet-b"]
}
`)
	vars, err := ParseTemplateVariables(dir)
	require.NoError(t, err)

	opt := vars["subnets"]
	assert.Equal(t, "list", opt.Type)
	assert.False(t, opt.Required)

	// Check inner types.
	require.Len(t, opt.InnerTypes, 1)
	assert.Equal(t, "string", opt.InnerTypes[0].Type)

	// Check values.
	values, ok := opt.Value.([]provisionings.TemplateOption)
	require.True(t, ok, "expected []TemplateOption, got %T", opt.Value)
	require.Len(t, values, 2)
	assert.Equal(t, "subnet-a", values[0].Value)
	assert.Equal(t, "subnet-b", values[1].Value)
}

func TestParseMapStringVariable(t *testing.T) {
	dir := writeTFFile(t, `
variable "tags" {
  type = map(string)
  default = {
    env  = "staging"
    team = "engineering"
  }
}
`)
	vars, err := ParseTemplateVariables(dir)
	require.NoError(t, err)

	opt := vars["tags"]
	assert.Equal(t, "map", opt.Type)

	values, ok := opt.Value.(map[string]provisionings.TemplateOption)
	require.True(t, ok, "expected map[string]TemplateOption, got %T", opt.Value)
	assert.Equal(t, "staging", values["env"].Value)
	assert.Equal(t, "engineering", values["team"].Value)
}

func TestParseObjectVariable(t *testing.T) {
	dir := writeTFFile(t, `
variable "config" {
  type = object({
    name = string
    port = number
    enabled = bool
  })
  default = {
    name    = "default"
    port    = 8080
    enabled = true
  }
}
`)
	vars, err := ParseTemplateVariables(dir)
	require.NoError(t, err)

	opt := vars["config"]
	assert.Equal(t, "object", opt.Type)

	values, ok := opt.Value.(map[string]provisionings.TemplateOption)
	require.True(t, ok)
	assert.Equal(t, "string", values["name"].Type)
	assert.Equal(t, "default", values["name"].Value)
	assert.Equal(t, "number", values["port"].Type)
	assert.Equal(t, float64(8080), values["port"].Value)
	assert.Equal(t, "bool", values["enabled"].Type)
	assert.Equal(t, true, values["enabled"].Value)
}

func TestParseObjectWithOptionalAndDefaults(t *testing.T) {
	dir := writeTFFile(t, `
variable "bucket_config" {
  type = object({
    region                      = optional(string, "us-east1")
    storage_class               = optional(string, "STANDARD")
    uniform_bucket_level_access = optional(bool, true)
    labels = optional(map(string), {
      "test_label" = "label_value"
    })
    force_destroy = optional(bool, false)
  })
}
`)
	vars, err := ParseTemplateVariables(dir)
	require.NoError(t, err)

	opt := vars["bucket_config"]
	assert.Equal(t, "object", opt.Type)
	assert.True(t, opt.Required, "bucket_config has no default block so it should be required")

	// The object should have the attributes from the type constraint.
	values, ok := opt.Value.(map[string]provisionings.TemplateOption)
	require.True(t, ok)

	assert.Equal(t, "string", values["region"].Type)
	assert.Equal(t, "string", values["storage_class"].Type)
	assert.Equal(t, "bool", values["uniform_bucket_level_access"].Type)
	assert.Equal(t, "map", values["labels"].Type)
	assert.Equal(t, "bool", values["force_destroy"].Type)
}

func TestParseObjectWithOptionalDefaultsApplied(t *testing.T) {
	// When a default IS provided at the variable level, the optional defaults
	// in the type constraint should fill in any missing attributes.
	dir := writeTFFile(t, `
variable "config" {
  type = object({
    region = optional(string, "us-east1")
    name   = string
  })
  default = {
    name = "my-bucket"
  }
}
`)
	vars, err := ParseTemplateVariables(dir)
	require.NoError(t, err)

	opt := vars["config"]
	assert.Equal(t, "object", opt.Type)
	assert.False(t, opt.Required)

	values, ok := opt.Value.(map[string]provisionings.TemplateOption)
	require.True(t, ok)
	assert.Equal(t, "my-bucket", values["name"].Value)
	// The optional default "us-east1" should be applied.
	assert.Equal(t, "us-east1", values["region"].Value)
}

func TestParseSetVariable(t *testing.T) {
	dir := writeTFFile(t, `
variable "allowed_ips" {
  type    = set(string)
  default = ["10.0.0.1", "10.0.0.2"]
}
`)
	vars, err := ParseTemplateVariables(dir)
	require.NoError(t, err)

	opt := vars["allowed_ips"]
	assert.Equal(t, "set", opt.Type)
	require.Len(t, opt.InnerTypes, 1)
	assert.Equal(t, "string", opt.InnerTypes[0].Type)
}

func TestParseTupleVariable(t *testing.T) {
	dir := writeTFFile(t, `
variable "mixed" {
  type    = tuple([string, number, bool])
  default = ["hello", 42, true]
}
`)
	vars, err := ParseTemplateVariables(dir)
	require.NoError(t, err)

	opt := vars["mixed"]
	assert.Equal(t, "tuple", opt.Type)
	require.Len(t, opt.InnerTypes, 3)
	assert.Equal(t, "string", opt.InnerTypes[0].Type)
	assert.Equal(t, "number", opt.InnerTypes[1].Type)
	assert.Equal(t, "bool", opt.InnerTypes[2].Type)

	values, ok := opt.Value.([]provisionings.TemplateOption)
	require.True(t, ok)
	require.Len(t, values, 3)
	assert.Equal(t, "hello", values[0].Value)
	assert.Equal(t, float64(42), values[1].Value)
	assert.Equal(t, true, values[2].Value)
}

func TestParseUntypedVariable(t *testing.T) {
	dir := writeTFFile(t, `
variable "anything" {
  default = "some-value"
}
`)
	vars, err := ParseTemplateVariables(dir)
	require.NoError(t, err)

	opt := vars["anything"]
	assert.False(t, opt.Required)
}

func TestParseVariableWithValidationBlock(t *testing.T) {
	dir := writeTFFile(t, `
variable "name" {
  type = string
  validation {
    condition     = length(var.name) > 0
    error_message = "Name must not be empty."
  }
}
`)
	vars, err := ParseTemplateVariables(dir)
	require.NoError(t, err)

	opt := vars["name"]
	assert.Equal(t, "string", opt.Type)
	assert.True(t, opt.Required)
}

func TestParseVariableWithNullDefault(t *testing.T) {
	dir := writeTFFile(t, `
variable "optional_name" {
  type    = string
  default = null
}
`)
	vars, err := ParseTemplateVariables(dir)
	require.NoError(t, err)

	opt := vars["optional_name"]
	assert.Equal(t, "string", opt.Type)
	// default = null means a default IS provided (null), so not required.
	assert.False(t, opt.Required)
}

func TestParseMultipleFiles(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "main.tf"), []byte(`
resource "null_resource" "example" {}
`), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "variables.tf"), []byte(`
variable "region" {
  type    = string
  default = "us-east-1"
}
`), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "outputs.tf"), []byte(`
output "id" {
  value = null_resource.example.id
}
`), 0o644))

	vars, err := ParseTemplateVariables(dir)
	require.NoError(t, err)
	assert.Len(t, vars, 1)
	assert.Contains(t, vars, "region")
}

func TestParseEmptyDirectory(t *testing.T) {
	dir := t.TempDir()
	vars, err := ParseTemplateVariables(dir)
	require.NoError(t, err)
	assert.Empty(t, vars)
}

func TestParseSkipsHiddenAndBackupFiles(t *testing.T) {
	dir := t.TempDir()
	// Valid file.
	require.NoError(t, os.WriteFile(filepath.Join(dir, "variables.tf"), []byte(`
variable "name" { type = string }
`), 0o644))
	// Hidden file (should be skipped).
	require.NoError(t, os.WriteFile(filepath.Join(dir, ".hidden.tf"), []byte(`
variable "hidden" { type = string }
`), 0o644))
	// Vim backup (should be skipped).
	require.NoError(t, os.WriteFile(filepath.Join(dir, "variables.tf~"), []byte(`
variable "backup" { type = string }
`), 0o644))
	// Emacs backup (should be skipped).
	require.NoError(t, os.WriteFile(filepath.Join(dir, "#variables.tf#"), []byte(`
variable "emacs" { type = string }
`), 0o644))

	vars, err := ParseTemplateVariables(dir)
	require.NoError(t, err)
	assert.Len(t, vars, 1)
	assert.Contains(t, vars, "name")
}

func TestParseListOfObjects(t *testing.T) {
	dir := writeTFFile(t, `
variable "instances" {
  type = list(object({
    name = string
    size = number
  }))
  default = [
    {
      name = "web"
      size = 1
    },
    {
      name = "db"
      size = 2
    }
  ]
}
`)
	vars, err := ParseTemplateVariables(dir)
	require.NoError(t, err)

	opt := vars["instances"]
	assert.Equal(t, "list", opt.Type)
	require.Len(t, opt.InnerTypes, 1)
	assert.Equal(t, "object", opt.InnerTypes[0].Type)

	values, ok := opt.Value.([]provisionings.TemplateOption)
	require.True(t, ok)
	require.Len(t, values, 2)

	// First element should be an object.
	first, ok := values[0].Value.(map[string]provisionings.TemplateOption)
	require.True(t, ok)
	assert.Equal(t, "web", first["name"].Value)
	assert.Equal(t, float64(1), first["size"].Value)
}

func TestParseGCSBucketTemplate(t *testing.T) {
	// Parse the actual gcs-bucket test template if it exists.
	templateDir := filepath.Join("./test-data", "gcs-bucket")

	// The test uses a relative path to the test-data directory.
	// Skip if the directory doesn't exist (e.g., in CI without the full repo).
	if _, err := os.Stat(templateDir); os.IsNotExist(err) {
		skip.IgnoreLint(t, "test-data/gcs-bucket not found, skipping")
	}

	vars, err := ParseTemplateVariables(templateDir)
	require.NoError(t, err)

	// The gcs-bucket template declares: identifier, prov_name, environment,
	// owner, gcp_project, bucket_config, unused.
	assert.Contains(t, vars, "identifier")
	assert.Contains(t, vars, "prov_name")
	assert.Contains(t, vars, "environment")
	assert.Contains(t, vars, "owner")
	assert.Contains(t, vars, "gcp_project")
	assert.Contains(t, vars, "bucket_config")
	assert.Contains(t, vars, "unused")

	// identifier should be required (no default).
	assert.True(t, vars["identifier"].Required)
	assert.Equal(t, "string", vars["identifier"].Type)

	// bucket_config should be an object with default = {}.
	bc := vars["bucket_config"]
	assert.Equal(t, "object", bc.Type)
	assert.False(t, bc.Required, "bucket_config has default = {}")

	// unused should have a null default and not be required.
	assert.False(t, vars["unused"].Required)
	assert.Equal(t, "string", vars["unused"].Type)
	assert.Equal(t, "This is an unused variable to test parsing", vars["unused"].Description)
}
