// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tofu

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/provisionings/templates"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	tfjson "github.com/hashicorp/terraform-json"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// skipIfNoTofu skips the test if the tofu binary is not available on $PATH.
func skipIfNoTofu(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("tofu"); err != nil {
		t.Skip("tofu binary not found on $PATH, skipping integration test")
	}
}

// testMainTF is the HCL template used for integration tests. It uses the
// null_resource provider which requires no cloud credentials.
const testMainTF = `
terraform {
  required_providers {
    null = {
      source = "hashicorp/null"
    }
  }
}

variable "identifier" {
  type = string
}

variable "test_var" {
  type    = string
  default = "default_value"
}

resource "null_resource" "test" {
  triggers = {
    identifier = var.identifier
    test_var   = var.test_var
  }
}

output "test_output" {
  value = "hello-${var.identifier}"
}

output "test_var_output" {
  value = var.test_var
}
`

// setupTestDir creates a temporary directory containing the null_resource
// template and a local backend configuration for testing.
func setupTestDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()

	err := os.WriteFile(filepath.Join(dir, "main.tf"), []byte(testMainTF), 0o644)
	require.NoError(t, err)

	backendContent := templates.GenerateLocalBackendTF()
	err = templates.WriteBackendTF(dir, backendContent)
	require.NoError(t, err)

	return dir
}

func testLogger() *logger.Logger {
	return logger.NewLogger("error")
}

// TestInitPlanApplyOutputDestroy exercises the complete lifecycle:
// init -> plan (changes) -> apply -> output -> plan (no changes) -> destroy.
func TestInitPlanApplyOutputDestroy(t *testing.T) {
	skipIfNoTofu(t)

	dir := setupTestDir(t)
	e := NewExecutor("")
	ctx := context.Background()
	l := testLogger()
	vars := map[string]string{"identifier": "test1234"}

	// Init.
	err := e.Init(ctx, l, dir, nil)
	require.NoError(t, err)

	// Plan — should detect changes (new resources).
	hasChanges, planJSON, err := e.Plan(ctx, l, dir, vars, nil)
	require.NoError(t, err)
	assert.True(t, hasChanges, "expected changes on first plan")
	assert.NotEmpty(t, planJSON, "plan JSON should not be empty")

	// Validate plan JSON is valid terraform-json format.
	var plan tfjson.Plan
	err = json.Unmarshal(planJSON, &plan)
	require.NoError(t, err, "plan JSON should parse as tfjson.Plan")

	// Apply using the saved plan.
	err = e.Apply(ctx, l, dir, vars, nil)
	require.NoError(t, err)

	// Output — verify values.
	outputs, err := e.Output(ctx, l, dir, nil)
	require.NoError(t, err)
	assert.Equal(t, "hello-test1234", outputs["test_output"])
	assert.Equal(t, "default_value", outputs["test_var_output"])

	// Plan again — should detect no changes.
	hasChanges, _, err = e.Plan(ctx, l, dir, vars, nil)
	require.NoError(t, err)
	assert.False(t, hasChanges, "expected no changes after apply")

	// Destroy.
	err = e.Destroy(ctx, l, dir, vars, nil)
	require.NoError(t, err)
}

// TestPlanWithChanges verifies that Plan returns hasChanges=true and valid
// plan JSON when resources need to be created.
func TestPlanWithChanges(t *testing.T) {
	skipIfNoTofu(t)

	dir := setupTestDir(t)
	e := NewExecutor("")
	ctx := context.Background()
	l := testLogger()
	vars := map[string]string{"identifier": "abc12345"}

	err := e.Init(ctx, l, dir, nil)
	require.NoError(t, err)

	hasChanges, planJSON, err := e.Plan(ctx, l, dir, vars, nil)
	require.NoError(t, err)
	assert.True(t, hasChanges)

	// Plan JSON should be parseable and non-empty.
	assert.True(t, json.Valid(planJSON), "plan JSON should be valid JSON")

	var plan tfjson.Plan
	require.NoError(t, json.Unmarshal(planJSON, &plan))
	assert.NotNil(t, plan.ResourceChanges, "plan should have resource changes")
}

// TestVarsPassed verifies that -var flags are correctly passed to the tofu
// subprocess, overriding default values.
func TestVarsPassed(t *testing.T) {
	skipIfNoTofu(t)

	dir := setupTestDir(t)
	e := NewExecutor("")
	ctx := context.Background()
	l := testLogger()
	vars := map[string]string{
		"identifier": "vartest1",
		"test_var":   "custom_value",
	}

	err := e.Init(ctx, l, dir, nil)
	require.NoError(t, err)

	_, _, err = e.Plan(ctx, l, dir, vars, nil)
	require.NoError(t, err)

	err = e.Apply(ctx, l, dir, vars, nil)
	require.NoError(t, err)

	outputs, err := e.Output(ctx, l, dir, nil)
	require.NoError(t, err)
	assert.Equal(t, "custom_value", outputs["test_var_output"],
		"custom var value should appear in output")
	assert.Equal(t, "hello-vartest1", outputs["test_output"])

	err = e.Destroy(ctx, l, dir, vars, nil)
	require.NoError(t, err)
}

// TestEnvVarsPassed verifies that TF_VAR_ environment variables are correctly
// passed to the tofu subprocess.
func TestEnvVarsPassed(t *testing.T) {
	skipIfNoTofu(t)

	dir := setupTestDir(t)
	e := NewExecutor("")
	ctx := context.Background()
	l := testLogger()
	vars := map[string]string{"identifier": "envtest1"}
	envVars := map[string]string{"TF_VAR_test_var": "from_env"}

	err := e.Init(ctx, l, dir, nil)
	require.NoError(t, err)

	_, _, err = e.Plan(ctx, l, dir, vars, envVars)
	require.NoError(t, err)

	err = e.Apply(ctx, l, dir, vars, envVars)
	require.NoError(t, err)

	outputs, err := e.Output(ctx, l, dir, envVars)
	require.NoError(t, err)
	assert.Equal(t, "from_env", outputs["test_var_output"],
		"TF_VAR_ env var should override default value")

	err = e.Destroy(ctx, l, dir, vars, envVars)
	require.NoError(t, err)
}

// TestContextCancellation verifies that a cancelled context causes the tofu
// command to fail.
func TestContextCancellation(t *testing.T) {
	skipIfNoTofu(t)

	dir := setupTestDir(t)
	e := NewExecutor("")
	l := testLogger()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	err := e.Init(ctx, l, dir, nil)
	assert.Error(t, err, "init with cancelled context should fail")
}

// TestPlanFailsWithUnknownVariable verifies that Plan returns an error
// (exit code 1) when a -var flag references a variable not declared in
// the template.
func TestPlanFailsWithUnknownVariable(t *testing.T) {
	skipIfNoTofu(t)

	dir := setupTestDir(t)
	e := NewExecutor("")
	ctx := context.Background()
	l := testLogger()

	require.NoError(t, e.Init(ctx, l, dir, nil))

	vars := map[string]string{
		"identifier":       "test1",
		"nonexistent_var":  "value",
	}
	hasChanges, planJSON, err := e.Plan(ctx, l, dir, vars, nil)
	assert.Error(t, err, "plan with undeclared variable should fail")
	assert.False(t, hasChanges)
	assert.Nil(t, planJSON)
	assert.Contains(t, err.Error(), "tofu plan:")
}

// TestInitFailsOnInvalidDir verifies that Init returns an error with
// stderr content when the working directory does not exist.
func TestInitFailsOnInvalidDir(t *testing.T) {
	skipIfNoTofu(t)

	e := NewExecutor("")
	ctx := context.Background()
	l := testLogger()

	err := e.Init(ctx, l, "/nonexistent/directory/that/does/not/exist", nil)
	assert.Error(t, err, "init on non-existent directory should fail")
}

// TestOutputParsing verifies that Output correctly extracts values from the
// terraform output JSON structure.
func TestOutputParsing(t *testing.T) {
	skipIfNoTofu(t)

	dir := setupTestDir(t)
	e := NewExecutor("")
	ctx := context.Background()
	l := testLogger()
	vars := map[string]string{"identifier": "outtest1"}

	require.NoError(t, e.Init(ctx, l, dir, nil))
	_, _, err := e.Plan(ctx, l, dir, vars, nil)
	require.NoError(t, err)
	require.NoError(t, e.Apply(ctx, l, dir, vars, nil))

	outputs, err := e.Output(ctx, l, dir, nil)
	require.NoError(t, err)

	// Verify both outputs exist with correct types and values.
	assert.Contains(t, outputs, "test_output")
	assert.Contains(t, outputs, "test_var_output")
	assert.IsType(t, "", outputs["test_output"], "string output should be a string")
	assert.Equal(t, "hello-outtest1", outputs["test_output"])
	assert.Equal(t, "default_value", outputs["test_var_output"])

	require.NoError(t, e.Destroy(ctx, l, dir, vars, nil))
}

// TestDestroyCleanup verifies that Destroy succeeds after Apply and leaves
// no resources.
func TestDestroyCleanup(t *testing.T) {
	skipIfNoTofu(t)

	dir := setupTestDir(t)
	e := NewExecutor("")
	ctx := context.Background()
	l := testLogger()
	vars := map[string]string{"identifier": "destroy1"}

	require.NoError(t, e.Init(ctx, l, dir, nil))
	_, _, err := e.Plan(ctx, l, dir, vars, nil)
	require.NoError(t, err)
	require.NoError(t, e.Apply(ctx, l, dir, vars, nil))

	// Destroy should succeed.
	err = e.Destroy(ctx, l, dir, vars, nil)
	require.NoError(t, err)

	// After destroy, plan should show changes (resources need to be recreated).
	hasChanges, _, err := e.Plan(ctx, l, dir, vars, nil)
	require.NoError(t, err)
	assert.True(t, hasChanges, "plan after destroy should show changes")
}

// TestCustomBinaryPath verifies that NewExecutor uses the provided binary
// path and that it defaults to "tofu" when empty.
func TestCustomBinaryPath(t *testing.T) {
	e := NewExecutor("")
	assert.Equal(t, "tofu", e.binaryPath, "empty path should default to 'tofu'")

	e = NewExecutor("/usr/local/bin/tofu")
	assert.Equal(t, "/usr/local/bin/tofu", e.binaryPath)
}

// TestAppendVarArgs verifies that var args are sorted and correctly formatted.
func TestAppendVarArgs(t *testing.T) {
	args := []string{"plan"}
	vars := map[string]string{
		"z_var": "z_val",
		"a_var": "a_val",
		"m_var": "m_val",
	}

	result := appendVarArgs(args, vars)
	expected := []string{
		"plan",
		"-var", "a_var=a_val",
		"-var", "m_var=m_val",
		"-var", "z_var=z_val",
	}
	assert.Equal(t, expected, result, "vars should be sorted by key")
}

// TestAppendVarArgsEmpty verifies that no var args are added for an empty map.
func TestAppendVarArgsEmpty(t *testing.T) {
	args := []string{"plan"}
	result := appendVarArgs(args, nil)
	assert.Equal(t, []string{"plan"}, result)

	result = appendVarArgs(args, map[string]string{})
	assert.Equal(t, []string{"plan"}, result)
}
