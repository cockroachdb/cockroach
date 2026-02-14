// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vars

import (
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/provisionings"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/environments/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildVarMaps_NoEnv_VarFlagsOnly(t *testing.T) {
	input := BuildVarMapsInput{
		UserVars: map[string]interface{}{
			"region": "us-east-1",
			"count":  float64(3),
		},
		TemplateVars: map[string]provisionings.TemplateOption{
			"identifier": {Type: "string"},
			"region":     {Type: "string"},
			"count":      {Type: "number"},
		},
		Identifier:   "ab12cd34",
		TemplateType: "competitor-rds",
	}

	vars, envVars, err := BuildVarMaps(input)
	require.NoError(t, err)

	assert.Equal(t, "us-east-1", vars["region"])
	assert.Equal(t, "3", vars["count"])
	assert.Equal(t, "ab12cd34", vars["identifier"])
	// No environment vars should be set (no resolved env, no backend env vars).
	assert.Empty(t, envVars)
}

func TestBuildVarMaps_EnvVarsAndUserVars(t *testing.T) {
	input := BuildVarMapsInput{
		ResolvedEnv: types.ResolvedEnvironment{
			Name: "aws-engineering",
			Variables: []types.ResolvedVariable{
				{Key: "vpc_id", Value: "vpc-0abc123", IsSecret: false},
				{Key: "aws_region", Value: "us-east-1", IsSecret: false},
				{Key: "AWS_ACCESS_KEY_ID", Value: "AKIA...", IsSecret: true},
			},
		},
		UserVars: map[string]interface{}{
			"aws_region": "us-west-2", // overrides env var
		},
		TemplateVars: map[string]provisionings.TemplateOption{
			"identifier": {Type: "string"},
			"vpc_id":     {Type: "string"},
			"aws_region": {Type: "string"},
		},
		Identifier:   "test1234",
		TemplateType: "competitor-rds",
	}

	vars, envVars, err := BuildVarMaps(input)
	require.NoError(t, err)

	// Step 1: All env vars should be in envVars as both KEY and TF_VAR_KEY.
	assert.Equal(t, "vpc-0abc123", envVars["vpc_id"])
	assert.Equal(t, "vpc-0abc123", envVars["TF_VAR_vpc_id"])
	assert.Equal(t, "us-east-1", envVars["aws_region"])
	assert.Equal(t, "us-east-1", envVars["TF_VAR_aws_region"])
	assert.Equal(t, "AKIA...", envVars["AWS_ACCESS_KEY_ID"])
	assert.Equal(t, "AKIA...", envVars["TF_VAR_AWS_ACCESS_KEY_ID"])

	// Step 2: Non-secret env vars matching template vars -> in vars.
	// But user override takes precedence.
	assert.Equal(t, "vpc-0abc123", vars["vpc_id"])

	// Step 3: User var overrides env var.
	assert.Equal(t, "us-west-2", vars["aws_region"])

	// Step 4: Auto-injected.
	assert.Equal(t, "test1234", vars["identifier"])

	// Secret env var should NOT be in vars (only in envVars).
	_, hasSecret := vars["AWS_ACCESS_KEY_ID"]
	assert.False(t, hasSecret, "secret should not be in vars")
}

func TestBuildVarMaps_SecretNeverInVars(t *testing.T) {
	input := BuildVarMapsInput{
		ResolvedEnv: types.ResolvedEnvironment{
			Name: "test-env",
			Variables: []types.ResolvedVariable{
				{Key: "SECRET_KEY", Value: "s3cr3t", IsSecret: true},
			},
		},
		TemplateVars: map[string]provisionings.TemplateOption{
			"identifier": {Type: "string"},
			"SECRET_KEY": {Type: "string"},
		},
		Identifier:   "abcd1234",
		TemplateType: "test",
	}

	vars, envVars, err := BuildVarMaps(input)
	require.NoError(t, err)

	// Secret should be in envVars (both raw and TF_VAR_).
	assert.Equal(t, "s3cr3t", envVars["SECRET_KEY"])
	assert.Equal(t, "s3cr3t", envVars["TF_VAR_SECRET_KEY"])

	// Secret must NOT be in vars (would appear on command line).
	_, hasInVars := vars["SECRET_KEY"]
	assert.False(t, hasInVars, "secret must not appear in -var flags")
}

func TestBuildVarMaps_SecretMatchingTemplate_UserOverride(t *testing.T) {
	// A secret env var matches a declared template variable, AND the
	// user explicitly provides an override via --var. The user's value
	// should appear in vars (it's their explicit choice to put it on the
	// command line), but the environment's secret value must not.
	input := BuildVarMapsInput{
		ResolvedEnv: types.ResolvedEnvironment{
			Name: "test-env",
			Variables: []types.ResolvedVariable{
				{Key: "db_password", Value: "env-secret-pw", IsSecret: true},
			},
		},
		UserVars: map[string]interface{}{
			"db_password": "user-provided-pw",
		},
		TemplateVars: map[string]provisionings.TemplateOption{
			"identifier":  {Type: "string"},
			"db_password": {Type: "string"},
		},
		Identifier:   "test1234",
		TemplateType: "test",
	}

	vars, envVars, err := BuildVarMaps(input)
	require.NoError(t, err)

	// User override appears in vars (user's explicit choice).
	assert.Equal(t, "user-provided-pw", vars["db_password"])
	// Secret env value is in envVars only.
	assert.Equal(t, "env-secret-pw", envVars["db_password"])
	assert.Equal(t, "env-secret-pw", envVars["TF_VAR_db_password"])
}

func TestBuildVarMaps_AutoInjectedOverrideUser(t *testing.T) {
	input := BuildVarMapsInput{
		UserVars: map[string]interface{}{
			"identifier": "user-override", // should be overridden
			"prov_name":  "user-prov",     // should be overridden
		},
		TemplateVars: map[string]provisionings.TemplateOption{
			"identifier": {Type: "string"},
			"prov_name":  {Type: "string"},
		},
		Identifier:   "ab12cd34",
		TemplateType: "competitor-rds",
	}

	vars, _, err := BuildVarMaps(input)
	require.NoError(t, err)

	// Auto-injected always wins.
	assert.Equal(t, "ab12cd34", vars["identifier"])
	assert.Equal(t, "competitor-rds-ab12cd34", vars["prov_name"])
}

func TestBuildVarMaps_ConditionalAutoInject(t *testing.T) {
	// Template that declares prov_name, environment, owner.
	input := BuildVarMapsInput{
		TemplateVars: map[string]provisionings.TemplateOption{
			"identifier":  {Type: "string"},
			"prov_name":   {Type: "string"},
			"environment": {Type: "string"},
			"owner":       {Type: "string"},
			"region":      {Type: "string"},
		},
		Identifier:   "test1234",
		TemplateType: "gcs-bucket",
		Environment:  "gcp-ephemeral",
		Owner:        "user@cockroachlabs.com",
	}

	vars, _, err := BuildVarMaps(input)
	require.NoError(t, err)

	assert.Equal(t, "test1234", vars["identifier"])
	assert.Equal(t, "gcs-bucket-test1234", vars["prov_name"])
	assert.Equal(t, "gcp-ephemeral", vars["environment"])
	assert.Equal(t, "user@cockroachlabs.com", vars["owner"])
}

func TestBuildVarMaps_ConditionalAutoInject_NotDeclared(t *testing.T) {
	// Template that does NOT declare prov_name, environment, owner.
	input := BuildVarMapsInput{
		TemplateVars: map[string]provisionings.TemplateOption{
			"identifier": {Type: "string"},
			"region":     {Type: "string"},
		},
		Identifier:   "test1234",
		TemplateType: "gcs-bucket",
		Environment:  "gcp-ephemeral",
		Owner:        "user@cockroachlabs.com",
	}

	vars, _, err := BuildVarMaps(input)
	require.NoError(t, err)

	assert.Equal(t, "test1234", vars["identifier"])
	_, hasProv := vars["prov_name"]
	assert.False(t, hasProv, "prov_name should not be injected if not declared")
	_, hasEnv := vars["environment"]
	assert.False(t, hasEnv, "environment should not be injected if not declared")
	_, hasOwner := vars["owner"]
	assert.False(t, hasOwner, "owner should not be injected if not declared")
}

func TestBuildVarMaps_EnvVarNotMatchingTemplate_NotInVars(t *testing.T) {
	// Env var that doesn't match any template variable should not be
	// in vars (would cause tofu to error), but should still be in
	// envVars as TF_VAR_ (tofu silently ignores unknown TF_VAR_).
	input := BuildVarMapsInput{
		ResolvedEnv: types.ResolvedEnvironment{
			Name: "test-env",
			Variables: []types.ResolvedVariable{
				{Key: "EXTRA_VAR", Value: "extra", IsSecret: false},
			},
		},
		TemplateVars: map[string]provisionings.TemplateOption{
			"identifier": {Type: "string"},
		},
		Identifier:   "test1234",
		TemplateType: "test",
	}

	vars, envVars, err := BuildVarMaps(input)
	require.NoError(t, err)

	// Should be in envVars (as raw and TF_VAR_).
	assert.Equal(t, "extra", envVars["EXTRA_VAR"])
	assert.Equal(t, "extra", envVars["TF_VAR_EXTRA_VAR"])

	// Should NOT be in vars (not a declared template variable).
	_, hasExtra := vars["EXTRA_VAR"]
	assert.False(t, hasExtra, "non-matching env var should not be in vars")
}

func TestBuildVarMaps_ComplexUserVar(t *testing.T) {
	input := BuildVarMapsInput{
		UserVars: map[string]interface{}{
			"tags": map[string]interface{}{
				"team":    "cockroach",
				"purpose": "test",
			},
			"enabled": true,
			"count":   float64(5),
		},
		TemplateVars: map[string]provisionings.TemplateOption{
			"identifier": {Type: "string"},
			"tags":       {Type: "object"},
			"enabled":    {Type: "bool"},
			"count":      {Type: "number"},
		},
		Identifier:   "test1234",
		TemplateType: "test",
	}

	vars, _, err := BuildVarMaps(input)
	require.NoError(t, err)

	assert.Equal(t, "true", vars["enabled"])
	assert.Equal(t, "5", vars["count"])
	// Complex type should be JSON-serialized.
	assert.Contains(t, vars["tags"], `"team"`)
	assert.Contains(t, vars["tags"], `"cockroach"`)
}

func TestBuildVarMaps_BackendEnvVars(t *testing.T) {
	t.Run("credentials provided", func(t *testing.T) {
		input := BuildVarMapsInput{
			TemplateVars: map[string]provisionings.TemplateOption{
				"identifier": {Type: "string"},
			},
			Identifier:   "test1234",
			TemplateType: "test",
			BackendEnvVars: map[string]string{
				"GOOGLE_BACKEND_CREDENTIALS": "/path/to/app-sa.json",
			},
		}

		_, envVars, err := BuildVarMaps(input)
		require.NoError(t, err)

		assert.Equal(t, "/path/to/app-sa.json", envVars["GOOGLE_BACKEND_CREDENTIALS"])
	})

	t.Run("no backend env vars", func(t *testing.T) {
		input := BuildVarMapsInput{
			TemplateVars: map[string]provisionings.TemplateOption{
				"identifier": {Type: "string"},
			},
			Identifier:   "test1234",
			TemplateType: "test",
		}

		_, envVars, err := BuildVarMaps(input)
		require.NoError(t, err)

		_, hasCreds := envVars["GOOGLE_BACKEND_CREDENTIALS"]
		assert.False(t, hasCreds, "should not set backend credentials when none provided")
	})

	t.Run("s3 backend env vars", func(t *testing.T) {
		input := BuildVarMapsInput{
			TemplateVars: map[string]provisionings.TemplateOption{
				"identifier": {Type: "string"},
			},
			Identifier:   "test1234",
			TemplateType: "test",
			BackendEnvVars: map[string]string{
				"AWS_ACCESS_KEY_ID":     "AKIA...",
				"AWS_SECRET_ACCESS_KEY": "secret",
			},
		}

		_, envVars, err := BuildVarMaps(input)
		require.NoError(t, err)

		assert.Equal(t, "AKIA...", envVars["AWS_ACCESS_KEY_ID"])
		assert.Equal(t, "secret", envVars["AWS_SECRET_ACCESS_KEY"])
	})
}

func TestBuildVarMaps_Precedence(t *testing.T) {
	// Full precedence test: auto-injected > user > env.
	input := BuildVarMapsInput{
		ResolvedEnv: types.ResolvedEnvironment{
			Name: "test-env",
			Variables: []types.ResolvedVariable{
				{Key: "region", Value: "env-region", IsSecret: false},
				{Key: "identifier", Value: "env-id", IsSecret: false},
			},
		},
		UserVars: map[string]interface{}{
			"region":     "user-region",
			"identifier": "user-id",
		},
		TemplateVars: map[string]provisionings.TemplateOption{
			"identifier": {Type: "string"},
			"region":     {Type: "string"},
		},
		Identifier:   "auto-id",
		TemplateType: "test",
	}

	vars, _, err := BuildVarMaps(input)
	require.NoError(t, err)

	// Auto-injected identifier wins over both user and env.
	assert.Equal(t, "auto-id", vars["identifier"])
	// User var wins over env var.
	assert.Equal(t, "user-region", vars["region"])
}

func TestBuildVarMaps_EmptyInput(t *testing.T) {
	input := BuildVarMapsInput{
		TemplateVars: map[string]provisionings.TemplateOption{
			"identifier": {Type: "string"},
		},
		Identifier:   "test1234",
		TemplateType: "test",
	}

	vars, envVars, err := BuildVarMaps(input)
	require.NoError(t, err)

	assert.Equal(t, "test1234", vars["identifier"])
	assert.Len(t, vars, 1)
	// Should have no TF_VAR_ entries (no resolved env, no backend env vars).
	tfVarCount := 0
	for k := range envVars {
		if strings.HasPrefix(k, "TF_VAR_") {
			tfVarCount++
		}
	}
	assert.Equal(t, 0, tfVarCount, "should have no TF_VAR_ entries")
}

func TestBuildVarMaps_NilInputs(t *testing.T) {
	// Nil UserVars and nil TemplateVars should not panic.
	input := BuildVarMapsInput{
		Identifier:   "test1234",
		TemplateType: "test",
	}

	vars, envVars, err := BuildVarMaps(input)
	require.NoError(t, err)

	// identifier is still unconditionally injected.
	assert.Equal(t, "test1234", vars["identifier"])
	assert.Len(t, vars, 1)
	assert.Empty(t, envVars)

	// Conditional auto-inject vars should NOT be injected since
	// TemplateVars is nil (not declared).
	_, hasProv := vars["prov_name"]
	assert.False(t, hasProv)
	_, hasEnv := vars["environment"]
	assert.False(t, hasEnv)
	_, hasOwner := vars["owner"]
	assert.False(t, hasOwner)
}

func TestBuildVarMaps_UnknownUserVar(t *testing.T) {
	input := BuildVarMapsInput{
		UserVars: map[string]interface{}{
			"region":      "us-east-1",
			"nonexistent": "value",
		},
		TemplateVars: map[string]provisionings.TemplateOption{
			"identifier": {Type: "string"},
			"region":     {Type: "string"},
		},
		Identifier:   "test1234",
		TemplateType: "test",
	}

	_, _, err := BuildVarMaps(input)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown variable(s)")
	assert.Contains(t, err.Error(), "nonexistent")
}

func TestBuildVarMaps_UnknownUserVar_IdentifierAlwaysAllowed(t *testing.T) {
	// "identifier" is unconditionally auto-injected, so a user-provided
	// --var identifier=... should never trigger the unknown-var check.
	input := BuildVarMapsInput{
		UserVars: map[string]interface{}{
			"identifier": "user-id",
		},
		TemplateVars: map[string]provisionings.TemplateOption{
			"identifier": {Type: "string"},
		},
		Identifier:   "ab12cd34",
		TemplateType: "test",
	}

	vars, _, err := BuildVarMaps(input)
	require.NoError(t, err)
	// Auto-injected identifier wins over user value.
	assert.Equal(t, "ab12cd34", vars["identifier"])
}

func TestBuildVarMaps_UnknownUserVar_ConditionalAutoInjectRejected(t *testing.T) {
	// Conditionally auto-injected variables (prov_name, environment,
	// owner) must be rejected if the user provides them via --var but
	// the template does not declare them — they would otherwise pass
	// through to tofu as undeclared -var args.
	input := BuildVarMapsInput{
		UserVars: map[string]interface{}{
			"prov_name": "user-prov",
		},
		TemplateVars: map[string]provisionings.TemplateOption{
			"identifier": {Type: "string"},
			// prov_name is NOT declared in the template.
		},
		Identifier:   "ab12cd34",
		TemplateType: "test",
	}

	_, _, err := BuildVarMaps(input)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown variable(s)")
	assert.Contains(t, err.Error(), "prov_name")
}

func TestBuildVarMaps_UnknownUserVar_ConditionalAutoInjectDeclared(t *testing.T) {
	// When the template DOES declare prov_name, the user can provide
	// it via --var (auto-injection will override their value).
	input := BuildVarMapsInput{
		UserVars: map[string]interface{}{
			"prov_name": "user-prov",
		},
		TemplateVars: map[string]provisionings.TemplateOption{
			"identifier": {Type: "string"},
			"prov_name":  {Type: "string"},
		},
		Identifier:   "ab12cd34",
		TemplateType: "test",
	}

	vars, _, err := BuildVarMaps(input)
	require.NoError(t, err)
	// Auto-injected prov_name wins over user value.
	assert.Equal(t, "test-ab12cd34", vars["prov_name"])
}

func TestBuildVarMaps_MissingRequired(t *testing.T) {
	input := BuildVarMapsInput{
		TemplateVars: map[string]provisionings.TemplateOption{
			"identifier":  {Type: "string"},
			"gcp_project": {Type: "string", Required: true},
			"region":      {Type: "string", Required: true},
		},
		Identifier:   "test1234",
		TemplateType: "test",
	}

	_, _, err := BuildVarMaps(input)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing required variable(s)")
	assert.Contains(t, err.Error(), "gcp_project")
	assert.Contains(t, err.Error(), "region")
}

func TestBuildVarMaps_MissingRequired_SatisfiedByEnv(t *testing.T) {
	// A required variable satisfied by a secret env var (only present
	// as TF_VAR_*) should not trigger the missing-required check.
	input := BuildVarMapsInput{
		ResolvedEnv: types.ResolvedEnvironment{
			Name: "test-env",
			Variables: []types.ResolvedVariable{
				{Key: "gcp_project", Value: "my-project", IsSecret: true},
			},
		},
		TemplateVars: map[string]provisionings.TemplateOption{
			"identifier":  {Type: "string"},
			"gcp_project": {Type: "string", Required: true},
		},
		Identifier:   "test1234",
		TemplateType: "test",
	}

	_, _, err := BuildVarMaps(input)
	require.NoError(t, err)
}

func TestBuildVarMaps_UnknownAndMissing(t *testing.T) {
	// When both unknown and missing vars exist, the unknown-var error
	// fires first (step 4.1 before step 4.2).
	input := BuildVarMapsInput{
		UserVars: map[string]interface{}{
			"bogus": "value",
		},
		TemplateVars: map[string]provisionings.TemplateOption{
			"identifier":  {Type: "string"},
			"gcp_project": {Type: "string", Required: true},
		},
		Identifier:   "test1234",
		TemplateType: "test",
	}

	_, _, err := BuildVarMaps(input)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown variable(s)")
	assert.Contains(t, err.Error(), "bogus")
}

func TestBuildVarMaps_NoTemplateVars_SkipsValidation(t *testing.T) {
	// When TemplateVars is nil, validation is skipped entirely —
	// arbitrary user vars don't trigger errors.
	input := BuildVarMapsInput{
		UserVars: map[string]interface{}{
			"anything": "goes",
		},
		Identifier:   "test1234",
		TemplateType: "test",
	}

	_, _, err := BuildVarMaps(input)
	require.NoError(t, err)
}

func TestFormatVarValue(t *testing.T) {
	tests := []struct {
		name    string
		val     interface{}
		want    string
		wantErr bool
	}{{
		name: "string",
		val:  "hello",
		want: "hello",
	}, {
		name: "float64 integer",
		val:  float64(42),
		want: "42",
	}, {
		name: "float64 decimal",
		val:  float64(3.14),
		want: "3.14",
	}, {
		name: "bool true",
		val:  true,
		want: "true",
	}, {
		name: "bool false",
		val:  false,
		want: "false",
	}, {
		name: "map",
		val:  map[string]interface{}{"key": "value"},
		want: `{"key":"value"}`,
	}, {
		name: "slice",
		val:  []interface{}{"a", "b"},
		want: `["a","b"]`,
	}, {
		name:    "unmarshalable value",
		val:     make(chan int),
		wantErr: true,
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := formatVarValue(tt.val)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
		})
	}
}
